use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use group_agent_model::{ToolDefinition, ToolName};
use group_agent_tool::{Tool, ToolRegistry, ToolRegistryError, ToolRuntime};
use rmcp::ServiceError;
use rmcp::model::{PaginatedRequestParams, TaskSupport, Tool as ProtocolTool};
use serde_json::Value;
use thiserror::Error;

use crate::{
    McpAdapterError, McpAdapterErrorKind, McpClientSession, McpConfigError, McpDiscoveryConfig,
    McpServerId, McpToolNamePolicyKind, config::validate_remote_name, session::service_error_kind,
    tool::McpRemoteTool,
};

/// Reversible local-to-remote name mapping frozen into a discovery snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpToolMapping {
    server_id: McpServerId,
    local_name: ToolName,
    remote_name: Arc<str>,
}

impl McpToolMapping {
    /// Returns the originating server.
    #[must_use]
    pub const fn server_id(&self) -> &McpServerId {
        &self.server_id
    }

    /// Returns the name registered in `ToolRegistry`.
    #[must_use]
    pub const fn local_name(&self) -> &ToolName {
        &self.local_name
    }

    /// Returns the original name sent through `call_tool`.
    #[must_use]
    pub fn remote_name(&self) -> &str {
        &self.remote_name
    }
}

/// Immutable discovery and registry snapshot.
#[derive(Clone)]
pub struct McpToolSet {
    registry: ToolRegistry,
    tools: Arc<[Arc<dyn Tool>]>,
    mappings: Arc<[McpToolMapping]>,
    server_ids: Arc<[McpServerId]>,
    name_policy_kinds: Arc<[McpToolNamePolicyKind]>,
}

impl McpToolSet {
    /// Maps an already discovered rmcp tool page set into one immutable
    /// registry snapshot.
    ///
    /// Most callers should use [`McpClientSession::discover`]. This separate
    /// mapping boundary supports deterministic offline validation and
    /// benchmarking without process or transport work.
    pub fn from_discovered(
        session: McpClientSession,
        tools: Vec<ProtocolTool>,
        config: McpDiscoveryConfig,
    ) -> Result<Self, McpAdapterError> {
        let server_id = session.server_id().clone();
        let name_policy_kind = config.name_policy().kind();
        let mut by_local = BTreeMap::<ToolName, (Arc<dyn Tool>, McpToolMapping)>::new();
        let mut discovered_remote = BTreeSet::new();

        for protocol_tool in tools {
            validate_remote_name(&protocol_tool.name).map_err(|source| {
                McpAdapterError::with_source(McpAdapterErrorKind::InvalidToolDefinition, source)
                    .with_server(session.server_id().clone())
            })?;
            if matches!(
                protocol_tool
                    .execution
                    .as_ref()
                    .and_then(|execution| execution.task_support),
                Some(TaskSupport::Required)
            ) {
                return Err(McpAdapterError::with_source(
                    McpAdapterErrorKind::InvalidToolDefinition,
                    McpProtocolDefinitionError::TaskExecutionRequired,
                )
                .with_server(session.server_id().clone()));
            }

            let remote_name = protocol_tool.name.into_owned();
            discovered_remote.insert(remote_name.clone());
            let local_name = config
                .name_policy()
                .local_name(session.server_id(), &remote_name)
                .map_err(|source| {
                    McpAdapterError::with_source(McpAdapterErrorKind::InvalidToolDefinition, source)
                        .with_server(session.server_id().clone())
                })?;
            if by_local.contains_key(&local_name) {
                return Err(McpAdapterError::new(McpAdapterErrorKind::ToolNameConflict)
                    .with_server(session.server_id().clone())
                    .with_tool(local_name));
            }

            let description = protocol_tool
                .description
                .map(|description| description.into_owned())
                .filter(|description| !description.trim().is_empty())
                .unwrap_or_else(|| format!("MCP tool `{remote_name}`"));
            let schema = Value::Object((*protocol_tool.input_schema).clone());
            let definition = ToolDefinition::new(local_name.clone(), description, schema);
            let behavior = config.behavior_for(&remote_name);
            let tool: Arc<dyn Tool> = Arc::new(McpRemoteTool::new(
                session.clone(),
                Arc::<str>::from(remote_name.as_str()),
                definition,
                behavior,
            ));
            let mapping = McpToolMapping {
                server_id: session.server_id().clone(),
                local_name: local_name.clone(),
                remote_name: Arc::from(remote_name),
            };
            by_local.insert(local_name, (tool, mapping));
        }

        if config
            .override_names()
            .any(|remote_name| !discovered_remote.contains(remote_name))
        {
            return Err(
                McpAdapterError::from(McpConfigError::UnknownBehaviorOverride)
                    .with_server(session.server_id().clone()),
            );
        }

        Self::from_ordered_entries(by_local.into_values(), [server_id], [name_policy_kind])
    }

    /// Combines immutable server snapshots without modifying any source set.
    ///
    /// A duplicate local name is a structured conflict. Use server namespaces
    /// or stable application prefixes before discovery to avoid collisions.
    pub fn combine<I>(sets: I) -> Result<Self, McpAdapterError>
    where
        I: IntoIterator<Item = Self>,
    {
        let mut by_local = BTreeMap::<ToolName, (Arc<dyn Tool>, McpToolMapping)>::new();
        let mut server_ids = BTreeSet::new();
        let mut name_policy_kinds = BTreeSet::new();
        for set in sets {
            server_ids.extend(set.server_ids.iter().cloned());
            name_policy_kinds.extend(set.name_policy_kinds.iter().copied());
            for (tool, mapping) in set.tools.iter().zip(set.mappings.iter()) {
                if by_local
                    .insert(
                        mapping.local_name.clone(),
                        (Arc::clone(tool), mapping.clone()),
                    )
                    .is_some()
                {
                    return Err(McpAdapterError::new(McpAdapterErrorKind::ToolNameConflict)
                        .with_server(mapping.server_id.clone())
                        .with_tool(mapping.local_name.clone()));
                }
            }
        }
        Self::from_ordered_entries(by_local.into_values(), server_ids, name_policy_kinds)
    }

    fn from_ordered_entries<I, S, N>(
        entries: I,
        server_ids: S,
        name_policy_kinds: N,
    ) -> Result<Self, McpAdapterError>
    where
        I: IntoIterator<Item = (Arc<dyn Tool>, McpToolMapping)>,
        S: IntoIterator<Item = McpServerId>,
        N: IntoIterator<Item = McpToolNamePolicyKind>,
    {
        let (tools, mappings): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let registry = ToolRegistry::try_from_tools(tools.iter().cloned()).map_err(map_registry)?;
        Ok(Self {
            registry,
            tools: tools.into(),
            mappings: mappings.into(),
            server_ids: server_ids.into_iter().collect(),
            name_policy_kinds: name_policy_kinds.into_iter().collect(),
        })
    }

    /// Returns the immutable Tool Registry snapshot.
    #[must_use]
    pub const fn registry(&self) -> &ToolRegistry {
        &self.registry
    }

    /// Creates a Tool Runtime over this immutable snapshot.
    #[must_use]
    pub fn runtime(&self) -> ToolRuntime {
        ToolRuntime::new(self.registry.clone())
    }

    /// Returns stable lexical local-to-remote mappings.
    #[must_use]
    pub fn mappings(&self) -> &[McpToolMapping] {
        &self.mappings
    }

    /// Resolves a local name to the exact remote name without parsing prefixes.
    #[must_use]
    pub fn remote_name(&self, local_name: &ToolName) -> Option<&str> {
        self.mappings
            .binary_search_by(|mapping| mapping.local_name.cmp(local_name))
            .ok()
            .map(|index| self.mappings[index].remote_name())
    }

    /// Returns the number of discovered tools.
    #[must_use]
    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Returns whether this snapshot contains no tools.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }
}

impl fmt::Debug for McpToolSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpToolSet")
            .field("server_ids", &self.server_ids)
            .field("tool_count", &self.tools.len())
            .field("local_name_count", &self.registry.len())
            .field("has_registry", &true)
            .field("name_policy_kinds", &self.name_policy_kinds)
            .finish()
    }
}

impl McpClientSession {
    /// Discovers all pages and returns a new immutable Tool snapshot.
    ///
    /// Pagination is adapter-owned and rejects repeated cursors, page-limit
    /// overflow, and tool-limit overflow. Definitions are accumulated privately
    /// and no Registry or mapping is constructed unless every page succeeds.
    ///
    /// Calling this method again is an explicit refresh. Existing snapshots and
    /// registries remain unchanged; `tools/list_changed` notifications are not
    /// applied automatically.
    pub async fn discover(
        &self,
        config: McpDiscoveryConfig,
    ) -> Result<McpToolSet, McpAdapterError> {
        let peer = self.peer()?;
        let server_info = peer.peer_info().ok_or_else(|| {
            McpAdapterError::new(McpAdapterErrorKind::InitializationFailed)
                .with_server(self.server_id().clone())
        })?;
        if server_info.capabilities.tools.is_none() {
            return Err(McpAdapterError::new(McpAdapterErrorKind::CapabilityMissing)
                .with_server(self.server_id().clone()));
        }
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut page_count = 0_usize;
        loop {
            page_count = page_count
                .checked_add(1)
                .ok_or_else(|| self.pagination_error(McpPaginationError::PageCountOverflow))?;
            if page_count > config.max_pages() {
                return Err(self.pagination_error(McpPaginationError::PageLimitExceeded));
            }
            let page = peer
                .list_tools(Some(PaginatedRequestParams::default().with_cursor(cursor)))
                .await
                .map_err(|source| {
                    let kind = discovery_error_kind(&source);
                    McpAdapterError::with_source(kind, source).with_server(self.server_id().clone())
                })?;
            let total_tools = tools
                .len()
                .checked_add(page.tools.len())
                .ok_or_else(|| self.pagination_error(McpPaginationError::ToolCountOverflow))?;
            if total_tools > config.max_tools() {
                return Err(self.pagination_error(McpPaginationError::ToolLimitExceeded));
            }
            tools.extend(page.tools);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(self.pagination_error(McpPaginationError::CursorCycle));
            }
            cursor = Some(next_cursor);
        }
        McpToolSet::from_discovered(self.clone(), tools, config)
    }

    fn pagination_error(&self, source: McpPaginationError) -> McpAdapterError {
        let kind = match source {
            McpPaginationError::CursorCycle => McpAdapterErrorKind::Protocol,
            _ => McpAdapterErrorKind::DiscoveryFailed,
        };
        McpAdapterError::with_source(kind, source).with_server(self.server_id().clone())
    }
}

fn discovery_error_kind(error: &ServiceError) -> McpAdapterErrorKind {
    match service_error_kind(error) {
        McpAdapterErrorKind::Transport => McpAdapterErrorKind::Transport,
        McpAdapterErrorKind::SessionClosed => McpAdapterErrorKind::SessionClosed,
        McpAdapterErrorKind::Protocol => McpAdapterErrorKind::Protocol,
        _ => McpAdapterErrorKind::DiscoveryFailed,
    }
}

fn map_registry(source: ToolRegistryError) -> McpAdapterError {
    let kind = match &source {
        ToolRegistryError::DuplicateTool { .. } => McpAdapterErrorKind::ToolNameConflict,
        ToolRegistryError::InvalidDefinition { .. } => McpAdapterErrorKind::InvalidToolDefinition,
        _ => McpAdapterErrorKind::Other,
    };
    McpAdapterError::with_source(kind, source)
}

#[derive(Debug, Error)]
enum McpProtocolDefinitionError {
    #[error("MCP task-required tools are unsupported")]
    TaskExecutionRequired,
}

#[derive(Clone, Copy, Debug, Error)]
enum McpPaginationError {
    #[error("MCP discovery cursor cycle detected")]
    CursorCycle,
    #[error("MCP discovery page limit exceeded")]
    PageLimitExceeded,
    #[error("MCP discovery tool limit exceeded")]
    ToolLimitExceeded,
    #[error("MCP discovery page count overflow")]
    PageCountOverflow,
    #[error("MCP discovery tool count overflow")]
    ToolCountOverflow,
}
