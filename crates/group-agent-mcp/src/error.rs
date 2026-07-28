use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use group_agent_model::ToolName;
use thiserror::Error;

use crate::McpServerId;

type SharedError = Arc<dyn StdError + Send + Sync + 'static>;

/// Stable MCP adapter failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpAdapterErrorKind {
    InvalidConfig,
    InitializationFailed,
    CapabilityMissing,
    DiscoveryFailed,
    InvalidToolDefinition,
    ToolNameConflict,
    UnsupportedContent,
    CallFailed,
    Protocol,
    Transport,
    SessionClosed,
    ShutdownFailed,
    Other,
}

/// Why local MCP configuration could not be accepted.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpConfigError {
    #[error("server identifier must not be empty")]
    EmptyServerId,
    #[error("server identifier must be canonical")]
    InvalidServerId,
    #[error("stdio executable must not be empty")]
    EmptyExecutable,
    #[error("environment key must be canonical")]
    InvalidEnvironmentKey,
    #[error("tool namespace prefix must not be empty")]
    EmptyToolPrefix,
    #[error("tool namespace prefix must be canonical")]
    InvalidToolPrefix,
    #[error("remote tool name must be canonical")]
    InvalidRemoteToolName,
    #[error("behavior override is duplicated")]
    DuplicateBehaviorOverride,
    #[error("behavior override does not match a discovered remote tool")]
    UnknownBehaviorOverride,
    #[error("discovery page limit must be greater than zero")]
    ZeroDiscoveryPageLimit,
    #[error("discovery tool limit must be greater than zero")]
    ZeroDiscoveryToolLimit,
}

/// Payload-safe MCP adapter error with an optional concrete source.
#[derive(Clone)]
#[non_exhaustive]
pub struct McpAdapterError {
    kind: McpAdapterErrorKind,
    server_id: Option<McpServerId>,
    tool_name: Option<ToolName>,
    source: Option<SharedError>,
}

impl McpAdapterError {
    pub(crate) fn new(kind: McpAdapterErrorKind) -> Self {
        Self {
            kind,
            server_id: None,
            tool_name: None,
            source: None,
        }
    }

    pub(crate) fn with_source(
        kind: McpAdapterErrorKind,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            server_id: None,
            tool_name: None,
            source: Some(Arc::new(source)),
        }
    }

    pub(crate) fn with_server(mut self, server_id: McpServerId) -> Self {
        self.server_id = Some(server_id);
        self
    }

    pub(crate) fn with_tool(mut self, tool_name: ToolName) -> Self {
        self.tool_name = Some(tool_name);
        self
    }

    /// Returns the stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> McpAdapterErrorKind {
        self.kind
    }

    /// Returns the affected server identity when available.
    #[must_use]
    pub const fn server_id(&self) -> Option<&McpServerId> {
        self.server_id.as_ref()
    }

    /// Returns the affected local tool name when available.
    #[must_use]
    pub const fn tool_name(&self) -> Option<&ToolName> {
        self.tool_name.as_ref()
    }
}

impl fmt::Display for McpAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MCP adapter failure ({:?})", self.kind)?;
        if let Some(server_id) = &self.server_id {
            write!(formatter, " for server `{server_id}`")?;
        }
        if let Some(tool_name) = &self.tool_name {
            write!(formatter, " and tool `{tool_name}`")?;
        }
        Ok(())
    }
}

impl fmt::Debug for McpAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpAdapterError")
            .field("kind", &self.kind)
            .field("server_id", &self.server_id)
            .field("tool_name", &self.tool_name)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl StdError for McpAdapterError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl From<McpConfigError> for McpAdapterError {
    fn from(source: McpConfigError) -> Self {
        Self::with_source(McpAdapterErrorKind::InvalidConfig, source)
    }
}

#[derive(Debug, Error)]
#[error("MCP result contains unsupported content")]
pub(crate) struct UnsupportedContentSource {
    kind: Arc<str>,
}

impl UnsupportedContentSource {
    pub(crate) fn new(kind: &'static str) -> Self {
        Self {
            kind: Arc::from(kind),
        }
    }
}
