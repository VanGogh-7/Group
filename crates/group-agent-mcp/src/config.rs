use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use group_agent_model::{IdentifierError, ToolName};
use group_agent_tool::ToolBehavior;

use crate::McpConfigError;

/// Default upper bound for one complete discovery traversal.
pub const DEFAULT_MAX_DISCOVERY_PAGES: usize = 256;
/// Default upper bound for tools accumulated before publishing a snapshot.
pub const DEFAULT_MAX_DISCOVERED_TOOLS: usize = 4_096;
/// Default grace period before explicit stdio shutdown kills the direct child.
pub const DEFAULT_STDIO_SHUTDOWN_GRACE: Duration = Duration::from_secs(3);

/// Stable identity for one configured MCP server.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct McpServerId(Arc<str>);

impl McpServerId {
    /// Creates a canonical server identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, McpConfigError> {
        let value = value.into();
        if value.is_empty() {
            return Err(McpConfigError::EmptyServerId);
        }
        if !is_canonical(&value) {
            return Err(McpConfigError::InvalidServerId);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Returns the identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for McpServerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Debug for McpServerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("McpServerId")
            .field(&self.as_str())
            .finish()
    }
}

/// Validated prefix used to namespace local tool names.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct McpToolPrefix(Arc<str>);

impl McpToolPrefix {
    /// Creates a canonical namespace prefix.
    pub fn new(value: impl Into<String>) -> Result<Self, McpConfigError> {
        let value = value.into();
        if value.is_empty() {
            return Err(McpConfigError::EmptyToolPrefix);
        }
        if !is_canonical(&value) {
            return Err(McpConfigError::InvalidToolPrefix);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Returns the prefix text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for McpToolPrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("McpToolPrefix")
            .field(&self.as_str())
            .finish()
    }
}

/// Stable local-name policy for discovered tools.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpToolNamePolicy {
    /// Preserve the remote tool name.
    #[default]
    Preserve,
    /// Prefix with the configured server identity and `__`.
    ServerNamespace,
    /// Prefix with an application-selected value and `__`.
    Prefix(McpToolPrefix),
}

/// Payload-safe category for one local naming policy.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum McpToolNamePolicyKind {
    Preserve,
    ServerNamespace,
    Prefix,
}

impl McpToolNamePolicy {
    /// Returns the payload-safe policy category.
    #[must_use]
    pub const fn kind(&self) -> McpToolNamePolicyKind {
        match self {
            Self::Preserve => McpToolNamePolicyKind::Preserve,
            Self::ServerNamespace => McpToolNamePolicyKind::ServerNamespace,
            Self::Prefix(_) => McpToolNamePolicyKind::Prefix,
        }
    }

    /// Maps one remote name to its deterministic local Tool name.
    pub fn local_name(
        &self,
        server_id: &McpServerId,
        remote_name: &str,
    ) -> Result<ToolName, McpConfigError> {
        validate_remote_name(remote_name)?;
        let local_name = match self {
            Self::Preserve => remote_name.to_owned(),
            Self::ServerNamespace => format!("{}__{remote_name}", server_id.as_str()),
            Self::Prefix(prefix) => format!("{}__{remote_name}", prefix.as_str()),
        };
        ToolName::new(local_name).map_err(map_local_identifier)
    }
}

fn map_local_identifier(_source: IdentifierError) -> McpConfigError {
    McpConfigError::InvalidRemoteToolName
}

/// Immutable discovery policy for one server snapshot.
#[derive(Clone, Default)]
pub struct McpDiscoveryConfig {
    name_policy: McpToolNamePolicy,
    behavior_overrides: BTreeMap<String, ToolBehavior>,
    max_pages: usize,
    max_tools: usize,
}

impl McpDiscoveryConfig {
    /// Creates conservative discovery configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            name_policy: McpToolNamePolicy::Preserve,
            behavior_overrides: BTreeMap::new(),
            max_pages: DEFAULT_MAX_DISCOVERY_PAGES,
            max_tools: DEFAULT_MAX_DISCOVERED_TOOLS,
        }
    }

    /// Selects the local naming strategy.
    #[must_use]
    pub fn with_name_policy(mut self, name_policy: McpToolNamePolicy) -> Self {
        self.name_policy = name_policy;
        self
    }

    /// Freezes an explicit behavior override for one remote tool name.
    ///
    /// A second entry for the same name is rejected even when both behavior
    /// values are equal, so merged configuration cannot silently use
    /// last-write-wins semantics.
    pub fn with_behavior_override(
        mut self,
        remote_name: impl Into<String>,
        behavior: ToolBehavior,
    ) -> Result<Self, McpConfigError> {
        let remote_name = remote_name.into();
        validate_remote_name(&remote_name)?;
        if self.behavior_overrides.contains_key(&remote_name) {
            return Err(McpConfigError::DuplicateBehaviorOverride);
        }
        self.behavior_overrides.insert(remote_name, behavior);
        Ok(self)
    }

    /// Sets a non-zero maximum page count for one discovery traversal.
    pub fn with_max_pages(mut self, max_pages: usize) -> Result<Self, McpConfigError> {
        if max_pages == 0 {
            return Err(McpConfigError::ZeroDiscoveryPageLimit);
        }
        self.max_pages = max_pages;
        Ok(self)
    }

    /// Sets a non-zero maximum accumulated tool count.
    pub fn with_max_tools(mut self, max_tools: usize) -> Result<Self, McpConfigError> {
        if max_tools == 0 {
            return Err(McpConfigError::ZeroDiscoveryToolLimit);
        }
        self.max_tools = max_tools;
        Ok(self)
    }

    /// Returns the local-name policy.
    #[must_use]
    pub const fn name_policy(&self) -> &McpToolNamePolicy {
        &self.name_policy
    }

    /// Returns the maximum number of requested pages.
    #[must_use]
    pub const fn max_pages(&self) -> usize {
        self.max_pages
    }

    /// Returns the maximum number of accumulated tools.
    #[must_use]
    pub const fn max_tools(&self) -> usize {
        self.max_tools
    }

    pub(crate) fn behavior_for(&self, remote_name: &str) -> ToolBehavior {
        self.behavior_overrides
            .get(remote_name)
            .copied()
            .unwrap_or_else(ToolBehavior::non_idempotent_write)
    }

    pub(crate) fn override_names(&self) -> impl Iterator<Item = &str> {
        self.behavior_overrides.keys().map(String::as_str)
    }
}

impl fmt::Debug for McpDiscoveryConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpDiscoveryConfig")
            .field("name_policy", &self.name_policy)
            .field("behavior_override_count", &self.behavior_overrides.len())
            .field("max_pages", &self.max_pages)
            .field("max_tools", &self.max_tools)
            .finish()
    }
}

/// Stdio process configuration using an executable and separate arguments.
pub struct McpServerConfig {
    server_id: McpServerId,
    executable: PathBuf,
    args: Vec<OsString>,
    environment: BTreeMap<OsString, OsString>,
    inherit_environment: bool,
    current_dir: Option<PathBuf>,
    shutdown_grace: Duration,
}

pub(crate) struct McpStdioParts {
    pub(crate) server_id: McpServerId,
    pub(crate) executable: PathBuf,
    pub(crate) args: Vec<OsString>,
    pub(crate) environment: BTreeMap<OsString, OsString>,
    pub(crate) inherit_environment: bool,
    pub(crate) current_dir: Option<PathBuf>,
    pub(crate) shutdown_grace: Duration,
}

impl McpServerConfig {
    /// Creates a stdio server configuration.
    pub fn new(
        server_id: McpServerId,
        executable: impl Into<PathBuf>,
    ) -> Result<Self, McpConfigError> {
        let executable = executable.into();
        if executable.as_os_str().is_empty() {
            return Err(McpConfigError::EmptyExecutable);
        }
        Ok(Self {
            server_id,
            executable,
            args: Vec::new(),
            environment: BTreeMap::new(),
            inherit_environment: true,
            current_dir: None,
            shutdown_grace: DEFAULT_STDIO_SHUTDOWN_GRACE,
        })
    }

    /// Appends one process argument without shell parsing.
    #[must_use]
    pub fn with_arg(mut self, argument: impl Into<OsString>) -> Self {
        self.args.push(argument.into());
        self
    }

    /// Appends process arguments without shell parsing.
    #[must_use]
    pub fn with_args<I, S>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(arguments.into_iter().map(Into::into));
        self
    }

    /// Adds one child environment value.
    pub fn with_environment(
        mut self,
        key: impl Into<OsString>,
        value: impl Into<OsString>,
    ) -> Result<Self, McpConfigError> {
        let key = key.into();
        validate_environment_key(&key)?;
        self.environment.insert(key, value.into());
        Ok(self)
    }

    /// Selects whether the child inherits the parent environment.
    #[must_use]
    pub const fn with_inherited_environment(mut self, inherited: bool) -> Self {
        self.inherit_environment = inherited;
        self
    }

    /// Sets the child working directory.
    #[must_use]
    pub fn with_current_dir(mut self, current_dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(current_dir.into());
        self
    }

    /// Sets the bounded grace period before killing a stubborn direct child.
    ///
    /// Zero is valid: shutdown performs one non-blocking exit check and then
    /// immediately kills and waits for a still-running direct child.
    pub fn with_shutdown_grace(mut self, shutdown_grace: Duration) -> Result<Self, McpConfigError> {
        self.shutdown_grace = shutdown_grace;
        Ok(self)
    }

    /// Returns the server identity.
    #[must_use]
    pub const fn server_id(&self) -> &McpServerId {
        &self.server_id
    }

    pub(crate) fn into_parts(self) -> McpStdioParts {
        McpStdioParts {
            server_id: self.server_id,
            executable: self.executable,
            args: self.args,
            environment: self.environment,
            inherit_environment: self.inherit_environment,
            current_dir: self.current_dir,
            shutdown_grace: self.shutdown_grace,
        }
    }
}

impl fmt::Debug for McpServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerConfig")
            .field("server_id", &self.server_id)
            .field("has_executable", &!self.executable.as_os_str().is_empty())
            .field("argument_count", &self.args.len())
            .field("environment_entry_count", &self.environment.len())
            .field("inherits_environment", &self.inherit_environment)
            .field("has_current_dir", &self.current_dir.is_some())
            .field("shutdown_grace", &self.shutdown_grace)
            .finish()
    }
}

pub(crate) fn validate_remote_name(name: &str) -> Result<(), McpConfigError> {
    if is_canonical(name) {
        Ok(())
    } else {
        Err(McpConfigError::InvalidRemoteToolName)
    }
}

fn is_canonical(value: &str) -> bool {
    !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control)
}

fn validate_environment_key(key: &OsStr) -> Result<(), McpConfigError> {
    if key.is_empty() || key.to_string_lossy().contains('=') {
        Err(McpConfigError::InvalidEnvironmentKey)
    } else {
        Ok(())
    }
}
