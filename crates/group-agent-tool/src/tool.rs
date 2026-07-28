use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use group_agent_model::{
    ContentPart, Extensions, ToolCallId, ToolDefinition, ToolName, ToolResult,
};
use serde_json::Value;

use crate::{IdempotencyKeyError, ToolBehaviorError, ToolError};

/// Coarse side-effect classification for application execution policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolSideEffect {
    ReadOnly,
    IdempotentWrite,
    NonIdempotentWrite,
}

/// Stable behavior metadata cached with a registered tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ToolBehavior {
    side_effect: ToolSideEffect,
    allows_parallel: bool,
    requires_idempotency_key: bool,
}

impl ToolBehavior {
    /// Creates the conservative default behavior for a side-effect class.
    #[must_use]
    pub const fn new(side_effect: ToolSideEffect) -> Self {
        let allows_parallel = !matches!(side_effect, ToolSideEffect::NonIdempotentWrite);
        Self {
            side_effect,
            allows_parallel,
            requires_idempotency_key: false,
        }
    }

    /// Creates read-only behavior, parallel by default.
    #[must_use]
    pub const fn read_only() -> Self {
        Self::new(ToolSideEffect::ReadOnly)
    }

    /// Creates idempotent-write behavior, parallel by default.
    #[must_use]
    pub const fn idempotent_write() -> Self {
        Self::new(ToolSideEffect::IdempotentWrite)
    }

    /// Creates non-idempotent-write behavior, sequential by default.
    #[must_use]
    pub const fn non_idempotent_write() -> Self {
        Self::new(ToolSideEffect::NonIdempotentWrite)
    }

    /// Explicitly declares whether this tool may overlap another batch call.
    #[must_use]
    pub const fn with_parallel(mut self, allows_parallel: bool) -> Self {
        self.allows_parallel = allows_parallel;
        self
    }

    /// Requires an application-supplied idempotency key.
    #[must_use]
    pub const fn with_required_idempotency_key(mut self, required: bool) -> Self {
        self.requires_idempotency_key = required;
        self
    }

    /// Returns the coarse side-effect class.
    #[must_use]
    pub const fn side_effect(self) -> ToolSideEffect {
        self.side_effect
    }

    /// Returns whether the tool explicitly permits batch overlap.
    #[must_use]
    pub const fn allows_parallel(self) -> bool {
        self.allows_parallel
    }

    /// Returns whether execution requires an idempotency key.
    #[must_use]
    pub const fn requires_idempotency_key(self) -> bool {
        self.requires_idempotency_key
    }

    pub(crate) fn validate(self) -> Result<(), ToolBehaviorError> {
        if matches!(self.side_effect, ToolSideEffect::ReadOnly) && self.requires_idempotency_key {
            return Err(ToolBehaviorError::ReadOnlyRequiresIdempotencyKey);
        }
        if matches!(self.side_effect, ToolSideEffect::NonIdempotentWrite)
            && self.requires_idempotency_key
        {
            return Err(ToolBehaviorError::NonIdempotentWriteRequiresIdempotencyKey);
        }
        Ok(())
    }
}

/// An opaque application idempotency key.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct IdempotencyKey(Arc<str>);

impl IdempotencyKey {
    /// Creates a non-empty, canonical execution key.
    pub fn new(value: impl Into<String>) -> Result<Self, IdempotencyKeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdempotencyKeyError::Empty);
        }
        if value.trim() != value {
            return Err(IdempotencyKeyError::SurroundingWhitespace);
        }
        if value.chars().any(char::is_control) {
            return Err(IdempotencyKeyError::ControlCharacter);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Explicitly returns the key value for a tool implementation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyKey")
            .field("bytes", &self.0.len())
            .finish()
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "IdempotencyKey({} bytes)", self.0.len())
    }
}

/// Borrowed, already validated input supplied to a tool implementation.
#[derive(Clone, Copy)]
pub struct ToolInput<'a> {
    call_id: &'a ToolCallId,
    tool_name: &'a ToolName,
    arguments: &'a Value,
    idempotency_key: Option<&'a IdempotencyKey>,
    metadata: &'a Extensions,
}

impl<'a> ToolInput<'a> {
    pub(crate) const fn new(
        call_id: &'a ToolCallId,
        tool_name: &'a ToolName,
        arguments: &'a Value,
        idempotency_key: Option<&'a IdempotencyKey>,
        metadata: &'a Extensions,
    ) -> Self {
        Self {
            call_id,
            tool_name,
            arguments,
            idempotency_key,
            metadata,
        }
    }

    /// Returns the stable tool call identifier.
    #[must_use]
    pub const fn call_id(self) -> &'a ToolCallId {
        self.call_id
    }

    /// Returns the registered tool name.
    #[must_use]
    pub const fn tool_name(self) -> &'a ToolName {
        self.tool_name
    }

    /// Returns structured arguments already accepted by the cached schema.
    #[must_use]
    pub const fn arguments(self) -> &'a Value {
        self.arguments
    }

    /// Returns the optional application idempotency key.
    #[must_use]
    pub const fn idempotency_key(self) -> Option<&'a IdempotencyKey> {
        self.idempotency_key
    }

    /// Returns provider-neutral execution metadata.
    #[must_use]
    pub const fn metadata(self) -> &'a Extensions {
        self.metadata
    }
}

impl fmt::Debug for ToolInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolInput")
            .field("call_id", &self.call_id)
            .field("tool_name", &self.tool_name)
            .field("arguments_bytes", &self.arguments.to_string().len())
            .field("has_idempotency_key", &self.idempotency_key.is_some())
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Tool-produced content and optional execution metadata.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolOutput {
    result: ToolResult,
    metadata: Extensions,
}

impl ToolOutput {
    /// Wraps an existing model-facing result.
    #[must_use]
    pub const fn new(result: ToolResult) -> Self {
        Self {
            result,
            metadata: Extensions::new(),
        }
    }

    /// Creates a successful text result.
    #[must_use]
    pub fn success_text(text: impl Into<String>) -> Self {
        Self::new(ToolResult::text(text))
    }

    /// Creates a business failure that callers may return to the model.
    #[must_use]
    pub fn business_error_text(text: impl Into<String>) -> Self {
        Self::new(ToolResult::error_text(text))
    }

    /// Creates ordered content with an explicit business-failure flag.
    #[must_use]
    pub const fn from_content(content: Vec<ContentPart>, is_error: bool) -> Self {
        Self::new(ToolResult::new(content, is_error))
    }

    /// Adds provider-neutral execution metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Extensions) -> Self {
        self.metadata = metadata;
        self
    }

    /// Returns the model-facing result.
    #[must_use]
    pub const fn result(&self) -> &ToolResult {
        &self.result
    }

    /// Returns provider-neutral execution metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Extensions {
        &self.metadata
    }

    /// Converts into the existing model-facing result type.
    #[must_use]
    pub fn into_result(self) -> ToolResult {
        self.result
    }
}

impl fmt::Debug for ToolOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolOutput")
            .field("result", &self.result)
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// One object-safe asynchronous local tool.
///
/// Implementations receive no Group `NodeContext` or cancellation token.
/// Dropping the returned future is the cancellation boundary.
#[async_trait]
pub trait Tool: Send + Sync {
    /// Returns the tool's stable advertised name.
    fn name(&self) -> &ToolName;

    /// Returns the immutable model-facing definition.
    fn definition(&self) -> &ToolDefinition;

    /// Returns immutable execution behavior metadata.
    fn behavior(&self) -> ToolBehavior;

    /// Executes one already validated input.
    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError>;
}
