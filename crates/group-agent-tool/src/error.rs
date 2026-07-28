use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use group_agent_model::{ToolCallId, ToolName};
use thiserror::Error;

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// Why an idempotency key could not be constructed.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum IdempotencyKeyError {
    /// The key was empty.
    #[error("idempotency key must not be empty")]
    Empty,
    /// Leading or trailing whitespace would make key identity ambiguous.
    #[error("idempotency key must not contain leading or trailing whitespace")]
    SurroundingWhitespace,
    /// Control characters are not accepted in an execution key.
    #[error("idempotency key must not contain control characters")]
    ControlCharacter,
}

/// An internally inconsistent tool behavior declaration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolBehaviorError {
    /// Read-only operations do not use write-idempotency keys.
    #[error("a read-only tool cannot require an idempotency key")]
    ReadOnlyRequiresIdempotencyKey,
    /// A write made safe by a mandatory key is idempotent behavior.
    #[error("a non-idempotent write cannot require an idempotency key")]
    NonIdempotentWriteRequiresIdempotencyKey,
}

/// A redacted JSON Schema validation location.
///
/// This type retains JSON pointers and the failing keyword, but never the
/// schema or instance value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaViolation {
    instance_path: Arc<str>,
    schema_path: Arc<str>,
    keyword: Arc<str>,
}

impl SchemaViolation {
    pub(crate) fn from_error(error: &jsonschema::ValidationError<'_>) -> Self {
        Self {
            instance_path: Arc::from(error.instance_path().to_string()),
            schema_path: Arc::from(error.schema_path().to_string()),
            keyword: Arc::from(error.kind().keyword()),
        }
    }

    /// Returns the JSON Pointer to the rejected instance value.
    #[must_use]
    pub fn instance_path(&self) -> &str {
        &self.instance_path
    }

    /// Returns the JSON Pointer to the rejecting schema keyword.
    #[must_use]
    pub fn schema_path(&self) -> &str {
        &self.schema_path
    }

    /// Returns the JSON Schema keyword that rejected the value.
    #[must_use]
    pub fn keyword(&self) -> &str {
        &self.keyword
    }
}

impl fmt::Display for SchemaViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "keyword `{}` rejected instance path `{}` at schema path `{}`",
            self.keyword, self.instance_path, self.schema_path
        )
    }
}

/// Why a tool definition could not be registered.
///
/// Default formatting is payload-safe. For an invalid schema, explicit
/// [`StdError::source`] traversal reaches the concrete
/// `jsonschema::ValidationError`; callers are responsible for filtering that
/// upstream diagnostic before logging it.
#[non_exhaustive]
pub enum ToolDefinitionError {
    /// The advertised name did not match the cached definition.
    NameMismatch {
        advertised: ToolName,
        defined: ToolName,
    },
    /// A tool name was not canonical enough for local execution.
    InvalidName,
    /// A useful model-facing description was not supplied.
    EmptyDescription,
    /// The input schema could not be compiled.
    InvalidSchema {
        violation: SchemaViolation,
        source: jsonschema::ValidationError<'static>,
    },
    /// Behavior metadata was internally inconsistent.
    InvalidBehavior { source: ToolBehaviorError },
}

impl fmt::Display for ToolDefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NameMismatch {
                advertised,
                defined,
            } => write!(
                formatter,
                "advertised tool name `{advertised}` does not match definition name `{defined}`"
            ),
            Self::InvalidName => {
                formatter.write_str("tool name must be trimmed and contain no control characters")
            }
            Self::EmptyDescription => formatter.write_str("tool description must not be empty"),
            Self::InvalidSchema { violation, .. } => {
                write!(formatter, "tool input schema is invalid: {violation}")
            }
            Self::InvalidBehavior { source } => {
                write!(formatter, "tool behavior is invalid: {source}")
            }
        }
    }
}

impl fmt::Debug for ToolDefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NameMismatch {
                advertised,
                defined,
            } => formatter
                .debug_struct("NameMismatch")
                .field("advertised", advertised)
                .field("defined", defined)
                .finish(),
            Self::InvalidName => formatter.write_str("InvalidName"),
            Self::EmptyDescription => formatter.write_str("EmptyDescription"),
            Self::InvalidSchema { violation, .. } => formatter
                .debug_struct("InvalidSchema")
                .field("violation", violation)
                .field("has_source", &true)
                .finish(),
            Self::InvalidBehavior { source } => formatter
                .debug_struct("InvalidBehavior")
                .field("source", source)
                .finish(),
        }
    }
}

impl StdError for ToolDefinitionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::InvalidSchema { source, .. } => Some(source),
            Self::InvalidBehavior { source } => Some(source),
            _ => None,
        }
    }
}

/// A deterministic registry construction failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ToolRegistryError {
    /// A tool definition or behavior declaration was invalid.
    #[error("tool `{tool_name}` has an invalid definition: {source}")]
    InvalidDefinition {
        tool_name: ToolName,
        #[source]
        source: ToolDefinitionError,
    },
    /// The stable registry name was already occupied.
    #[error("tool `{tool_name}` is already registered")]
    DuplicateTool { tool_name: ToolName },
}

/// Stable classification shared by runtime errors and redacted events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolRuntimeErrorKind {
    InvalidDefinition,
    DuplicateTool,
    ToolNotFound,
    InvalidToolCall,
    InvalidArguments,
    UnsupportedExecution,
    MissingIdempotencyKey,
    TimedOut,
    ExecutionFailed,
    Cancelled,
    NotStartedDueToFailFast,
    ObserverFailed,
    Other,
}

impl ToolRegistryError {
    /// Returns the stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> ToolRuntimeErrorKind {
        match self {
            Self::InvalidDefinition { .. } => ToolRuntimeErrorKind::InvalidDefinition,
            Self::DuplicateTool { .. } => ToolRuntimeErrorKind::DuplicateTool,
        }
    }
}

/// Safe identity and input position for one tool execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolCallContext {
    call_id: ToolCallId,
    tool_name: ToolName,
    batch_index: Option<usize>,
}

impl ToolCallContext {
    pub(crate) const fn new(
        call_id: ToolCallId,
        tool_name: ToolName,
        batch_index: Option<usize>,
    ) -> Self {
        Self {
            call_id,
            tool_name,
            batch_index,
        }
    }

    /// Returns the stable model-produced call identifier.
    #[must_use]
    pub const fn call_id(&self) -> &ToolCallId {
        &self.call_id
    }

    /// Returns the registered tool name.
    #[must_use]
    pub const fn tool_name(&self) -> &ToolName {
        &self.tool_name
    }

    /// Returns the original batch index, or `None` for a single call.
    #[must_use]
    pub const fn batch_index(&self) -> Option<usize> {
        self.batch_index
    }
}

/// A source-preserving, payload-redacted tool runtime failure.
pub struct ToolRuntimeError {
    kind: ToolRuntimeErrorKind,
    context: ToolCallContext,
    violation: Option<Box<SchemaViolation>>,
    timeout: Option<Duration>,
    source: Option<BoxedError>,
}

impl ToolRuntimeError {
    pub(crate) const fn new(kind: ToolRuntimeErrorKind, context: ToolCallContext) -> Self {
        Self {
            kind,
            context,
            violation: None,
            timeout: None,
            source: None,
        }
    }

    pub(crate) fn with_source<E>(
        kind: ToolRuntimeErrorKind,
        context: ToolCallContext,
        source: E,
    ) -> Self
    where
        E: Into<BoxedError>,
    {
        let mut error = Self::new(kind, context);
        error.source = Some(source.into());
        error
    }

    pub(crate) fn invalid_arguments(
        context: ToolCallContext,
        violation: SchemaViolation,
        source: jsonschema::ValidationError<'static>,
    ) -> Self {
        let mut error = Self::new(ToolRuntimeErrorKind::InvalidArguments, context);
        error.violation = Some(Box::new(violation));
        error.source = Some(Box::new(source));
        error
    }

    pub(crate) fn observer_failed(context: ToolCallContext, source: ToolObserverFailure) -> Self {
        Self::with_source(ToolRuntimeErrorKind::ObserverFailed, context, source)
    }

    pub(crate) fn timed_out<E>(context: ToolCallContext, timeout: Duration, source: E) -> Self
    where
        E: Into<BoxedError>,
    {
        let mut error = Self::with_source(ToolRuntimeErrorKind::TimedOut, context, source);
        error.timeout = Some(timeout);
        error
    }

    /// Returns the stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> ToolRuntimeErrorKind {
        self.kind
    }

    /// Returns safe call identity and batch position.
    #[must_use]
    pub const fn context(&self) -> &ToolCallContext {
        &self.context
    }

    /// Returns a redacted schema location for invalid arguments.
    #[must_use]
    pub fn schema_violation(&self) -> Option<&SchemaViolation> {
        self.violation.as_deref()
    }

    /// Returns the configured timeout when it elapsed.
    #[must_use]
    pub const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}

impl fmt::Display for ToolRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tool runtime error ({:?}) for call `{}` and tool `{}`",
            self.kind, self.context.call_id, self.context.tool_name
        )?;
        if let Some(index) = self.context.batch_index {
            write!(formatter, " at batch index {index}")?;
        }
        if let Some(violation) = &self.violation {
            write!(formatter, ": {violation}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ToolRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRuntimeError")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("violation", &self.violation)
            .field("timeout", &self.timeout)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl StdError for ToolRuntimeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// Why a tool implementation failed to complete its infrastructure work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolErrorKind {
    Cancelled,
    Other,
}

/// A source-preserving error returned by a [`crate::Tool`].
pub struct ToolError {
    kind: ToolErrorKind,
    message: String,
    source: Option<BoxedError>,
}

impl ToolError {
    /// Creates a classified message-only error.
    #[must_use]
    pub fn new(kind: ToolErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// Creates a classified error while retaining its concrete source.
    #[must_use]
    pub fn with_source<E>(kind: ToolErrorKind, message: impl Into<String>, source: E) -> Self
    where
        E: Into<BoxedError>,
    {
        let mut error = Self::new(kind, message);
        error.source = Some(source.into());
        error
    }

    /// Returns the tool-owned failure classification.
    #[must_use]
    pub const fn kind(&self) -> ToolErrorKind {
        self.kind
    }

    /// Explicitly exposes the tool-supplied message.
    #[must_use]
    pub fn as_message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "tool execution error ({:?})", self.kind)
    }
}

impl fmt::Debug for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolError")
            .field("kind", &self.kind)
            .field("message_redacted", &true)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl StdError for ToolError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// A caller-defined observer error.
///
/// The message and optional source are available only through explicit
/// accessors or source traversal. Default formatting is redacted.
pub struct ToolObserverError {
    message: String,
    source: Option<BoxedError>,
}

impl ToolObserverError {
    /// Creates a message-only observer error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Creates an observer error while retaining its concrete source.
    #[must_use]
    pub fn with_source<E>(message: impl Into<String>, source: E) -> Self
    where
        E: Into<BoxedError>,
    {
        let mut error = Self::new(message);
        error.source = Some(source.into());
        error
    }

    /// Explicitly exposes the observer-supplied message.
    #[must_use]
    pub fn as_message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ToolObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("tool observer error")
    }
}

impl fmt::Debug for ToolObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolObserverError")
            .field("message_redacted", &true)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl StdError for ToolObserverError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// Stable classification for a caught observer failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolObserverFailureKind {
    /// The observer returned an error.
    ReturnedError,
    /// The observer panicked. The panic payload was discarded without formatting.
    Panicked,
}

/// A redacted observer failure caught at an execution boundary.
pub struct ToolObserverFailure {
    kind: ToolObserverFailureKind,
    source: Option<ToolObserverError>,
}

impl ToolObserverFailure {
    pub(crate) const fn returned(source: ToolObserverError) -> Self {
        Self {
            kind: ToolObserverFailureKind::ReturnedError,
            source: Some(source),
        }
    }

    pub(crate) const fn panicked() -> Self {
        Self {
            kind: ToolObserverFailureKind::Panicked,
            source: None,
        }
    }

    /// Returns whether the callback returned an error or panicked.
    #[must_use]
    pub const fn kind(&self) -> ToolObserverFailureKind {
        self.kind
    }
}

impl fmt::Display for ToolObserverFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "tool observer failed ({:?})", self.kind)
    }
}

impl fmt::Debug for ToolObserverFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolObserverFailure")
            .field("kind", &self.kind)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl StdError for ToolObserverFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// A batch-level failure detected before ordinary per-call results can be returned.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolBatchError {
    /// A zero concurrency limit cannot make progress.
    #[error("tool batch max concurrency must be greater than zero")]
    ZeroConcurrency,
    /// One call identifier appeared more than once.
    #[error(
        "tool call id `{call_id}` is duplicated at batch indices {first_index} and \
         {duplicate_index}"
    )]
    DuplicateToolCallId {
        call_id: ToolCallId,
        first_index: usize,
        duplicate_index: usize,
    },
}
