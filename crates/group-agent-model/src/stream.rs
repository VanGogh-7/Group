use std::collections::BTreeMap;
use std::fmt;

use futures_core::Stream;
use futures_util::StreamExt;
use thiserror::Error;

use crate::{
    AssistantMessage, ChatResponse, ContentPart, ExtensionMergeError, Extensions, FinishReason,
    ModelError, ModelErrorKind, ModelId, ResponseId, TokenUsage, TokenUsageError, ToolCall,
    ToolCallId, ToolName,
};

const DEFAULT_MAX_TOOL_CALL_INDEX: u32 = 1_024;
const DEFAULT_MAX_TOOL_CALL_EXTENSIONS: usize = 256;
const DEFAULT_MAX_TEXT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_TOOL_ARGUMENT_BYTES: usize = 16 * 1024 * 1024;

/// One partial tool call in a streamed response.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolCallDelta {
    index: u32,
    id: Option<ToolCallId>,
    name: Option<ToolName>,
    arguments_fragment: String,
    extensions: Extensions,
}

impl ToolCallDelta {
    /// Creates one ordered fragment for a stable tool-call index.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self {
            index,
            id: None,
            name: None,
            arguments_fragment: String::new(),
            extensions: Extensions::new(),
        }
    }

    /// Supplies the call identifier on this fragment.
    #[must_use]
    pub fn with_id(mut self, id: ToolCallId) -> Self {
        self.id = Some(id);
        self
    }

    /// Supplies the tool name on this fragment.
    #[must_use]
    pub fn with_name(mut self, name: ToolName) -> Self {
        self.name = Some(name);
        self
    }

    /// Supplies the next JSON argument fragment.
    #[must_use]
    pub fn with_arguments_fragment(mut self, fragment: impl Into<String>) -> Self {
        self.arguments_fragment = fragment.into();
        self
    }

    /// Supplies provider-neutral continuation metadata for this stable index.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Returns the stable stream index.
    #[must_use]
    pub const fn index(&self) -> u32 {
        self.index
    }

    /// Returns a call identifier supplied by this fragment.
    #[must_use]
    pub const fn id(&self) -> Option<&ToolCallId> {
        self.id.as_ref()
    }

    /// Returns a tool name supplied by this fragment.
    #[must_use]
    pub const fn name(&self) -> Option<&ToolName> {
        self.name.as_ref()
    }

    /// Returns the argument fragment.
    #[must_use]
    pub fn arguments_fragment(&self) -> &str {
        &self.arguments_fragment
    }

    /// Returns continuation metadata supplied by this fragment.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl fmt::Debug for ToolCallDelta {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolCallDelta")
            .field("index", &self.index)
            .field("id", &self.id)
            .field("name", &self.name)
            .field("arguments_fragment_bytes", &self.arguments_fragment.len())
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// One provider-neutral streamed response event.
#[derive(Clone, PartialEq)]
#[non_exhaustive]
pub enum ChatStreamEvent {
    /// Optional response identity and metadata discovered during initialization.
    ResponseStarted {
        response_id: Option<ResponseId>,
        model: Option<ModelId>,
        extensions: Extensions,
    },
    /// Text appended in stream order.
    TextDelta(String),
    /// One fragment of a tool call.
    ToolCallDelta(ToolCallDelta),
    /// A cumulative, independently partial usage snapshot.
    Usage(TokenUsage),
    /// The single logical end marker.
    Finished(FinishReason),
}

impl fmt::Debug for ChatStreamEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResponseStarted {
                response_id,
                model,
                extensions,
            } => formatter
                .debug_struct("ResponseStarted")
                .field("response_id", response_id)
                .field("model", model)
                .field("extensions", extensions)
                .finish(),
            Self::TextDelta(delta) => formatter
                .debug_struct("TextDelta")
                .field("bytes", &delta.len())
                .field("chars", &delta.chars().count())
                .finish(),
            Self::ToolCallDelta(delta) => {
                formatter.debug_tuple("ToolCallDelta").field(delta).finish()
            }
            Self::Usage(usage) => formatter.debug_tuple("Usage").field(usage).finish(),
            Self::Finished(reason) => formatter.debug_tuple("Finished").field(reason).finish(),
        }
    }
}

/// A streaming protocol or aggregation failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StreamProtocolError {
    /// Response metadata appeared more than once.
    #[error("response-started event appeared more than once")]
    DuplicateResponseStarted,
    /// An event appeared after the logical finish marker.
    #[error("stream event `{event}` appeared after finished")]
    EventAfterFinished { event: &'static str },
    /// A prior event permanently poisoned the collector.
    #[error("chat stream collector already failed")]
    CollectorAlreadyFailed,
    /// The transport ended without a logical finish marker.
    #[error("stream closed before a finished event")]
    MissingFinished,
    /// A sparse index exceeded the collector limit.
    #[error("tool call index {index} exceeds configured maximum {maximum}")]
    ToolCallIndexTooLarge { index: u32, maximum: u32 },
    /// A stable tool-call field was supplied more than once.
    #[error("tool call index {index} supplied `{field}` more than once")]
    DuplicateToolCallField { index: u32, field: &'static str },
    /// Tool-call continuation metadata conflicted between fragments.
    #[error("tool call index {index} extension conflict: {source}")]
    ConflictingToolCallExtension {
        index: u32,
        #[source]
        source: ExtensionMergeError,
    },
    /// One tool-call index accumulated too many extension keys.
    #[error("tool call index {index} exceeds extension-key maximum {maximum}")]
    ExtensionLimitExceeded { index: u32, maximum: usize },
    /// Accumulated assistant text exceeded the configured byte maximum.
    #[error("stream text exceeds configured byte maximum {maximum}")]
    TextLimitExceeded { maximum: usize },
    /// Accumulated arguments for one tool call exceeded the configured byte
    /// maximum.
    #[error("tool call index {index} arguments exceed byte maximum {maximum}")]
    ToolArgumentsLimitExceeded { index: u32, maximum: usize },
    /// Cumulative usage decreased or became inconsistent.
    #[error("invalid cumulative usage: {source}")]
    InvalidUsage {
        #[source]
        source: TokenUsageError,
    },
    /// A complete tool call lacked an identifier.
    #[error("tool call index {index} has no id")]
    MissingToolCallId { index: u32 },
    /// A complete tool call lacked a name.
    #[error("tool call index {index} has no name")]
    MissingToolCallName { index: u32 },
    /// Concatenated arguments were not valid JSON.
    #[error("tool call index {index} arguments are invalid JSON: {source}")]
    InvalidToolArguments {
        index: u32,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Default)]
struct PendingToolCall {
    id: Option<ToolCallId>,
    name: Option<ToolName>,
    arguments: String,
    extensions: Extensions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CollectorState {
    Active,
    Finished,
    Failed,
}

/// Stateful protocol validator and response accumulator.
///
/// Tool calls are stored in a map rather than a vector, so a sparse hostile
/// index cannot trigger proportional allocation. The default maximum accepted
/// index is 1024 and can be lowered for an application.
///
/// Each event is validated in full before its effects are committed. A
/// rejected event contributes no response data and permanently moves an
/// active collector to a failed state. Every later [`Self::push`] and
/// [`Self::finish`] returns
/// [`StreamProtocolError::CollectorAlreadyFailed`]. A successfully finished
/// collector rejects every later event as
/// [`StreamProtocolError::EventAfterFinished`].
///
/// Manual collection with this type and [`collect_chat_stream`] are
/// alternatives: the helper constructs and owns its own collector.
pub struct ChatStreamCollector {
    state: CollectorState,
    response_id: Option<ResponseId>,
    model: Option<ModelId>,
    extensions: Extensions,
    response_started: bool,
    text: String,
    tool_calls: BTreeMap<u32, PendingToolCall>,
    usage: Option<TokenUsage>,
    finish_reason: Option<FinishReason>,
    max_tool_call_index: u32,
    max_tool_call_extensions: usize,
    max_text_bytes: usize,
    max_tool_argument_bytes: usize,
}

impl ChatStreamCollector {
    /// Creates an empty response accumulator.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: CollectorState::Active,
            response_id: None,
            model: None,
            extensions: Extensions::new(),
            response_started: false,
            text: String::new(),
            tool_calls: BTreeMap::new(),
            usage: None,
            finish_reason: None,
            max_tool_call_index: DEFAULT_MAX_TOOL_CALL_INDEX,
            max_tool_call_extensions: DEFAULT_MAX_TOOL_CALL_EXTENSIONS,
            max_text_bytes: DEFAULT_MAX_TEXT_BYTES,
            max_tool_argument_bytes: DEFAULT_MAX_TOOL_ARGUMENT_BYTES,
        }
    }

    /// Sets the largest accepted stable tool-call index.
    #[must_use]
    pub const fn with_max_tool_call_index(mut self, maximum: u32) -> Self {
        self.max_tool_call_index = maximum;
        self
    }

    /// Sets the largest number of distinct extension keys accepted for one
    /// stable tool-call index.
    #[must_use]
    pub const fn with_max_tool_call_extensions(mut self, maximum: usize) -> Self {
        self.max_tool_call_extensions = maximum;
        self
    }

    /// Sets the largest accumulated assistant-text byte length.
    #[must_use]
    pub const fn with_max_text_bytes(mut self, maximum: usize) -> Self {
        self.max_text_bytes = maximum;
        self
    }

    /// Sets the largest accumulated argument byte length for each tool-call
    /// index.
    #[must_use]
    pub const fn with_max_tool_argument_bytes(mut self, maximum: usize) -> Self {
        self.max_tool_argument_bytes = maximum;
        self
    }

    /// Validates and atomically applies one event.
    ///
    /// The first error while active permanently poisons this collector.
    pub fn push(&mut self, event: ChatStreamEvent) -> Result<(), ModelError> {
        match self.state {
            CollectorState::Failed => {
                return Err(protocol_error(StreamProtocolError::CollectorAlreadyFailed));
            }
            CollectorState::Finished => {
                return Err(protocol_error(StreamProtocolError::EventAfterFinished {
                    event: event_name(&event),
                }));
            }
            CollectorState::Active => {}
        }

        let result = self.push_active(event);
        if result.is_err() {
            self.state = CollectorState::Failed;
        }
        result
    }

    fn push_active(&mut self, event: ChatStreamEvent) -> Result<(), ModelError> {
        match event {
            ChatStreamEvent::ResponseStarted {
                response_id,
                model,
                extensions,
            } => {
                if self.response_started {
                    return Err(protocol_error(
                        StreamProtocolError::DuplicateResponseStarted,
                    ));
                }
                self.response_started = true;
                self.response_id = response_id;
                self.model = model;
                self.extensions = extensions;
            }
            ChatStreamEvent::TextDelta(delta) => {
                let new_len = self
                    .text
                    .len()
                    .checked_add(delta.len())
                    .filter(|length| *length <= self.max_text_bytes)
                    .ok_or_else(|| {
                        protocol_error(StreamProtocolError::TextLimitExceeded {
                            maximum: self.max_text_bytes,
                        })
                    })?;
                self.text.reserve(new_len - self.text.len());
                self.text.push_str(&delta);
            }
            ChatStreamEvent::ToolCallDelta(delta) => self.push_tool_delta(delta)?,
            ChatStreamEvent::Usage(usage) => {
                if let Some(existing) = &mut self.usage {
                    existing.merge_snapshot(usage).map_err(|source| {
                        protocol_error(StreamProtocolError::InvalidUsage { source })
                    })?;
                } else {
                    self.usage = Some(usage);
                }
            }
            ChatStreamEvent::Finished(reason) => {
                self.validate_finished()?;
                self.finish_reason = Some(reason);
                self.state = CollectorState::Finished;
            }
        }
        Ok(())
    }

    /// Completes aggregation after transport EOF.
    pub fn finish(self) -> Result<ChatResponse, ModelError> {
        match self.state {
            CollectorState::Failed => {
                return Err(protocol_error(StreamProtocolError::CollectorAlreadyFailed));
            }
            CollectorState::Active => {
                return Err(protocol_error(StreamProtocolError::MissingFinished));
            }
            CollectorState::Finished => {}
        }

        let finish_reason = self
            .finish_reason
            .ok_or_else(|| protocol_error(StreamProtocolError::MissingFinished))?;

        let mut calls = Vec::with_capacity(self.tool_calls.len());
        for (index, pending) in self.tool_calls {
            let id = pending
                .id
                .ok_or_else(|| protocol_error(StreamProtocolError::MissingToolCallId { index }))?;
            let name = pending.name.ok_or_else(|| {
                protocol_error(StreamProtocolError::MissingToolCallName { index })
            })?;
            let arguments = serde_json::from_str(&pending.arguments).map_err(|source| {
                decode_error(StreamProtocolError::InvalidToolArguments { index, source })
            })?;
            calls.push(ToolCall::new(id, name, arguments).with_extensions(pending.extensions));
        }

        let content = if self.text.is_empty() {
            Vec::new()
        } else {
            vec![ContentPart::text(self.text)]
        };
        let mut response = ChatResponse::new(AssistantMessage::new(content, calls), finish_reason)
            .with_extensions(self.extensions);
        if let Some(usage) = self.usage {
            response = response.with_usage(usage);
        }
        if let Some(response_id) = self.response_id {
            response = response.with_response_id(response_id);
        }
        if let Some(model) = self.model {
            response = response.with_model(model);
        }
        Ok(response)
    }

    fn push_tool_delta(&mut self, delta: ToolCallDelta) -> Result<(), ModelError> {
        let index = delta.index;
        if index > self.max_tool_call_index {
            return Err(protocol_error(StreamProtocolError::ToolCallIndexTooLarge {
                index,
                maximum: self.max_tool_call_index,
            }));
        }

        let empty_extensions = Extensions::new();
        let existing = self.tool_calls.get(&index);
        if delta.id.is_some() && existing.is_some_and(|pending| pending.id.is_some()) {
            return Err(protocol_error(
                StreamProtocolError::DuplicateToolCallField { index, field: "id" },
            ));
        }
        if delta.name.is_some() && existing.is_some_and(|pending| pending.name.is_some()) {
            return Err(protocol_error(
                StreamProtocolError::DuplicateToolCallField {
                    index,
                    field: "name",
                },
            ));
        }

        let existing_arguments = existing.map_or(0, |pending| pending.arguments.len());
        existing_arguments
            .checked_add(delta.arguments_fragment.len())
            .filter(|length| *length <= self.max_tool_argument_bytes)
            .ok_or_else(|| {
                protocol_error(StreamProtocolError::ToolArgumentsLimitExceeded {
                    index,
                    maximum: self.max_tool_argument_bytes,
                })
            })?;

        let existing_extensions = existing
            .map(|pending| &pending.extensions)
            .unwrap_or(&empty_extensions);
        let new_keys = existing_extensions
            .validate_idempotent_merge(&delta.extensions)
            .map_err(|source| {
                protocol_error(StreamProtocolError::ConflictingToolCallExtension { index, source })
            })?;
        if existing_extensions
            .len()
            .checked_add(new_keys)
            .is_none_or(|length| length > self.max_tool_call_extensions)
        {
            return Err(protocol_error(
                StreamProtocolError::ExtensionLimitExceeded {
                    index,
                    maximum: self.max_tool_call_extensions,
                },
            ));
        }

        let pending = self.tool_calls.entry(index).or_default();
        if let Some(id) = delta.id {
            pending.id = Some(id);
        }
        if let Some(name) = delta.name {
            pending.name = Some(name);
        }
        pending.arguments.push_str(&delta.arguments_fragment);
        pending.extensions.commit_idempotent_merge(delta.extensions);
        Ok(())
    }

    fn validate_finished(&self) -> Result<(), ModelError> {
        for (index, pending) in &self.tool_calls {
            if pending.id.is_none() {
                return Err(protocol_error(StreamProtocolError::MissingToolCallId {
                    index: *index,
                }));
            }
            if pending.name.is_none() {
                return Err(protocol_error(StreamProtocolError::MissingToolCallName {
                    index: *index,
                }));
            }
            serde_json::from_str::<serde_json::Value>(&pending.arguments).map_err(|source| {
                decode_error(StreamProtocolError::InvalidToolArguments {
                    index: *index,
                    source,
                })
            })?;
        }
        Ok(())
    }
}

impl Default for ChatStreamCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Consumes a stream, stops on its first item error, and aggregates a response.
pub async fn collect_chat_stream<S>(stream: S) -> Result<ChatResponse, ModelError>
where
    S: Stream<Item = Result<ChatStreamEvent, ModelError>> + Send,
{
    futures_util::pin_mut!(stream);
    let mut collector = ChatStreamCollector::new();
    while let Some(event) = stream.next().await {
        collector.push(event?)?;
    }
    collector.finish()
}

fn event_name(event: &ChatStreamEvent) -> &'static str {
    match event {
        ChatStreamEvent::ResponseStarted { .. } => "response_started",
        ChatStreamEvent::TextDelta(_) => "text_delta",
        ChatStreamEvent::ToolCallDelta(_) => "tool_call_delta",
        ChatStreamEvent::Usage(_) => "usage",
        ChatStreamEvent::Finished(_) => "finished",
    }
}

fn protocol_error(source: StreamProtocolError) -> ModelError {
    ModelError::with_source(
        ModelErrorKind::Protocol,
        "chat stream protocol violation",
        source,
    )
}

fn decode_error(source: StreamProtocolError) -> ModelError {
    ModelError::with_source(
        ModelErrorKind::Decode,
        "chat stream data could not be decoded",
        source,
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn id(value: &str) -> ToolCallId {
        ToolCallId::new(value).expect("valid call id")
    }

    fn name(value: &str) -> ToolName {
        ToolName::new(value).expect("valid tool name")
    }

    fn extension(key: &str, value: serde_json::Value) -> Extensions {
        Extensions::new().with(key, value).expect("valid extension")
    }

    #[test]
    fn conflicting_tool_delta_commits_none_of_its_fields() {
        let mut collector = ChatStreamCollector::new();
        collector
            .push(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0)
                    .with_id(id("original"))
                    .with_arguments_fragment("{\"ok\":")
                    .with_extensions(extension("m", json!(1))),
            ))
            .expect("first fragment");

        collector
            .push(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0)
                    .with_name(name("lookup"))
                    .with_arguments_fragment("true}")
                    .with_extensions(
                        Extensions::try_from_iter([
                            ("a", json!("must-not-commit")),
                            ("m", json!("conflict")),
                        ])
                        .expect("valid fragment extensions"),
                    ),
            ))
            .expect_err("conflict");

        assert_eq!(collector.state, CollectorState::Failed);
        let pending = collector.tool_calls.get(&0).expect("original call");
        assert_eq!(
            pending.id.as_ref().map(ToolCallId::as_str),
            Some("original")
        );
        assert!(pending.name.is_none());
        assert_eq!(pending.arguments, "{\"ok\":");
        assert_eq!(pending.extensions.keys().collect::<Vec<_>>(), ["m"]);
    }

    #[test]
    fn duplicate_id_and_extension_limit_commit_no_sibling_data() {
        let mut duplicate = ChatStreamCollector::new();
        duplicate
            .push(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0).with_id(id("original")),
            ))
            .expect("first id");
        duplicate
            .push(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0)
                    .with_id(id("duplicate"))
                    .with_name(name("must-not-commit"))
                    .with_arguments_fragment("{}")
                    .with_extensions(extension("must-not-commit", json!(1))),
            ))
            .expect_err("duplicate id");
        let pending = duplicate.tool_calls.get(&0).expect("original call");
        assert_eq!(
            pending.id.as_ref().map(ToolCallId::as_str),
            Some("original")
        );
        assert!(pending.name.is_none());
        assert!(pending.arguments.is_empty());
        assert!(pending.extensions.is_empty());

        let mut bounded = ChatStreamCollector::new().with_max_tool_call_extensions(0);
        bounded
            .push(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0)
                    .with_id(id("must-not-commit"))
                    .with_name(name("must-not-commit"))
                    .with_arguments_fragment("{}")
                    .with_extensions(extension("too-many", json!(1))),
            ))
            .expect_err("extension limit");
        assert!(!bounded.tool_calls.contains_key(&0));
    }

    #[test]
    fn text_usage_start_and_finished_failures_leave_event_data_uncommitted() {
        let mut text = ChatStreamCollector::new().with_max_text_bytes(3);
        text.push(ChatStreamEvent::TextDelta("abc".to_owned()))
            .expect("at limit");
        text.push(ChatStreamEvent::TextDelta("d".to_owned()))
            .expect_err("over limit");
        assert_eq!(text.text, "abc");

        let mut usage = ChatStreamCollector::new();
        usage
            .push(ChatStreamEvent::Usage(
                TokenUsage::from_parts(Some(5), None, None).expect("usage"),
            ))
            .expect("first usage");
        usage
            .push(ChatStreamEvent::Usage(
                TokenUsage::from_parts(Some(4), Some(2), None).expect("standalone usage"),
            ))
            .expect_err("decrease");
        let retained = usage.usage.as_ref().expect("retained usage");
        assert_eq!(retained.input_tokens(), Some(5));
        assert_eq!(retained.output_tokens(), None);

        let mut started = ChatStreamCollector::new();
        started
            .push(ChatStreamEvent::ResponseStarted {
                response_id: Some(ResponseId::new("first").expect("valid response id")),
                model: None,
                extensions: Extensions::new(),
            })
            .expect("first start");
        started
            .push(ChatStreamEvent::ResponseStarted {
                response_id: Some(ResponseId::new("second").expect("valid response id")),
                model: Some(ModelId::new("must-not-commit").expect("valid model id")),
                extensions: extension("must-not-commit", json!(1)),
            })
            .expect_err("duplicate start");
        assert_eq!(
            started.response_id.as_ref().map(ResponseId::as_str),
            Some("first")
        );
        assert!(started.model.is_none());
        assert!(started.extensions.is_empty());

        let mut finished = ChatStreamCollector::new();
        finished
            .push(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0).with_name(name("lookup")),
            ))
            .expect("partial call");
        finished
            .push(ChatStreamEvent::Finished(FinishReason::ToolCalls))
            .expect_err("incomplete call");
        assert_eq!(finished.state, CollectorState::Failed);
        assert!(finished.finish_reason.is_none());
    }
}
