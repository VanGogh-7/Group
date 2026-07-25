use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use group_agent_model::{
    ChatStreamEvent, Extensions, ModelError, ModelId, ProviderId, ResponseId, ToolCallDelta,
    ToolCallId, ToolName,
};

use crate::GenaiMappingError;
use crate::config::GenaiAdapterConfig;
use crate::error::map_genai_error;
use crate::extensions::{
    ADAPTER_KIND, RAW_STOP_REASON, REASONING_CONTENT, RESOLVED_MODEL, THOUGHT_SIGNATURES,
    insert_string, insert_string_list,
};
use crate::response::{map_finish_reason, reconcile_signatures};
use crate::usage::map_usage;

pub(crate) struct GenaiEventStream<S = genai::chat::ChatStream> {
    inner: S,
    resolved_model: genai::ModelIden,
    provider: ProviderId,
    model: ModelId,
    config: GenaiAdapterConfig,
    pending: VecDeque<Result<ChatStreamEvent, ModelError>>,
    saw_start: bool,
    saw_end: bool,
    terminal: bool,
    tool_calls: BTreeMap<String, StreamToolCall>,
    next_tool_index: u32,
    reasoning: String,
    thought_signatures: Vec<String>,
    thought_signature_bytes: usize,
}

struct StreamToolCall {
    index: u32,
    id_emitted: bool,
    name: Option<String>,
    arguments: String,
}

impl GenaiEventStream<genai::chat::ChatStream> {
    pub(crate) fn new(
        response: genai::chat::ChatStreamResponse,
        config: GenaiAdapterConfig,
        provider: ProviderId,
        model: ModelId,
    ) -> Self {
        Self::from_inner(
            response.stream,
            response.model_iden,
            config,
            provider,
            model,
        )
    }
}

impl<S> GenaiEventStream<S> {
    fn from_inner(
        inner: S,
        resolved_model: genai::ModelIden,
        config: GenaiAdapterConfig,
        provider: ProviderId,
        model: ModelId,
    ) -> Self {
        Self {
            inner,
            resolved_model,
            provider,
            model,
            config,
            pending: VecDeque::new(),
            saw_start: false,
            saw_end: false,
            terminal: false,
            tool_calls: BTreeMap::new(),
            next_tool_index: 0,
            reasoning: String::new(),
            thought_signatures: Vec::new(),
            thought_signature_bytes: 0,
        }
    }

    fn fail(
        &mut self,
        error: GenaiMappingError,
    ) -> Poll<Option<Result<ChatStreamEvent, ModelError>>> {
        self.terminal = true;
        let error = error.into_model_error(&self.provider, &self.model);
        Poll::Ready(Some(Err(error)))
    }

    fn push_reasoning(&mut self, chunk: String) -> Result<(), GenaiMappingError> {
        if !self.config.retain_reasoning_content() {
            return Ok(());
        }
        let maximum = self.config.streaming_limits().max_reasoning_bytes();
        self.reasoning
            .len()
            .checked_add(chunk.len())
            .filter(|length| *length <= maximum)
            .ok_or(GenaiMappingError::ReasoningLimitExceeded { maximum })?;
        self.reasoning.push_str(&chunk);
        Ok(())
    }

    fn push_signature(&mut self, signature: String) -> Result<(), GenaiMappingError> {
        let maximum = self.config.streaming_limits().max_thought_signature_bytes();
        let next = checked_signature_total(self.thought_signature_bytes, signature.len(), maximum)?;
        self.thought_signature_bytes = next;
        self.thought_signatures.push(signature);
        Ok(())
    }

    fn normalize_tool_call(
        &mut self,
        call: genai::chat::ToolCall,
        terminal_capture: bool,
    ) -> Result<Option<ChatStreamEvent>, GenaiMappingError> {
        if call.call_id.trim().is_empty() {
            return Err(GenaiMappingError::EmptyStreamToolCallId);
        }
        if let Some(signatures) = call.thought_signatures.as_ref() {
            let reconciled = reconcile_signatures(&self.thought_signatures, Some(signatures))?;
            if self.thought_signatures.is_empty() {
                for signature in reconciled {
                    self.push_signature(signature)?;
                }
            }
        }

        let call_id = call.call_id;
        let incoming_name = (!call.fn_name.is_empty()).then_some(call.fn_name);
        let incoming_arguments = call.fn_arguments;

        if !self.tool_calls.contains_key(&call_id) {
            if self.next_tool_index >= self.config.streaming_limits().max_tool_calls() {
                return Err(GenaiMappingError::StreamToolCallLimit {
                    maximum: self.config.streaming_limits().max_tool_calls(),
                });
            }
            let index = self.next_tool_index;
            self.next_tool_index = self.next_tool_index.checked_add(1).ok_or(
                GenaiMappingError::StreamToolCallLimit {
                    maximum: self.config.streaming_limits().max_tool_calls(),
                },
            )?;
            self.tool_calls.insert(
                call_id.clone(),
                StreamToolCall {
                    index,
                    id_emitted: false,
                    name: None,
                    arguments: String::new(),
                },
            );
        }

        let state = self
            .tool_calls
            .get_mut(&call_id)
            .expect("tool call was inserted");
        let mut delta = ToolCallDelta::new(state.index);
        let mut has_event_data = false;
        if !state.id_emitted {
            delta = delta.with_id(ToolCallId::new(call_id.clone()).map_err(|source| {
                GenaiMappingError::InvalidIdentifier {
                    field: "stream.tool_call.call_id",
                    source,
                }
            })?);
            state.id_emitted = true;
            has_event_data = true;
        }

        if let Some(name) = incoming_name {
            match &state.name {
                Some(existing) if existing != &name => {
                    return Err(GenaiMappingError::ConflictingTerminalToolCall);
                }
                Some(_) => {}
                None => {
                    delta = delta.with_name(ToolName::new(name.clone()).map_err(|source| {
                        GenaiMappingError::InvalidIdentifier {
                            field: "stream.tool_call.fn_name",
                            source,
                        }
                    })?);
                    state.name = Some(name);
                    has_event_data = true;
                }
            }
        }

        let fragment = if terminal_capture {
            terminal_arguments_fragment(&state.arguments, &incoming_arguments)?
        } else {
            let current_raw = incoming_arguments
                .as_str()
                .ok_or(GenaiMappingError::UnexpectedStreamToolArgumentsKind)?;
            cumulative_arguments_fragment(&state.arguments, current_raw)?
        };
        if !fragment.is_empty() {
            state.arguments.push_str(&fragment);
            delta = delta.with_arguments_fragment(fragment);
            has_event_data = true;
        }

        Ok(has_event_data.then_some(ChatStreamEvent::ToolCallDelta(delta)))
    }

    fn handle_end(&mut self, end: genai::chat::StreamEnd) -> Result<(), GenaiMappingError> {
        let mut captured_signatures = Vec::new();
        let mut captured_calls = Vec::new();
        if let Some(content) = end.captured_content {
            for part in content.into_parts() {
                match part {
                    genai::chat::ContentPart::ThoughtSignature(signature) => {
                        captured_signatures.push(signature);
                    }
                    genai::chat::ContentPart::ToolCall(call) => captured_calls.push(call),
                    genai::chat::ContentPart::Text(_) => {
                        // capture_content is disabled, so terminal text must not be
                        // replayed after already emitted chunks.
                    }
                    genai::chat::ContentPart::ReasoningContent(reasoning) => {
                        if self.config.retain_reasoning_content() {
                            self.push_reasoning(reasoning)?;
                        }
                    }
                    genai::chat::ContentPart::Binary(_) => {
                        return Err(GenaiMappingError::UnsupportedResponseContent {
                            kind: "Binary",
                        });
                    }
                    genai::chat::ContentPart::Custom(_) => {
                        return Err(GenaiMappingError::UnsupportedResponseContent {
                            kind: "Custom",
                        });
                    }
                    genai::chat::ContentPart::ToolResponse(_) => {
                        return Err(GenaiMappingError::UnsupportedResponseContent {
                            kind: "ToolResponse in assistant stream",
                        });
                    }
                }
            }
        }

        let signatures =
            reconcile_signatures(&self.thought_signatures, Some(&captured_signatures))?;
        if self.thought_signatures.is_empty() {
            for signature in signatures {
                self.push_signature(signature)?;
            }
        }

        let mut terminal_tool_events = Vec::new();
        if !captured_calls.is_empty() {
            return Err(GenaiMappingError::UnexpectedToolCallInTextOnlyStream);
        }
        for call in captured_calls {
            if let Some(event) = self.normalize_tool_call(call, true)? {
                terminal_tool_events.push(event);
            }
        }

        if self.config.retain_reasoning_content()
            && let Some(captured) = end.captured_reasoning_content
        {
            if self.reasoning.is_empty() {
                self.push_reasoning(captured)?;
            } else if self.reasoning != captured {
                return Err(GenaiMappingError::ConflictingReasoningContent);
            }
        }

        let (finish_reason, raw_stop_reason) = map_finish_reason(end.captured_stop_reason);
        let mut response_extensions = Extensions::new();
        insert_string(
            &mut response_extensions,
            RESOLVED_MODEL,
            self.resolved_model.model_name.as_str(),
        )?;
        insert_string(
            &mut response_extensions,
            ADAPTER_KIND,
            self.resolved_model.adapter_kind.as_lower_str(),
        )?;
        insert_string(&mut response_extensions, RAW_STOP_REASON, raw_stop_reason)?;
        if self.config.retain_reasoning_content() && !self.reasoning.is_empty() {
            insert_string_list(
                &mut response_extensions,
                REASONING_CONTENT,
                vec![std::mem::take(&mut self.reasoning)],
            )?;
        }

        let first_tool_index = self.tool_calls.values().map(|call| call.index).min();
        if self.thought_signatures.is_empty() {
            // Nothing to attach.
        } else if let Some(index) = first_tool_index {
            let mut extensions = Extensions::new();
            insert_string_list(
                &mut extensions,
                THOUGHT_SIGNATURES,
                std::mem::take(&mut self.thought_signatures),
            )?;
            terminal_tool_events.push(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(index).with_extensions(extensions),
            ));
        } else {
            insert_string_list(
                &mut response_extensions,
                THOUGHT_SIGNATURES,
                std::mem::take(&mut self.thought_signatures),
            )?;
        }

        let response_id = end
            .captured_response_id
            .map(|id| {
                ResponseId::new(id).map_err(|source| GenaiMappingError::InvalidIdentifier {
                    field: "stream.response_id",
                    source,
                })
            })
            .transpose()?;
        let model = ModelId::new(self.resolved_model.model_name.as_str()).map_err(|source| {
            GenaiMappingError::InvalidIdentifier {
                field: "stream.resolved_model",
                source,
            }
        })?;
        self.pending.push_back(Ok(ChatStreamEvent::ResponseStarted {
            response_id,
            model: Some(model),
            extensions: response_extensions,
        }));
        self.pending
            .extend(terminal_tool_events.into_iter().map(Ok));
        if let Some(usage) = map_usage(
            end.captured_usage.unwrap_or_default(),
            self.config.retain_usage_details(),
        )? {
            self.pending.push_back(Ok(ChatStreamEvent::Usage(usage)));
        }
        self.pending
            .push_back(Ok(ChatStreamEvent::Finished(finish_reason)));
        self.saw_end = true;
        Ok(())
    }
}

fn cumulative_arguments_fragment(
    previous_raw: &str,
    current_raw: &str,
) -> Result<String, GenaiMappingError> {
    if current_raw == previous_raw {
        Ok(String::new())
    } else if let Some(suffix) = current_raw.strip_prefix(previous_raw) {
        Ok(suffix.to_owned())
    } else {
        Err(GenaiMappingError::ConflictingStreamToolArguments)
    }
}

fn terminal_arguments_fragment(
    accumulated_raw: &str,
    terminal_value: &serde_json::Value,
) -> Result<String, GenaiMappingError> {
    if accumulated_raw.is_empty() {
        return serde_json::to_string(terminal_value)
            .map_err(GenaiMappingError::ToolArgumentsSerialization);
    }
    let accumulated_value = serde_json::from_str::<serde_json::Value>(accumulated_raw)
        .map_err(GenaiMappingError::InvalidAccumulatedToolArguments)?;
    if accumulated_value == *terminal_value {
        Ok(String::new())
    } else {
        Err(GenaiMappingError::ConflictingTerminalToolCall)
    }
}

impl<S> Stream for GenaiEventStream<S>
where
    S: Stream<Item = genai::Result<genai::chat::ChatStreamEvent>> + Unpin,
{
    type Item = Result<ChatStreamEvent, ModelError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(event) = this.pending.pop_front() {
            if this.saw_end && this.pending.is_empty() {
                this.terminal = true;
            }
            return Poll::Ready(Some(event));
        }
        if this.terminal {
            return Poll::Ready(None);
        }

        loop {
            match Pin::new(&mut this.inner).poll_next(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    if this.saw_end {
                        this.terminal = true;
                        return Poll::Ready(None);
                    }
                    return this.fail(GenaiMappingError::MissingStreamEnd);
                }
                Poll::Ready(Some(Err(error))) => {
                    this.terminal = true;
                    return Poll::Ready(Some(Err(map_genai_error(
                        error,
                        &this.provider,
                        &this.model,
                    ))));
                }
                Poll::Ready(Some(Ok(event))) =>
                {
                    #[allow(unreachable_patterns)]
                    match event {
                        genai::chat::ChatStreamEvent::Start => {
                            if this.saw_start {
                                return this.fail(GenaiMappingError::DuplicateStreamStart);
                            }
                            this.saw_start = true;
                        }
                        genai::chat::ChatStreamEvent::Chunk(chunk) => {
                            return Poll::Ready(Some(Ok(ChatStreamEvent::TextDelta(
                                chunk.content,
                            ))));
                        }
                        genai::chat::ChatStreamEvent::ReasoningChunk(chunk) => {
                            if let Err(error) = this.push_reasoning(chunk.content) {
                                return this.fail(error);
                            }
                        }
                        genai::chat::ChatStreamEvent::ThoughtSignatureChunk(chunk) => {
                            let _ = chunk;
                            return this.fail(
                                GenaiMappingError::UnexpectedThoughtSignatureInTextOnlyStream,
                            );
                        }
                        genai::chat::ChatStreamEvent::ToolCallChunk(chunk) => {
                            let _ = chunk;
                            return this
                                .fail(GenaiMappingError::UnexpectedToolCallInTextOnlyStream);
                        }
                        genai::chat::ChatStreamEvent::End(end) => {
                            if this.saw_end {
                                return this.fail(GenaiMappingError::ConflictingTerminalToolCall);
                            }
                            if let Err(error) = this.handle_end(end) {
                                return this.fail(error);
                            }
                            let event = this
                                .pending
                                .pop_front()
                                .expect("End always queues ResponseStarted and Finished");
                            if this.pending.is_empty() {
                                this.terminal = true;
                            }
                            return Poll::Ready(Some(event));
                        }
                        _ => {
                            return this.fail(GenaiMappingError::UnsupportedResponseContent {
                                kind: "future genai stream event",
                            });
                        }
                    }
                }
            }
        }
    }
}

fn checked_signature_total(
    current: usize,
    additional: usize,
    maximum: usize,
) -> Result<usize, GenaiMappingError> {
    let next = current
        .checked_add(additional)
        .ok_or(GenaiMappingError::ThoughtSignatureLengthOverflow)?;
    if next > maximum {
        return Err(GenaiMappingError::ThoughtSignatureLimitExceeded { maximum });
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use futures_core::Stream;
    use futures_util::StreamExt;
    use genai::adapter::AdapterKind;
    use genai::chat::{ChatStreamEvent as GenaiStreamEvent, StreamChunk};
    use group_agent_model::{ModelCapabilities, ModelErrorKind, ModelId, ProviderId};
    use serde_json::json;

    use super::{
        GenaiEventStream, checked_signature_total, cumulative_arguments_fragment,
        terminal_arguments_fragment,
    };
    use crate::{GenaiAdapterConfig, GenaiMappingError, GenaiModelConfig};

    struct CountingStream {
        items: VecDeque<genai::Result<GenaiStreamEvent>>,
        polls: Arc<AtomicUsize>,
    }

    impl Stream for CountingStream {
        type Item = genai::Result<GenaiStreamEvent>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.items.pop_front())
        }
    }

    fn stream_config() -> GenaiAdapterConfig {
        GenaiAdapterConfig::new(
            GenaiModelConfig::new(
                "model",
                ProviderId::new("provider").expect("provider"),
                ModelId::new("model").expect("model"),
                ModelCapabilities::new().with_streaming(true),
            )
            .expect("model config"),
        )
    }

    #[test]
    fn cumulative_arguments_require_an_append_only_prefix() {
        assert_eq!(
            cumulative_arguments_fragment("", r#"{"a":"#).expect("initial"),
            r#"{"a":"#
        );
        assert_eq!(
            cumulative_arguments_fragment(r#"{"a":"#, r#"{"a":"#).expect("idempotent"),
            ""
        );
        assert_eq!(
            cumulative_arguments_fragment(r#"{"a":"#, r#"{"a":1}"#).expect("suffix"),
            "1}"
        );
        assert!(matches!(
            cumulative_arguments_fragment(r#"{"a":1"#, r#"{"a":2"#),
            Err(crate::GenaiMappingError::ConflictingStreamToolArguments)
        ));
        assert_eq!(
            cumulative_arguments_fragment(r#"{"文字":"#, r#"{"文字":"值"}"#).expect("UTF-8 suffix"),
            "\"值\"}"
        );
        assert_eq!(
            cumulative_arguments_fragment("", "").expect("explicit empty cumulative value"),
            ""
        );
    }

    #[test]
    fn terminal_arguments_compare_all_json_value_kinds_structurally() {
        let values = [
            json!("value"),
            json!(7),
            json!(true),
            serde_json::Value::Null,
            json!([1, "two"]),
            json!({"a": 1}),
        ];
        for value in values {
            let raw = serde_json::to_string(&value).expect("JSON");
            assert_eq!(
                terminal_arguments_fragment(&raw, &value).expect("equal terminal value"),
                ""
            );
            assert_eq!(
                terminal_arguments_fragment("", &value).expect("initial terminal capture"),
                raw
            );
            assert_eq!(
                terminal_arguments_fragment(&raw, &value).expect("duplicate terminal capture"),
                ""
            );
        }
        assert_eq!(
            terminal_arguments_fragment("", &json!("")).expect("empty JSON string"),
            r#""""#
        );
        assert_eq!(
            terminal_arguments_fragment(r#"{"a":1,"b":2}"#, &json!({"b":2,"a":1}))
                .expect("object property order is not semantic"),
            ""
        );
        assert!(
            terminal_arguments_fragment("1", &json!(1.0)).is_err(),
            "serde_json preserves the integer versus floating-number distinction"
        );
    }

    #[test]
    fn terminal_mismatch_is_protocol_and_invalid_json_is_decode_with_source() {
        let mismatch = terminal_arguments_fragment(r#"{"a":1}"#, &json!({"a":2}))
            .expect_err("different values conflict")
            .into_model_error(
                &ProviderId::new("provider").expect("provider"),
                &ModelId::new("model").expect("model"),
            );
        assert_eq!(mismatch.kind(), &ModelErrorKind::Protocol);

        let invalid = terminal_arguments_fragment("{", &json!({}))
            .expect_err("invalid accumulated JSON")
            .into_model_error(
                &ProviderId::new("provider").expect("provider"),
                &ModelId::new("model").expect("model"),
            );
        assert_eq!(invalid.kind(), &ModelErrorKind::Decode);
        let mapping = std::error::Error::source(&invalid).expect("mapping source");
        assert!(
            std::error::Error::source(mapping).is_some(),
            "serde_json source must remain in the chain"
        );
    }

    #[test]
    fn streaming_signature_length_uses_checked_accounting() {
        assert_eq!(checked_signature_total(2, 3, 5).expect("exact limit"), 5);
        assert!(matches!(
            checked_signature_total(2, 4, 5),
            Err(GenaiMappingError::ThoughtSignatureLimitExceeded { maximum: 5 })
        ));
        assert!(matches!(
            checked_signature_total(usize::MAX, 1, usize::MAX),
            Err(GenaiMappingError::ThoughtSignatureLengthOverflow)
        ));
    }

    #[test]
    fn thought_signature_chunks_fail_closed_without_polling_again_or_retaining_content() {
        let sentinel = "thought-signature-stream-sentinel";
        for signature in ["", sentinel] {
            let polls = Arc::new(AtomicUsize::new(0));
            let inner = CountingStream {
                items: VecDeque::from([
                    Ok(GenaiStreamEvent::ThoughtSignatureChunk(StreamChunk {
                        content: signature.to_owned(),
                    })),
                    Ok(GenaiStreamEvent::Chunk(StreamChunk {
                        content: "must-not-be-polled".to_owned(),
                    })),
                ]),
                polls: Arc::clone(&polls),
            };
            let mut stream = GenaiEventStream::from_inner(
                inner,
                genai::ModelIden::new(AdapterKind::OpenAI, "model"),
                stream_config(),
                ProviderId::new("provider").expect("provider"),
                ModelId::new("model").expect("model"),
            );

            let error = futures_executor::block_on(stream.next())
                .expect("one terminal item")
                .expect_err("thought-signature chunks are not valid text-only data");
            assert_eq!(error.kind(), &ModelErrorKind::Protocol);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert!(stream.thought_signatures.is_empty());
            assert_eq!(stream.thought_signature_bytes, 0);
            assert!(futures_executor::block_on(stream.next()).is_none());
            assert_eq!(
                polls.load(Ordering::SeqCst),
                1,
                "terminal wrapper must not poll the remaining provider event"
            );
            for rendered in [format!("{error:?}"), error.to_string()] {
                assert!(!rendered.contains(sentinel));
                if !signature.is_empty() {
                    assert!(!rendered.contains(signature));
                }
                assert!(!rendered.contains("must-not-be-polled"));
            }
        }
    }
}
