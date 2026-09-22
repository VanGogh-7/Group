use std::collections::{BTreeMap, BTreeSet, VecDeque};

use group_agent_model::{
    ChatStreamEvent, Extensions, FinishReason, ModelId, ResponseId, TokenUsage, ToolCallDelta,
    ToolCallId, ToolName,
};
use serde_json::Value;

use super::invalid;
use crate::extensions::{
    ADAPTER_KIND, COMPLETION_TOKEN_DETAILS, PROMPT_TOKEN_DETAILS, RAW_STOP_REASON, RESOLVED_MODEL,
    insert, insert_string,
};
use crate::{GenaiAdapterConfig, GenaiMappingError};

struct Call {
    id: ToolCallId,
    name: ToolName,
    arguments: String,
}

/// Decode only the supported single-choice Chat protocol. Never synthesize IDs.
/// Source: https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events
pub(super) struct Decoder {
    config: GenaiAdapterConfig,
    calls: BTreeMap<u32, Call>,
    ids: BTreeSet<String>,
    argument_bytes: usize,
    response_id: Option<ResponseId>,
    model: Option<ModelId>,
    finish: Option<(FinishReason, &'static str)>,
    pub(super) pending: VecDeque<ChatStreamEvent>,
    pub(super) done: bool,
    resolved_model: String,
}

impl Decoder {
    pub(super) fn new(config: GenaiAdapterConfig, resolved_model: String) -> Self {
        Self {
            config,
            resolved_model,
            calls: BTreeMap::new(),
            ids: BTreeSet::new(),
            argument_bytes: 0,
            response_id: None,
            model: None,
            finish: None,
            pending: VecDeque::new(),
            done: false,
        }
    }

    pub(super) fn push(&mut self, data: &str) -> Result<(), GenaiMappingError> {
        if data == "[DONE]" {
            return self.finish();
        }
        let value: Value = serde_json::from_str(data).map_err(GenaiMappingError::OpenAiChatJson)?;
        let object = value.as_object().ok_or_else(|| invalid("chunk"))?;
        if object.get("error").is_some_and(|e| !e.is_null()) {
            return Err(invalid("provider error"));
        }
        if let Some(kind) = object.get("object")
            && kind != "chat.completion.chunk"
        {
            return Err(invalid("object"));
        }
        self.identity(&value)?;
        let choices = value
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("choices"))?;
        if choices.len() > 1 {
            return Err(invalid("multiple choices"));
        }
        if let Some(choice) = choices.first() {
            if self.finish.is_some() {
                return Err(invalid("choice after finish_reason"));
            }
            if choice.get("index").and_then(Value::as_u64) != Some(0) {
                return Err(invalid("choice index"));
            }
            if self.response_id.is_none() || self.model.is_none() {
                return Err(invalid("response identity"));
            }
            let delta = choice
                .get("delta")
                .and_then(Value::as_object)
                .ok_or_else(|| invalid("delta"))?;
            if optional_string(delta.get("role"), "role")?.is_some_and(|role| role != "assistant") {
                return Err(invalid("role"));
            }
            // Unsupported content is rejected rather than silently omitted.
            for field in [
                "function_call",
                "refusal",
                "audio",
                "reasoning",
                "reasoning_content",
                "thought_signature",
            ] {
                if delta.get(field).is_some_and(|v| !v.is_null()) {
                    return Err(invalid(field));
                }
            }
            if let Some(content) = optional_string(delta.get("content"), "content")?
                && !content.is_empty()
            {
                self.pending
                    .push_back(ChatStreamEvent::TextDelta(content.to_owned()));
            }
            if let Some(tools) = delta.get("tool_calls").filter(|v| !v.is_null()) {
                let tools = tools.as_array().ok_or_else(|| invalid("tool_calls"))?;
                // Bound per-frame event expansion even when all entries reuse one index.
                if tools.len() > self.config.streaming_limits().max_tool_calls() as usize {
                    return Err(GenaiMappingError::StreamToolCallLimit {
                        maximum: self.config.streaming_limits().max_tool_calls(),
                    });
                }
                for tool in tools {
                    self.tool(tool)?;
                }
            }
            if let Some(reason) = optional_string(choice.get("finish_reason"), "finish_reason")? {
                let finish = match reason {
                    "stop" if self.calls.is_empty() => (FinishReason::Stop, "stop"),
                    "length" if self.calls.is_empty() => (FinishReason::Length, "length"),
                    "content_filter" if self.calls.is_empty() => {
                        (FinishReason::ContentFilter, "content_filter")
                    }
                    "tool_calls" if !self.calls.is_empty() => {
                        (FinishReason::ToolCalls, "tool_calls")
                    }
                    _ => return Err(invalid("finish_reason/tool_calls")),
                };
                self.finish = Some(finish);
            }
        } else if self.finish.is_none() {
            return Err(invalid("usage before finish_reason"));
        }
        if let Some(usage) = value.get("usage").filter(|v| !v.is_null()) {
            self.pending
                .push_back(ChatStreamEvent::Usage(self.usage(usage)?));
        } else if choices.is_empty() {
            return Err(invalid("empty usage tail"));
        }
        Ok(())
    }

    fn identity(&mut self, value: &Value) -> Result<(), GenaiMappingError> {
        if let Some(id) = optional_string(value.get("id"), "id")? {
            let id =
                ResponseId::new(id).map_err(|source| GenaiMappingError::InvalidIdentifier {
                    field: "id",
                    source,
                })?;
            if self.response_id.as_ref().is_some_and(|prior| prior != &id) {
                return Err(invalid("changed response id"));
            }
            self.response_id = Some(id);
        }
        if let Some(model) = optional_string(value.get("model"), "model")? {
            let model =
                ModelId::new(model).map_err(|source| GenaiMappingError::InvalidIdentifier {
                    field: "model",
                    source,
                })?;
            if self.model.as_ref().is_some_and(|prior| prior != &model) {
                return Err(invalid("changed model"));
            }
            self.model = Some(model);
        }
        Ok(())
    }

    fn tool(&mut self, value: &Value) -> Result<(), GenaiMappingError> {
        let index = value
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| invalid("tool index"))?;
        let maximum = self.config.streaming_limits().max_tool_calls();
        if index >= maximum {
            return Err(GenaiMappingError::StreamToolCallLimit { maximum });
        }
        if let Some(kind) = optional_string(value.get("type"), "tool type")?
            && kind != "function"
        {
            return Err(invalid("tool type"));
        }
        let function = value
            .get("function")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("tool function"))?;
        let id = optional_string(value.get("id"), "tool id")?;
        let name = optional_string(function.get("name"), "tool name")?;
        let arguments = optional_string(function.get("arguments"), "tool arguments")?.unwrap_or("");
        let mut delta = ToolCallDelta::new(index);
        if let Some(call) = self.calls.get(&index) {
            if id.is_some_and(|v| v != call.id.as_str())
                || name.is_some_and(|v| v != call.name.as_str())
            {
                return Err(invalid("changed tool identity"));
            }
        } else {
            let id = ToolCallId::new(id.ok_or_else(|| invalid("missing tool id"))?).map_err(
                |source| GenaiMappingError::InvalidIdentifier {
                    field: "tool id",
                    source,
                },
            )?;
            let name = ToolName::new(name.ok_or_else(|| invalid("missing tool name"))?).map_err(
                |source| GenaiMappingError::InvalidIdentifier {
                    field: "tool name",
                    source,
                },
            )?;
            if !self.ids.insert(id.as_str().to_owned()) {
                return Err(invalid("duplicate tool id"));
            }
            delta = delta.with_id(id.clone()).with_name(name.clone());
            self.calls.insert(
                index,
                Call {
                    id,
                    name,
                    arguments: String::new(),
                },
            );
        }
        let maximum = self.config.streaming_limits().max_tool_argument_bytes();
        self.argument_bytes = self
            .argument_bytes
            .checked_add(arguments.len())
            .filter(|v| *v <= maximum)
            .ok_or(GenaiMappingError::OpenAiChatByteLimit {
                field: "tool arguments",
                maximum,
            })?;
        if let Some(call) = self.calls.get_mut(&index) {
            call.arguments.push_str(arguments);
        }
        self.pending.push_back(ChatStreamEvent::ToolCallDelta(
            delta.with_arguments_fragment(arguments),
        ));
        Ok(())
    }

    fn usage(&self, value: &Value) -> Result<TokenUsage, GenaiMappingError> {
        let object = value.as_object().ok_or_else(|| invalid("usage"))?;
        let counter = |name| -> Result<Option<u64>, GenaiMappingError> {
            object
                .get(name)
                .filter(|v| !v.is_null())
                .map(|v| v.as_u64().ok_or_else(|| invalid(name)))
                .transpose()
        };
        let mut extensions = Extensions::new();
        for (field, key) in [
            ("prompt_tokens_details", PROMPT_TOKEN_DETAILS),
            ("completion_tokens_details", COMPLETION_TOKEN_DETAILS),
        ] {
            if let Some(details) = object.get(field).filter(|v| !v.is_null()) {
                if !details.is_object() {
                    return Err(invalid(field));
                }
                if self.config.retain_usage_details() {
                    insert(&mut extensions, key, details.clone())?;
                }
            }
        }
        Ok(TokenUsage::from_parts(
            counter("prompt_tokens")?,
            counter("completion_tokens")?,
            counter("total_tokens")?,
        )
        .map_err(GenaiMappingError::InvalidTokenUsage)?
        .with_extensions(extensions))
    }

    fn finish(&mut self) -> Result<(), GenaiMappingError> {
        let (reason, raw) = self
            .finish
            .take()
            .ok_or_else(|| invalid("missing finish_reason"))?;
        for call in self.calls.values() {
            serde_json::from_str::<Value>(&call.arguments)
                .map_err(GenaiMappingError::InvalidAccumulatedToolArguments)?;
        }
        let mut extensions = Extensions::new();
        insert_string(&mut extensions, ADAPTER_KIND, "openai")?;
        insert_string(&mut extensions, RESOLVED_MODEL, &self.resolved_model)?;
        insert_string(&mut extensions, RAW_STOP_REASON, raw)?;
        self.pending.push_back(ChatStreamEvent::ResponseStarted {
            response_id: self.response_id.take(),
            model: self.model.take(),
            extensions,
        });
        self.pending.push_back(ChatStreamEvent::Finished(reason));
        self.calls.clear();
        self.ids.clear();
        self.done = true;
        Ok(())
    }
}

fn optional_string<'a>(
    value: Option<&'a Value>,
    field: &'static str,
) -> Result<Option<&'a str>, GenaiMappingError> {
    value
        .filter(|v| !v.is_null())
        .map(|v| v.as_str().ok_or_else(|| invalid(field)))
        .transpose()
}
