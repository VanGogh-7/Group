use super::{OpenAiChat, decode::Decoder, invalid, request};
use crate::{GenaiAdapterConfig, GenaiMappingError, MappedChatRequest};
use genai::ServiceTarget;
use group_agent_model::{
    ChatResponse, ChatStreamCollector, ModelError, ModelErrorKind, Retryability, StructuredOutput,
};
use serde_json::{Value, json};

const MAX_BODY: usize = 4 * 1024 * 1024;
impl OpenAiChat {
    pub(crate) async fn complete(
        &self,
        target: &ServiceTarget,
        mapped: MappedChatRequest,
        output: &StructuredOutput,
        config: GenaiAdapterConfig,
    ) -> Result<ChatResponse, ModelError> {
        let mapping = |e: GenaiMappingError| {
            e.into_model_error(
                config.model().metadata().provider(),
                config.model().metadata().model(),
            )
        };
        let transport = |e: reqwest::Error| {
            let kind = if e.is_timeout() {
                ModelErrorKind::Timeout
            } else if e.is_builder() {
                ModelErrorKind::InvalidRequest
            } else {
                ModelErrorKind::ProviderUnavailable
            };
            ModelError::with_source(kind, "OpenAI Chat transport failed", e)
                .with_model_context(
                    config.model().metadata().provider().clone(),
                    config.model().metadata().model().clone(),
                )
                .with_retryability(Retryability::Never)
        };
        let mut response = request::build(
            &self.client,
            &self.endpoint,
            target,
            &self.defaults,
            mapped,
            false,
            Some(output),
        )
        .map_err(mapping)?
        .send()
        .await
        .map_err(transport)?;
        if !response.status().is_success() {
            let source = genai::Error::WebModelCall {
                model_iden: target.model.clone(),
                webc_error: genai::webc::Error::ResponseFailedStatus {
                    status: response.status(),
                    headers: Box::new(response.headers().clone()),
                    body: String::new(),
                },
            };
            return Err(crate::error::map_genai_error(
                source,
                config.model().metadata().provider(),
                config.model().metadata().model(),
            ));
        }
        let media = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        if !media.eq_ignore_ascii_case("application/json") {
            return Err(mapping(invalid("Content-Type")));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport)? {
            if body
                .len()
                .checked_add(chunk.len())
                .is_none_or(|n| n > MAX_BODY)
            {
                return Err(mapping(GenaiMappingError::OpenAiChatByteLimit {
                    field: "completion body",
                    maximum: MAX_BODY,
                }));
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&body)
            .map_err(|e| mapping(GenaiMappingError::OpenAiChatJson(e)))?;
        let events = decode(value, target, config.clone()).map_err(mapping)?;
        let mut collector = ChatStreamCollector::new();
        for event in events {
            collector.push(event)?;
        }
        collector.finish()
    }
}

fn decode(
    mut value: Value,
    target: &ServiceTarget,
    config: GenaiAdapterConfig,
) -> Result<std::collections::VecDeque<group_agent_model::ChatStreamEvent>, GenaiMappingError> {
    if value.get("object") != Some(&json!("chat.completion")) {
        return Err(invalid("object"));
    }
    let choices = value
        .get_mut("choices")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("choices"))?;
    if choices.len() != 1 {
        return Err(invalid("multiple or missing choices"));
    }
    let choice = choices[0]
        .as_object_mut()
        .ok_or_else(|| invalid("choice"))?;
    let mut message = choice.remove("message").ok_or_else(|| invalid("message"))?;
    if message.get("role") != Some(&json!("assistant")) {
        return Err(invalid("role"));
    }
    if let Some(tools) = message.get_mut("tool_calls").filter(|v| !v.is_null()) {
        let tools = tools.as_array_mut().ok_or_else(|| invalid("tool_calls"))?;
        for (index, tool) in tools.iter_mut().enumerate() {
            let tool = tool.as_object_mut().ok_or_else(|| invalid("tool"))?;
            tool.insert("index".into(), json!(index));
        }
    }
    choice.insert("delta".into(), message);
    value["object"] = json!("chat.completion.chunk");
    let mut decoder = Decoder::new(config, target.model.model_name.as_str().to_owned(), true);
    decoder.push_value(value)?;
    decoder.push("[DONE]")?;
    Ok(decoder.pending)
}
