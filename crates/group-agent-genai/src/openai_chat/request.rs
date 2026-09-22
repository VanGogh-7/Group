use genai::chat::{ChatMessage, ChatOptions, ChatRole, ContentPart, ToolChoice};
use genai::resolver::AuthData;
use genai::{ClientConfig, ServiceTarget};
use serde_json::{Value, json};

use crate::{GenaiAdapterConfigError, GenaiMappingError, MappedChatRequest};

pub(super) fn validate_config(
    config: &ClientConfig,
    target: &ServiceTarget,
) -> Result<(), GenaiAdapterConfigError> {
    let (_, name) = target.model.model_name.namespace_and_name();
    if genai::chat::ReasoningEffort::from_model_name(name)
        .0
        .is_some()
    {
        return Err(unsupported("model reasoning-effort suffix"));
    }
    if !matches!(
        target.auth,
        AuthData::Key(_) | AuthData::None | AuthData::RequestOverride { .. }
    ) {
        return Err(unsupported(
            "auth (supply explicit credentials or request override)",
        ));
    }
    if let Some(options) = config.chat_options() {
        for (field, present) in [
            ("response_format", options.response_format.is_some()),
            (
                "normalize_reasoning_content",
                options.normalize_reasoning_content == Some(true),
            ),
            ("reasoning_effort", options.reasoning_effort.is_some()),
            ("verbosity", options.verbosity.is_some()),
            ("seed", options.seed.is_some()),
            ("service_tier", options.service_tier.is_some()),
            ("cache_control", options.cache_control.is_some()),
            ("prompt_cache_key", options.prompt_cache_key.is_some()),
            ("extra_body", options.extra_body.is_some()),
            (
                "extra_headers (use WebConfig default_headers)",
                options.extra_headers.is_some(),
            ),
        ] {
            if present {
                return Err(unsupported(field));
            }
        }
    }
    Ok(())
}

fn unsupported(field: &'static str) -> GenaiAdapterConfigError {
    GenaiAdapterConfigError::UnsupportedOpenAiChatSetting { field }
}

pub(super) fn endpoint(target: &ServiceTarget) -> Result<reqwest::Url, GenaiAdapterConfigError> {
    let resolved = match &target.auth {
        AuthData::RequestOverride { url, .. } => reqwest::Url::parse(url),
        _ => reqwest::Url::parse(target.endpoint.base_url()).and_then(|base| {
            let mut endpoint = base.join("chat/completions")?;
            endpoint.set_query(base.query());
            Ok(endpoint)
        }),
    }
    .map_err(|source| {
        GenaiAdapterConfigError::OpenAiChatEndpoint(Box::new(
            group_agent_model::ModelError::with_source(
                group_agent_model::ModelErrorKind::InvalidRequest,
                "invalid OpenAI Chat endpoint",
                source,
            ),
        ))
    })?;
    if !matches!(resolved.scheme(), "http" | "https") {
        return Err(unsupported("endpoint scheme"));
    }
    Ok(resolved)
}

pub(super) fn build(
    client: &reqwest::Client,
    endpoint: &reqwest::Url,
    target: &ServiceTarget,
    defaults: &ChatOptions,
    mapped: MappedChatRequest,
) -> Result<reqwest::RequestBuilder, GenaiMappingError> {
    let request = mapped.request;
    let options = mapped.options;
    if request.previous_response_id.is_some() {
        return Err(GenaiMappingError::UnsupportedRequestContent {
            kind: "Chat previous_response_id",
        });
    }
    let messages = request
        .messages
        .into_iter()
        .map(message)
        .collect::<Result<Vec<_>, _>>()?;
    let (_, model_name) = target.model.model_name.namespace_and_name();
    let mut body = json!({
        "model":model_name, "messages":messages,
        "stream":true, "n":1, "stream_options":{"include_usage":true}
    });
    if let Some(tools) = request.tools {
        body["tools"] = Value::Array(
            tools
                .into_iter()
                .map(|tool| {
                    json!({
                        "type":"function", "function":{
                            "name":tool.name.as_str(), "description":tool.description,
                            "parameters":tool.schema
                        }
                    })
                })
                .collect(),
        );
    }
    // A per-request ToolChoice is always supplied by the canonical Group mapper.
    if let Some(choice) = options.tool_choice
        && (body.get("tools").is_some() || !matches!(choice, ToolChoice::Auto))
    {
        body["tool_choice"] = match choice {
            ToolChoice::Auto => json!("auto"),
            ToolChoice::None => json!("none"),
            ToolChoice::Required => json!("required"),
            ToolChoice::Tool { name } => json!({"type":"function","function":{"name":name}}),
        };
    }
    if let Some(value) = options.temperature.or(defaults.temperature) {
        body["temperature"] = json!(value);
    }
    if let Some(value) = options.top_p.or(defaults.top_p) {
        body["top_p"] = json!(value);
    }
    if let Some(value) = options.max_tokens.or(defaults.max_tokens) {
        // Preserve the pinned Genai OpenAI request convention on both paths.
        let key = if ["gpt-5", "o1", "o3", "o4"]
            .iter()
            .any(|prefix| model_name.starts_with(prefix))
        {
            "max_completion_tokens"
        } else {
            "max_tokens"
        };
        body[key] = json!(value);
    }
    let stop = &options.stop_sequences;
    if !stop.is_empty() {
        body["stop"] = json!(stop);
    }
    if let Some(value) = request.store {
        body["store"] = json!(value);
    }

    let mut builder = match &target.auth {
        AuthData::RequestOverride { headers, .. } => {
            let mut builder = client.post(endpoint.clone());
            for (name, value) in headers.iter() {
                builder = builder.header(name, value);
            }
            builder
        }
        auth => {
            let mut builder = client.post(endpoint.clone());
            if let AuthData::Key(key) = auth {
                builder = builder.bearer_auth(key);
            }
            builder
        }
    };
    builder = builder
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .json(&body);
    Ok(builder)
}

fn message(message: ChatMessage) -> Result<Value, GenaiMappingError> {
    let role = match message.role {
        ChatRole::System => "system",
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::Tool => "tool",
    };
    let mut wire = json!({"role":role});
    let mut text = String::new();
    let mut calls = Vec::new();
    for part in message.content {
        match part {
            ContentPart::Text(value) => text.push_str(&value),
            ContentPart::ToolCall(call) => {
                calls.push(json!({"id":call.call_id,"type":"function","function":{
                    "name":call.fn_name,"arguments":serde_json::to_string(&call.fn_arguments)
                        .map_err(GenaiMappingError::ToolArgumentsSerialization)?
                }}));
            }
            ContentPart::ToolResponse(result) => {
                wire["tool_call_id"] = json!(result.call_id);
                text.push_str(&result.content);
            }
            // Continuation metadata belonging to other protocols must never be lost.
            _ => {
                return Err(GenaiMappingError::UnsupportedRequestContent {
                    kind: "strict Chat continuation or content",
                });
            }
        }
    }
    wire["content"] = if text.is_empty() && !calls.is_empty() {
        Value::Null
    } else {
        json!(text)
    };
    if !calls.is_empty() {
        wire["tool_calls"] = Value::Array(calls);
    }
    Ok(wire)
}
