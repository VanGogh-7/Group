use std::collections::BTreeMap;

use genai::chat::{
    ChatMessage as GenaiMessage, ChatOptions as GenaiOptions, ChatRequest as GenaiRequest,
    ContentPart as GenaiContentPart, MessageContent as GenaiMessageContent, Tool as GenaiTool,
    ToolCall as GenaiToolCall, ToolChoice as GenaiToolChoice, ToolResponse as GenaiToolResponse,
};
use group_agent_model::{
    AssistantMessage, ChatRequest, ContentPart, Message, ToolCall, ToolChoice, ValidatedChatRequest,
};

use crate::GenaiMappingError;
use crate::config::GenaiAdapterConfig;
use crate::extensions::{
    REASONING_CONTENT, THOUGHT_SIGNATURES, string_list, validate_context_extensions,
    validate_request_extensions,
};

/// genai request data produced without performing network I/O.
pub struct MappedChatRequest {
    /// Ordered genai messages, tools, and explicit stateful fields.
    pub request: GenaiRequest,
    /// Provider-neutral genai chat controls.
    pub options: GenaiOptions,
}

/// Validates and maps a standalone Group request without network I/O.
///
/// Adapter dispatch still receives only [`ValidatedChatRequest`]; this helper
/// exists for deterministic inspection, testing, and benchmarking.
pub fn map_chat_request(
    request: ChatRequest,
    config: &GenaiAdapterConfig,
) -> Result<MappedChatRequest, GenaiMappingError> {
    request
        .validate()
        .map_err(GenaiMappingError::InvalidGroupRequest)?;
    map_chat_request_inner(request, config)
}

pub(crate) fn map_request(
    request: ValidatedChatRequest,
    config: &GenaiAdapterConfig,
) -> Result<MappedChatRequest, GenaiMappingError> {
    map_chat_request_inner(request.into_inner(), config)
}

fn map_chat_request_inner(
    request: ChatRequest,
    config: &GenaiAdapterConfig,
) -> Result<MappedChatRequest, GenaiMappingError> {
    let (previous_response_id, store) = validate_request_extensions(
        request.extensions(),
        config.allow_response_id_continuation(),
    )?;

    let mut known_tool_names = BTreeMap::<String, String>::new();
    let mut messages = Vec::with_capacity(request.messages().len());
    for message in request.messages() {
        messages.push(map_message(message, &mut known_tool_names)?);
    }

    let tools = if request.tools().is_empty() {
        None
    } else {
        Some(
            request
                .tools()
                .iter()
                .map(|tool| {
                    GenaiTool::new(tool.name().as_str())
                        .with_description(tool.description())
                        .with_schema(tool.input_schema().clone())
                })
                .collect(),
        )
    };

    let generation = request.generation();
    if generation.parallel_tool_calls().is_some() {
        return Err(GenaiMappingError::ParallelToolCallsUnsupported);
    }

    let options = GenaiOptions {
        temperature: generation.temperature(),
        max_tokens: generation.max_output_tokens(),
        top_p: generation.top_p(),
        stop_sequences: generation.stop_sequences().to_vec(),
        tool_choice: Some(map_tool_choice(request.tool_choice())?),
        capture_usage: Some(true),
        capture_content: Some(false),
        capture_reasoning_content: Some(config.retain_reasoning_content()),
        capture_tool_calls: Some(true),
        capture_raw_body: Some(false),
        ..GenaiOptions::default()
    };

    Ok(MappedChatRequest {
        request: GenaiRequest {
            system: None,
            messages,
            tools,
            previous_response_id,
            store,
        },
        options,
    })
}

fn map_message(
    message: &Message,
    known_tool_names: &mut BTreeMap<String, String>,
) -> Result<GenaiMessage, GenaiMappingError> {
    match message {
        Message::System(message) => Ok(GenaiMessage::system(map_text_content(message.content())?)),
        Message::User(message) => Ok(GenaiMessage::user(map_text_content(message.content())?)),
        Message::Assistant(message) => map_assistant(message, known_tool_names),
        Message::Tool(message) => {
            let call_id = message.tool_call_id().as_str().to_owned();
            let content = join_text_content(message.result().content())?;
            let mut response = GenaiToolResponse::new(call_id.clone(), content);
            if let Some(name) = known_tool_names.get(&call_id) {
                response = response.with_fn_name(name.clone());
            }
            // genai 0.6.5 has no wire-level `is_error` field. The original
            // result content is sent unchanged, without an invented prefix.
            Ok(GenaiMessage::tool(response))
        }
        _ => Err(GenaiMappingError::UnsupportedRequestContent {
            kind: "future group message",
        }),
    }
}

fn map_assistant(
    message: &AssistantMessage,
    known_tool_names: &mut BTreeMap<String, String>,
) -> Result<GenaiMessage, GenaiMappingError> {
    validate_context_extensions(message.extensions(), &[REASONING_CONTENT])?;
    let mut parts = map_text_parts(message.content())?;

    if let Some(reasoning) = string_list(message.extensions(), REASONING_CONTENT)? {
        parts.extend(
            reasoning
                .into_iter()
                .map(GenaiContentPart::ReasoningContent),
        );
    }

    for tool_call in message.tool_calls() {
        parts.extend(map_tool_call(tool_call, known_tool_names)?);
    }

    Ok(GenaiMessage::assistant(GenaiMessageContent::from_parts(
        parts,
    )))
}

fn map_tool_call(
    call: &ToolCall,
    known_tool_names: &mut BTreeMap<String, String>,
) -> Result<Vec<GenaiContentPart>, GenaiMappingError> {
    validate_context_extensions(call.extensions(), &[THOUGHT_SIGNATURES])?;
    let mut parts = Vec::new();
    if let Some(signatures) = string_list(call.extensions(), THOUGHT_SIGNATURES)? {
        parts.extend(
            signatures
                .into_iter()
                .map(GenaiContentPart::ThoughtSignature),
        );
    }

    let call_id = call.id().as_str().to_owned();
    let fn_name = call.name().as_str().to_owned();
    known_tool_names.insert(call_id.clone(), fn_name.clone());
    parts.push(GenaiContentPart::ToolCall(GenaiToolCall {
        call_id,
        fn_name,
        fn_arguments: call.arguments().clone(),
        // Signatures are represented exactly once as ordered ContentPart
        // values immediately before this call.
        thought_signatures: None,
    }));
    Ok(parts)
}

fn map_text_content(content: &[ContentPart]) -> Result<GenaiMessageContent, GenaiMappingError> {
    Ok(GenaiMessageContent::from_parts(map_text_parts(content)?))
}

fn map_text_parts(content: &[ContentPart]) -> Result<Vec<GenaiContentPart>, GenaiMappingError> {
    if content.is_empty() {
        return Ok(Vec::new());
    }
    let capacity = content
        .iter()
        .map(|part| match part {
            ContentPart::Text(text) => Ok(text.len()),
            _ => Err(GenaiMappingError::UnsupportedRequestContent {
                kind: "future group content",
            }),
        })
        .sum::<Result<usize, _>>()?;
    let mut text = String::with_capacity(capacity);
    for part in content {
        match part {
            ContentPart::Text(part) => text.push_str(part),
            _ => {
                return Err(GenaiMappingError::UnsupportedRequestContent {
                    kind: "future group content",
                });
            }
        }
    }
    // genai's provider adapters may join separate text parts with provider
    // separators. One ordered concatenation preserves Group's no-separator
    // text semantics, including empty parts, at the wire boundary.
    Ok(vec![GenaiContentPart::Text(text)])
}

fn join_text_content(content: &[ContentPart]) -> Result<String, GenaiMappingError> {
    let capacity = content
        .iter()
        .map(|part| match part {
            ContentPart::Text(text) => Ok(text.len()),
            _ => Err(GenaiMappingError::UnsupportedRequestContent {
                kind: "future group tool result content",
            }),
        })
        .sum::<Result<usize, _>>()?;
    let mut joined = String::with_capacity(capacity);
    for part in content {
        match part {
            ContentPart::Text(text) => joined.push_str(text),
            _ => {
                return Err(GenaiMappingError::UnsupportedRequestContent {
                    kind: "future group tool result content",
                });
            }
        }
    }
    Ok(joined)
}

fn map_tool_choice(choice: &ToolChoice) -> Result<GenaiToolChoice, GenaiMappingError> {
    Ok(match choice {
        ToolChoice::Auto => GenaiToolChoice::Auto,
        ToolChoice::None => GenaiToolChoice::None,
        ToolChoice::Required => GenaiToolChoice::Required,
        ToolChoice::Named(name) => GenaiToolChoice::tool(name.as_str()),
        _ => {
            return Err(GenaiMappingError::UnsupportedRequestContent {
                kind: "future group tool choice",
            });
        }
    })
}
