use std::error::Error as StdError;
use std::fmt;

use group_agent_model::{
    AssistantMessage, ChatRequest, ChatResponse, ChatStreamEvent, ContentPart, Extensions,
    FinishReason, GenerationConfig, Message, ModelCapabilities, ModelError, ModelErrorKind,
    ModelId, ModelMetadata, ProviderId, SystemMessage, TokenUsage, ToolCall, ToolCallDelta,
    ToolCallId, ToolDefinition, ToolName, ToolResult, UserMessage,
};
use serde_json::json;

const PROMPT_SECRET: &str = "PROMPT_SECRET_SENTINEL_16_2";
const ASSISTANT_SECRET: &str = "ASSISTANT_SECRET_SENTINEL_16_2";
const TOOL_ARGUMENTS_SECRET: &str = "TOOL_ARGUMENTS_SECRET_SENTINEL_16_2";
const TOOL_RESULT_SECRET: &str = "TOOL_RESULT_SECRET_SENTINEL_16_2";
const SCHEMA_SECRET: &str = "SCHEMA_SECRET_SENTINEL_16_2";
const EXTENSION_SECRET: &str = "EXTENSION_SECRET_SENTINEL_16_2";
const STREAM_TEXT_SECRET: &str = "STREAM_TEXT_SECRET_SENTINEL_16_2";
const STREAM_ARGUMENT_SECRET: &str = "STREAM_ARGUMENT_SECRET_SENTINEL_16_2";
const PROVIDER_MESSAGE_SECRET: &str = "PROVIDER_MESSAGE_SECRET_SENTINEL_16_2";
const PROVIDER_SOURCE_SECRET: &str = "PROVIDER_SOURCE_SECRET_SENTINEL_16_2";

fn extensions() -> Extensions {
    Extensions::new()
        .with("safe.extension.key", json!({"value": EXTENSION_SECRET}))
        .expect("valid extension")
}

fn call() -> ToolCall {
    ToolCall::new(
        ToolCallId::new("call-safe-id").expect("valid call id"),
        ToolName::new("safe-tool").expect("valid tool name"),
        json!({"secret": TOOL_ARGUMENTS_SECRET}),
    )
    .with_extensions(extensions())
}

fn assert_debug_redacted<T: fmt::Debug>(value: &T, secret: &str, structural_marker: &str) {
    let rendered = format!("{value:?}");
    assert!(
        !rendered.contains(secret),
        "{structural_marker} leaked its sentinel: {rendered}"
    );
    assert!(
        rendered.contains(structural_marker),
        "{structural_marker} lost safe structure: {rendered}"
    );
}

#[test]
fn message_and_tool_debug_redact_independent_payload_categories() {
    let prompt = ContentPart::text(PROMPT_SECRET);
    let system = SystemMessage::new(vec![prompt.clone()]);
    let user = UserMessage::new(vec![prompt.clone()]);
    let assistant = AssistantMessage::new(vec![ContentPart::text(ASSISTANT_SECRET)], vec![call()])
        .with_extensions(extensions());
    let result = ToolResult::error_text(TOOL_RESULT_SECRET);
    let tool_message = Message::tool(
        ToolCallId::new("call-safe-id").expect("valid call id"),
        result.clone(),
    );
    let definition = ToolDefinition::new(
        ToolName::new("safe-tool").expect("valid tool name"),
        "safe description",
        json!({"description": SCHEMA_SECRET}),
    );

    assert_debug_redacted(&prompt, PROMPT_SECRET, "Text");
    assert_debug_redacted(&system, PROMPT_SECRET, "SystemMessage");
    assert_debug_redacted(&user, PROMPT_SECRET, "UserMessage");
    assert_debug_redacted(&Message::User(user), PROMPT_SECRET, "User");
    assert_debug_redacted(&assistant, ASSISTANT_SECRET, "AssistantMessage");
    assert_debug_redacted(&assistant, TOOL_ARGUMENTS_SECRET, "call-safe-id");
    assert_debug_redacted(&assistant, EXTENSION_SECRET, "safe.extension.key");
    assert_debug_redacted(&call(), TOOL_ARGUMENTS_SECRET, "call-safe-id");
    assert_debug_redacted(&call(), EXTENSION_SECRET, "safe.extension.key");
    assert_debug_redacted(&result, TOOL_RESULT_SECRET, "ToolResult");
    assert_debug_redacted(&tool_message, TOOL_RESULT_SECRET, "Tool");
    assert_debug_redacted(&definition, SCHEMA_SECRET, "safe-tool");

    let display = Message::user(PROMPT_SECRET).to_string();
    assert!(!display.contains(PROMPT_SECRET), "message Display leaked");
    assert!(display.contains("User"), "message Display lost safe role");
}

#[test]
fn request_response_usage_and_metadata_debug_redact_independent_categories() {
    let generation = GenerationConfig::new().with_stop_sequences([PROMPT_SECRET]);
    let request = ChatRequest::new(vec![Message::user(PROMPT_SECRET)])
        .with_generation(generation.clone())
        .with_tools(vec![ToolDefinition::new(
            ToolName::new("safe-tool").expect("valid tool"),
            "safe description",
            json!({"secret": SCHEMA_SECRET}),
        )])
        .with_extensions(extensions());
    let usage = TokenUsage::from_parts(Some(1), None, None)
        .expect("valid usage")
        .with_extensions(extensions());
    let response = ChatResponse::new(
        AssistantMessage::new(vec![ContentPart::text(ASSISTANT_SECRET)], vec![call()])
            .with_extensions(extensions()),
        FinishReason::Other(ASSISTANT_SECRET.to_owned()),
    )
    .with_usage(usage.clone())
    .with_extensions(extensions());
    let metadata = ModelMetadata::new(
        ProviderId::new("safe-provider").expect("valid provider"),
        ModelId::new("safe-model").expect("valid model"),
        ModelCapabilities::new(),
    )
    .with_extensions(extensions());

    assert_debug_redacted(&extensions(), EXTENSION_SECRET, "safe.extension.key");
    assert_debug_redacted(&generation, PROMPT_SECRET, "stop_sequence_bytes");
    assert_debug_redacted(&request, PROMPT_SECRET, "ChatRequest");
    assert_debug_redacted(&request, SCHEMA_SECRET, "safe-tool");
    assert_debug_redacted(&request, EXTENSION_SECRET, "safe.extension.key");
    assert_debug_redacted(&usage, EXTENSION_SECRET, "input_tokens");
    assert_debug_redacted(&response, ASSISTANT_SECRET, "ChatResponse");
    assert_debug_redacted(&response, TOOL_ARGUMENTS_SECRET, "call-safe-id");
    assert_debug_redacted(&response, EXTENSION_SECRET, "safe.extension.key");
    assert_debug_redacted(&metadata, EXTENSION_SECRET, "safe-provider");
}

#[test]
fn stream_debug_redacts_text_arguments_and_extension_values_separately() {
    let delta = ToolCallDelta::new(0)
        .with_id(ToolCallId::new("call-safe-id").expect("valid call id"))
        .with_name(ToolName::new("safe-tool").expect("valid tool"))
        .with_arguments_fragment(STREAM_ARGUMENT_SECRET)
        .with_extensions(extensions());

    assert_debug_redacted(&delta, STREAM_ARGUMENT_SECRET, "arguments_fragment_bytes");
    assert_debug_redacted(&delta, EXTENSION_SECRET, "safe.extension.key");
    assert_debug_redacted(
        &ChatStreamEvent::ToolCallDelta(delta),
        STREAM_ARGUMENT_SECRET,
        "ToolCallDelta",
    );
    assert_debug_redacted(
        &ChatStreamEvent::TextDelta(STREAM_TEXT_SECRET.to_owned()),
        STREAM_TEXT_SECRET,
        "TextDelta",
    );
    assert_debug_redacted(
        &ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: extensions(),
        },
        EXTENSION_SECRET,
        "ResponseStarted",
    );
}

#[derive(Debug)]
struct ProviderRoot;

impl fmt::Display for ProviderRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(PROVIDER_SOURCE_SECRET)
    }
}

impl StdError for ProviderRoot {}

#[test]
fn model_error_debug_and_display_redact_message_and_source_but_preserve_chain() {
    let error = ModelError::with_source(
        ModelErrorKind::Authentication,
        format!("request={PROVIDER_MESSAGE_SECRET}"),
        ProviderRoot,
    );

    for rendered in [format!("{error:?}"), error.to_string()] {
        assert!(
            !rendered.contains(PROVIDER_MESSAGE_SECRET),
            "provider message leaked: {rendered}"
        );
        assert!(
            !rendered.contains(PROVIDER_SOURCE_SECRET),
            "provider source leaked: {rendered}"
        );
    }
    assert_eq!(
        error.as_message(),
        format!("request={PROVIDER_MESSAGE_SECRET}")
    );
    let source = error.source().expect("concrete provider source remains");
    assert!(source.is::<ProviderRoot>());
    assert_eq!(source.to_string(), PROVIDER_SOURCE_SECRET);
}
