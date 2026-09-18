#![cfg(feature = "serde")]

use group_agent_model::{
    AssistantMessage, ContentPart, Extensions, Message, Role, SystemMessage, TokenUsage, ToolCall,
    ToolCallId, ToolMessage, ToolName, ToolResult, UserMessage,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

fn call_id(value: &str) -> ToolCallId {
    ToolCallId::new(value).expect("test call id is valid")
}

fn tool_name(value: &str) -> ToolName {
    ToolName::new(value).expect("test tool name is valid")
}

fn nested_extensions() -> Extensions {
    Extensions::new()
        .with(
            "reasoning",
            json!({"summary": "step one", "tokens": {"draft": 12, "final": 4}}),
        )
        .and_then(|extensions| extensions.with("citations", json!(["doc-1", "doc-2"])))
        .expect("test extensions are valid")
}

fn assert_canonical_roundtrip<T>(value: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let first = serde_json::to_vec(value).expect("value serializes");
    let decoded: T = serde_json::from_slice(&first).expect("value deserializes");
    assert_eq!(&decoded, value, "roundtrip preserves the value");
    let second = serde_json::to_vec(&decoded).expect("decoded value serializes");
    assert_eq!(
        first, second,
        "serialize→deserialize→serialize is canonical"
    );
}

#[test]
fn role_roundtrips_canonically() {
    for role in [Role::System, Role::User, Role::Assistant, Role::Tool] {
        assert_canonical_roundtrip(&role);
    }
}

#[test]
fn content_part_variants_roundtrip_canonically() {
    assert_canonical_roundtrip(&ContentPart::text("hello"));
    assert_canonical_roundtrip(&ContentPart::text(""));
}

#[test]
fn extensions_with_nested_values_roundtrip_canonically() {
    assert_canonical_roundtrip(&Extensions::new());
    assert_canonical_roundtrip(&nested_extensions());
}

#[test]
fn tool_call_with_arguments_roundtrips_canonically() {
    let call = ToolCall::new(
        call_id("call-1"),
        tool_name("search"),
        json!({"q": "rust", "limit": 10, "filters": {"lang": ["en", "de"]}}),
    )
    .with_extensions(nested_extensions());

    assert_canonical_roundtrip(&call);
}

#[test]
fn tool_message_roundtrips_canonically() {
    let message = ToolMessage::new(call_id("call-9"), ToolResult::error_text("not found"));

    assert_canonical_roundtrip(&message);
}

#[test]
fn every_message_variant_roundtrips_canonically() {
    let tool_call = ToolCall::new(call_id("call-1"), tool_name("search"), json!({"q": "rust"}))
        .with_extensions(nested_extensions());
    let messages = [
        Message::System(SystemMessage::new(vec![ContentPart::text("rules")])),
        Message::User(UserMessage::new(vec![
            ContentPart::text("first"),
            ContentPart::text(""),
        ])),
        Message::Assistant(
            AssistantMessage::new(vec![ContentPart::text("Checking")], vec![tool_call])
                .with_extensions(nested_extensions()),
        ),
        Message::Tool(ToolMessage::new(
            call_id("call-1"),
            ToolResult::text("result"),
        )),
    ];

    for message in &messages {
        assert_canonical_roundtrip(message);
    }
}

#[test]
fn token_usage_roundtrips_canonically() {
    assert_canonical_roundtrip(&TokenUsage::new());

    let usage = TokenUsage::from_parts(Some(10), Some(4), Some(16))
        .expect("test usage is consistent")
        .with_extensions(nested_extensions());
    assert_canonical_roundtrip(&usage);

    let partial = TokenUsage::from_parts(None, Some(7), None).expect("partial usage is consistent");
    assert_canonical_roundtrip(&partial);
}

#[test]
fn invalid_identifiers_are_rejected_on_decode() {
    let whitespace_id = serde_json::from_slice::<ToolCallId>(br#""   ""#)
        .expect_err("whitespace-only ToolCallId must be rejected");
    assert!(
        whitespace_id.to_string().contains("must not be empty"),
        "unexpected error: {whitespace_id}"
    );

    let empty_name =
        serde_json::from_slice::<ToolName>(br#""""#).expect_err("empty ToolName must be rejected");
    assert!(
        empty_name.to_string().contains("must not be empty"),
        "unexpected error: {empty_name}"
    );
}

#[test]
fn inconsistent_token_usage_is_rejected_on_decode() {
    let bytes = br#"{"input_tokens":10,"output_tokens":4,"total_tokens":5,"extensions":{}}"#;

    let error = serde_json::from_slice::<TokenUsage>(bytes)
        .expect_err("inconsistent TokenUsage must be rejected");

    assert!(
        error.to_string().contains("inconsistent"),
        "unexpected error: {error}"
    );
}

#[test]
fn extensions_decode_validates_keys() {
    let blank = serde_json::from_slice::<Extensions>(br#"{"  ": 1}"#)
        .expect_err("blank extension key must be rejected");
    assert!(
        blank.to_string().contains("must not be empty"),
        "unexpected error: {blank}"
    );

    let normalized: Extensions =
        serde_json::from_slice(br#"{" key ": 1}"#).expect("key is trimmed on decode");
    assert!(normalized.get("key").is_some());
}
