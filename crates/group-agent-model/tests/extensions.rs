use group_agent_model::{
    AssistantMessage, ChatRequest, ExtensionError, ExtensionMergeError, Extensions, Message,
    ToolCall, ToolCallId, ToolName,
};
use serde_json::json;

fn extensions() -> Extensions {
    Extensions::new()
        .with("provider.continuation", json!({"opaque": "value"}))
        .expect("valid extensions")
}

#[test]
fn assistant_and_tool_call_extensions_round_trip() {
    let tool_call = ToolCall::new(
        ToolCallId::new("call-1").expect("valid call id"),
        ToolName::new("lookup").expect("valid tool name"),
        json!({"query": "rust"}),
    )
    .with_extensions(extensions());
    let assistant =
        AssistantMessage::new(Vec::new(), vec![tool_call.clone()]).with_extensions(extensions());

    assert_eq!(assistant.extensions(), &extensions());
    assert_eq!(assistant.tool_calls()[0].extensions(), &extensions());
    assert_eq!(assistant.tool_calls()[0], tool_call);
}

#[test]
fn extension_keys_are_trimmed_validated_and_stably_ordered() {
    let mut extensions = Extensions::new();
    extensions.insert(" z ", json!(3)).expect("valid key");
    extensions.insert("a", json!(1)).expect("valid key");
    extensions.insert("m", json!(2)).expect("valid key");

    assert_eq!(extensions.keys().collect::<Vec<_>>(), ["a", "m", "z"]);
    assert_eq!(extensions.get(" z "), Some(&json!(3)));
    assert_eq!(
        extensions.insert(" ", json!(0)),
        Err(ExtensionError::EmptyKey)
    );
    assert_eq!(
        extensions.insert("a", json!(9)),
        Err(ExtensionError::DuplicateKey {
            key: "a".to_owned()
        })
    );
}

#[test]
fn assistant_continuation_metadata_survives_next_request() {
    let assistant = AssistantMessage::text("continue").with_extensions(extensions());
    let request = ChatRequest::new(vec![Message::Assistant(assistant.clone())]);

    request.validate().expect("request remains valid");
    let restored = request.messages()[0]
        .as_assistant()
        .expect("assistant message");
    assert_eq!(restored, &assistant);
    assert_eq!(
        restored.extensions().get("provider.continuation"),
        Some(&json!({"opaque": "value"}))
    );
}

#[test]
fn idempotent_merge_is_atomic_and_stably_ordered() {
    let mut existing = Extensions::new()
        .with("m", json!(1))
        .expect("valid existing extension");
    let conflicting = Extensions::try_from_iter([
        ("a", json!("would-be-new")),
        ("m", json!("conflict")),
        ("z", json!("would-be-new")),
    ])
    .expect("valid fragment");

    assert_eq!(
        existing.merge_idempotent(conflicting),
        Err(ExtensionMergeError::ConflictingValue {
            key: "m".to_owned()
        })
    );
    assert_eq!(existing.keys().collect::<Vec<_>>(), ["m"]);
    assert_eq!(existing.get("a"), None);
    assert_eq!(existing.get("z"), None);

    existing
        .merge_idempotent(
            Extensions::try_from_iter([("z", json!(3)), ("a", json!(2)), ("m", json!(1))])
                .expect("valid fragment"),
        )
        .expect("idempotent merge");
    assert_eq!(existing.keys().collect::<Vec<_>>(), ["a", "m", "z"]);
}
