use group_agent_model::{
    AssistantMessage, ContentPart, IdentifierError, Message, Role, SystemMessage, ToolCall,
    ToolCallId, ToolName, ToolResult, UserMessage,
};
use serde_json::json;

fn call_id(value: &str) -> ToolCallId {
    ToolCallId::new(value).expect("test call id is valid")
}

fn tool_name(value: &str) -> ToolName {
    ToolName::new(value).expect("test tool name is valid")
}

#[test]
fn constructs_each_strong_message_variant() {
    let system = Message::System(SystemMessage::new(vec![ContentPart::text("rules")]));
    let user = Message::User(UserMessage::new(vec![ContentPart::text("question")]));
    let assistant = Message::assistant("answer");
    let tool = Message::tool(call_id("call-1"), ToolResult::text("result"));

    assert_eq!(system.role(), Role::System);
    assert_eq!(user.role(), Role::User);
    assert_eq!(assistant.role(), Role::Assistant);
    assert_eq!(tool.role(), Role::Tool);
}

#[test]
fn assistant_text_and_tool_calls_coexist() {
    let call = ToolCall::new(call_id("call-1"), tool_name("search"), json!({"q": "rust"}));
    let message = AssistantMessage::new(vec![ContentPart::text("Checking")], vec![call.clone()]);

    assert_eq!(message.text_content(), "Checking");
    assert_eq!(message.tool_calls(), [call]);
}

#[test]
fn tool_result_keeps_call_id_and_business_error_flag() {
    let id = call_id("call-9");
    let message = Message::tool(id.clone(), ToolResult::error_text("not found"));
    let tool = message.as_tool().expect("tool message");

    assert_eq!(tool.tool_call_id(), &id);
    assert!(tool.result().is_error());
    assert_eq!(tool.result().content()[0].as_text(), Some("not found"));
}

#[test]
fn content_order_and_empty_text_are_preserved() {
    let message = Message::User(UserMessage::new(vec![
        ContentPart::text("first"),
        ContentPart::text(""),
        ContentPart::text("third"),
    ]));

    assert_eq!(
        message
            .content()
            .iter()
            .map(ContentPart::as_text)
            .collect::<Vec<_>>(),
        [Some("first"), Some(""), Some("third")]
    );
}

#[test]
fn text_helpers_preserve_text_order() {
    let message = Message::User(UserMessage::new(vec![
        ContentPart::text("one"),
        ContentPart::text("two"),
    ]));

    assert!(message.has_text());
    assert_eq!(message.text_parts().collect::<Vec<_>>(), ["one", "two"]);
    assert_eq!(message.text_content(), "onetwo");
}

#[test]
fn empty_tool_names_and_ids_are_rejected() {
    assert_eq!(
        ToolName::new(" \t").expect_err("empty tool name"),
        IdentifierError::EmptyToolName
    );
    assert_eq!(
        ToolCallId::new("").expect_err("empty call id"),
        IdentifierError::EmptyToolCallId
    );
}

#[test]
fn message_display_does_not_expose_content() {
    let rendered = Message::user("secret-value").to_string();

    assert!(rendered.contains("User"));
    assert!(!rendered.contains("secret-value"));
}
