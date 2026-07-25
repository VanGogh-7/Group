use group_agent_model::{
    ContentPart, ToolCall, ToolCallId, ToolChoice, ToolDefinition, ToolName, ToolResult,
};
use serde_json::json;

#[test]
fn tool_definition_is_provider_neutral_data() {
    let name = ToolName::new("lookup").expect("valid tool name");
    let schema = json!({"type": "object", "properties": {"id": {"type": "string"}}});
    let tool = ToolDefinition::new(name.clone(), "Find an item", schema.clone());

    assert_eq!(tool.name(), &name);
    assert_eq!(tool.description(), "Find an item");
    assert_eq!(tool.input_schema(), &schema);
}

#[test]
fn complete_tool_call_uses_structured_arguments() {
    let arguments = json!({"id": 42});
    let call = ToolCall::new(
        ToolCallId::new("call-42").expect("valid id"),
        ToolName::new("lookup").expect("valid name"),
        arguments.clone(),
    );

    assert_eq!(call.arguments(), &arguments);
}

#[test]
fn tool_choices_cover_common_selection_modes() {
    let named = ToolChoice::Named(ToolName::new("lookup").expect("valid name"));

    assert_eq!(ToolChoice::default(), ToolChoice::Auto);
    assert_eq!(ToolChoice::None, ToolChoice::None);
    assert_eq!(ToolChoice::Required, ToolChoice::Required);
    assert!(matches!(named, ToolChoice::Named(_)));
}

#[test]
fn tool_results_allow_ordered_empty_content() {
    let result = ToolResult::new(
        vec![ContentPart::text(""), ContentPart::text("tail")],
        false,
    );

    assert!(!result.is_error());
    assert_eq!(result.content()[0].as_text(), Some(""));
    assert_eq!(result.content()[1].as_text(), Some("tail"));
}
