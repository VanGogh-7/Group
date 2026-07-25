use group_agent_model::{
    AssistantMessage, ChatRequest, ContentPart, Extensions, GenerationConfig, Message,
    RequestValidationError, ToolCall, ToolCallId, ToolChoice, ToolDefinition, ToolName, ToolResult,
};
use serde_json::json;

fn tool(name: &str) -> ToolDefinition {
    ToolDefinition::new(
        ToolName::new(name).expect("valid tool name"),
        "description",
        json!({"type": "object"}),
    )
}

#[test]
fn empty_messages_are_rejected() {
    assert_eq!(
        ChatRequest::new(Vec::new())
            .validate()
            .expect_err("empty request"),
        RequestValidationError::EmptyMessages
    );
}

#[test]
fn duplicate_tool_definitions_are_rejected() {
    let request = ChatRequest::new(vec![Message::user("hello")])
        .with_tools(vec![tool("lookup"), tool("lookup")]);

    assert!(matches!(
        request.validate(),
        Err(RequestValidationError::DuplicateToolDefinition { name })
            if name.as_str() == "lookup"
    ));
}

#[test]
fn named_choice_must_reference_declared_tool() {
    let request = ChatRequest::new(vec![Message::user("hello")])
        .with_tools(vec![tool("lookup")])
        .with_tool_choice(ToolChoice::Named(
            ToolName::new("missing").expect("valid name"),
        ));

    assert!(matches!(
        request.validate(),
        Err(RequestValidationError::UnknownNamedTool { name })
            if name.as_str() == "missing"
    ));
}

#[test]
fn invalid_generation_values_are_structured() {
    let nan = ChatRequest::new(vec![Message::user("hello")])
        .with_generation(GenerationConfig::new().with_temperature(f64::NAN));
    let top_p = ChatRequest::new(vec![Message::user("hello")])
        .with_generation(GenerationConfig::new().with_top_p(1.1));
    let zero_tokens = ChatRequest::new(vec![Message::user("hello")])
        .with_generation(GenerationConfig::new().with_max_output_tokens(0));
    let empty_stop = ChatRequest::new(vec![Message::user("hello")])
        .with_generation(GenerationConfig::new().with_stop_sequences([""]));

    assert!(matches!(
        nan.validate(),
        Err(RequestValidationError::InvalidTemperature { value }) if value.is_nan()
    ));
    assert!(matches!(
        top_p.validate(),
        Err(RequestValidationError::InvalidTopP { value }) if value == 1.1
    ));
    assert_eq!(
        zero_tokens.validate(),
        Err(RequestValidationError::ZeroMaxOutputTokens)
    );
    assert_eq!(
        empty_stop.validate(),
        Err(RequestValidationError::EmptyStopSequence { index: 0 })
    );
}

#[test]
fn generation_and_extensions_round_trip_without_changing_messages() {
    let generation = GenerationConfig::new()
        .with_temperature(0.4)
        .with_top_p(0.8)
        .with_max_output_tokens(512)
        .with_stop_sequences(["END", "STOP"])
        .with_parallel_tool_calls(true);
    let extensions = Extensions::new()
        .with("adapter.option", json!({"enabled": true}))
        .expect("valid extension");
    let request = ChatRequest::new(vec![Message::user("hello")])
        .with_generation(generation.clone())
        .with_extensions(extensions.clone());

    request.validate().expect("request is valid");
    assert_eq!(request.generation(), &generation);
    assert_eq!(request.extensions(), &extensions);
    assert_eq!(request.messages(), [Message::user("hello")]);
}

#[test]
fn provider_neutral_sampling_boundaries_are_validated() {
    for value in [0.0, 1.0] {
        ChatRequest::new(vec![Message::user("hello")])
            .with_generation(GenerationConfig::new().with_top_p(value))
            .validate()
            .expect("inclusive top_p boundary");
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1] {
        assert!(matches!(
            ChatRequest::new(vec![Message::user("hello")])
                .with_generation(GenerationConfig::new().with_top_p(value))
                .validate(),
            Err(RequestValidationError::InvalidTopP { .. })
        ));
    }

    for value in [0.0, 2.0, 100.0] {
        ChatRequest::new(vec![Message::user("hello")])
            .with_generation(GenerationConfig::new().with_temperature(value))
            .validate()
            .expect("finite non-negative temperature");
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1] {
        assert!(matches!(
            ChatRequest::new(vec![Message::user("hello")])
                .with_generation(GenerationConfig::new().with_temperature(value))
                .validate(),
            Err(RequestValidationError::InvalidTemperature { .. })
        ));
    }
}

#[test]
fn empty_stop_list_and_duplicates_are_provider_neutral() {
    for stops in [Vec::<String>::new(), vec!["END".into(), "END".into()]] {
        ChatRequest::new(vec![Message::user("hello")])
            .with_generation(GenerationConfig::new().with_stop_sequences(stops))
            .validate()
            .expect("no provider-specific count or uniqueness limit");
    }
}

#[test]
fn tool_results_must_reference_an_earlier_call_once() {
    let id = ToolCallId::new("call-1").expect("valid call id");
    let unknown = ChatRequest::new(vec![
        Message::user("hello"),
        Message::tool(id.clone(), ToolResult::text("result")),
    ]);
    assert!(matches!(
        unknown.validate(),
        Err(RequestValidationError::UnknownToolCallReference { .. })
    ));

    let call = ToolCall::new(
        id.clone(),
        ToolName::new("lookup").expect("valid tool name"),
        json!({}),
    );
    let duplicate = ChatRequest::new(vec![
        Message::user("hello"),
        Message::Assistant(AssistantMessage::new(
            vec![ContentPart::text("")],
            vec![call],
        )),
        Message::tool(id.clone(), ToolResult::text("first")),
        Message::tool(id, ToolResult::text("second")),
    ]);
    assert!(matches!(
        duplicate.validate(),
        Err(RequestValidationError::DuplicateToolResult { .. })
    ));
}
