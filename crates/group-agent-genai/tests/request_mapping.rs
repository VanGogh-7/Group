mod support;

use group_agent_genai::extensions::{PREVIOUS_RESPONSE_ID, STORE, THOUGHT_SIGNATURES};
use group_agent_model::{
    AssistantMessage, ChatRequest, ContentPart, Extensions, GenerationConfig, Message,
    ModelCapabilities, ModelErrorKind, SystemMessage, ToolCall, ToolCallId, ToolChoice,
    ToolDefinition, ToolName, ToolResult, UserMessage,
};
use serde_json::json;
use support::{
    MockResponse, MockServer, model, openai_client, stable_openai_model, stable_responses_model,
};

fn capabilities() -> ModelCapabilities {
    ModelCapabilities::new()
        .with_streaming(true)
        .with_tool_calling(true)
        .with_usage_reporting(true)
}

fn success_response() -> MockResponse {
    MockResponse::json(
        r#"{
          "model":"provider-model",
          "choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]
        }"#,
    )
}

#[tokio::test]
async fn ordered_messages_tools_generation_and_continuation_reach_genai_wire() {
    let mut server = MockServer::start(success_response()).await.expect("server");
    let model = stable_openai_model(server.base_url(), capabilities()).expect("model");

    let tool_name = ToolName::new("lookup").expect("tool name");
    let call_id = ToolCallId::new("call-1").expect("call id");
    let mut call_extensions = Extensions::new();
    call_extensions
        .insert(THOUGHT_SIGNATURES, json!(["signature-sentinel"]))
        .expect("extension");
    let call = ToolCall::new(call_id.clone(), tool_name.clone(), json!({"city":"Paris"}))
        .with_extensions(call_extensions);
    let assistant = AssistantMessage::new(
        vec![ContentPart::text("before"), ContentPart::text("")],
        vec![call],
    );

    let messages = vec![
        Message::System(SystemMessage::new(vec![
            ContentPart::text("system-one"),
            ContentPart::text(""),
        ])),
        Message::User(UserMessage::new(vec![
            ContentPart::text("user-one"),
            ContentPart::text("user-two"),
        ])),
        Message::system("system-two"),
        Message::Assistant(assistant),
        Message::tool(call_id, ToolResult::error_text("tool-output-sentinel")),
        Message::user("last-user"),
    ];
    let tool = ToolDefinition::new(
        tool_name.clone(),
        "lookup description",
        json!({
            "type":"object",
            "properties":{"city":{"type":["string","null"]}},
            "additionalProperties":false
        }),
    );
    let generation = GenerationConfig::new()
        .with_temperature(0.25)
        .with_top_p(0.0)
        .with_max_output_tokens(123)
        .with_stop_sequences(["first-stop", "second-stop"]);
    let mut extensions = Extensions::new();
    extensions
        .insert(PREVIOUS_RESPONSE_ID, json!("response-previous"))
        .expect("extension");
    extensions.insert(STORE, json!(true)).expect("extension");
    extensions
        .insert("other.adapter.private", json!("do-not-forward"))
        .expect("extension");
    extensions
        .insert("authorization", json!("secret-header-sentinel"))
        .expect("extension");

    let request = ChatRequest::new(messages)
        .with_tools(vec![tool])
        .with_tool_choice(ToolChoice::Named(tool_name))
        .with_generation(generation)
        .with_extensions(extensions);
    model.complete(request).await.expect("completion");

    let wire = server.request_json().await;
    let roles: Vec<_> = wire["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|message| message["role"].as_str().expect("role"))
        .collect();
    assert_eq!(
        roles,
        ["system", "user", "system", "assistant", "tool", "user"]
    );
    assert_eq!(wire["messages"][0]["content"], "system-one");
    assert_eq!(wire["messages"][1]["content"], "user-oneuser-two");
    assert_eq!(wire["messages"][3]["content"], "before");
    assert_eq!(
        wire["messages"][3]["tool_calls"][0]["function"]["arguments"],
        r#"{"city":"Paris"}"#
    );
    assert_eq!(wire["messages"][4]["tool_call_id"], "call-1");
    assert_eq!(
        wire["messages"][4]["content"], "tool-output-sentinel",
        "is_error must not rewrite tool output"
    );
    assert_eq!(wire["tools"][0]["function"]["name"], "lookup");
    assert_eq!(
        wire["tools"][0]["function"]["parameters"]["additionalProperties"],
        false
    );
    assert_eq!(
        wire["tools"][0]["function"]["parameters"]["properties"]["city"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(wire["tool_choice"]["function"]["name"], "lookup");
    assert_eq!(wire["temperature"], 0.25);
    assert_eq!(wire["top_p"], 0.0);
    assert_eq!(wire["max_tokens"], 123);
    assert_eq!(wire["stop"], json!(["first-stop", "second-stop"]));
    let encoded = wire.to_string();
    assert!(!encoded.contains("do-not-forward"));
    assert!(!encoded.contains("secret-header-sentinel"));
}

#[tokio::test]
async fn responses_api_preserves_explicit_state_and_thought_continuation() {
    let mut server = MockServer::start(MockResponse::json(
        r#"{
          "id":"resp-current",
          "status":"completed",
          "model":"provider-response-model",
          "output":[{
            "type":"message",
            "role":"assistant",
            "content":[{"type":"output_text","text":"done","annotations":[]}]
          }]
        }"#,
    ))
    .await
    .expect("server");
    let model = stable_responses_model(server.base_url(), capabilities()).expect("model");
    let mut signature_extensions = Extensions::new();
    signature_extensions
        .insert(THOUGHT_SIGNATURES, json!(["signature-sentinel"]))
        .expect("signature");
    let call_id = ToolCallId::new("call-continuation").expect("id");
    let call = ToolCall::new(
        call_id.clone(),
        ToolName::new("lookup").expect("name"),
        json!({"key":"value"}),
    )
    .with_extensions(signature_extensions);
    let mut request_extensions = Extensions::new();
    request_extensions
        .insert(PREVIOUS_RESPONSE_ID, json!("resp-previous"))
        .expect("previous response");
    request_extensions
        .insert(STORE, json!(true))
        .expect("store");
    let request = ChatRequest::new(vec![
        Message::Assistant(AssistantMessage::new(Vec::new(), vec![call])),
        Message::tool(call_id, ToolResult::text("tool-result")),
        Message::user("continue"),
    ])
    .with_extensions(request_extensions);

    let response = model.complete(request).await.expect("completion");
    assert_eq!(
        response.response_id().expect("response id").as_str(),
        "resp-current"
    );
    let wire = server.request_json().await;
    assert_eq!(wire["previous_response_id"], "resp-previous");
    assert_eq!(wire["store"], true);
    assert_eq!(wire["input"][0]["type"], "reasoning");
    assert_eq!(wire["input"][0]["encrypted_content"], "signature-sentinel");
    assert_eq!(wire["input"][1]["type"], "function_call");
    assert_eq!(wire["input"][1]["call_id"], "call-continuation");
    assert_eq!(wire["input"][1]["name"], "lookup");
    assert_eq!(wire["input"][1]["arguments"], r#"{"key":"value"}"#);
    assert_eq!(wire["input"][2]["type"], "function_call_output");
    assert_eq!(wire["input"][2]["call_id"], "call-continuation");
    assert_eq!(wire["input"][2]["output"], "tool-result");
    assert_eq!(wire["input"][3]["role"], "user");
}

#[tokio::test]
async fn all_tool_choices_map_without_downgrade() {
    let cases = [
        (ToolChoice::Auto, json!("auto")),
        (ToolChoice::None, json!("none")),
        (ToolChoice::Required, json!("required")),
        (
            ToolChoice::Named(ToolName::new("lookup").expect("name")),
            json!({"type":"function","function":{"name":"lookup"}}),
        ),
    ];
    for (choice, expected) in cases {
        let mut server = MockServer::start(success_response()).await.expect("server");
        let model = stable_openai_model(server.base_url(), capabilities()).expect("model");
        let tool = ToolDefinition::new(
            ToolName::new("lookup").expect("name"),
            "lookup",
            json!({"type":"object"}),
        );
        model
            .complete(
                ChatRequest::new(vec![Message::user("hello")])
                    .with_tools(vec![tool])
                    .with_tool_choice(choice),
            )
            .await
            .expect("completion");
        let wire = server.request_json().await;
        assert_eq!(wire["tool_choice"], expected);
    }
}

#[tokio::test]
async fn adapter_owned_unknown_extension_and_parallel_control_are_rejected() {
    let server = MockServer::start(success_response()).await.expect("server");
    let adapter_model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let extensions = Extensions::new()
        .with("group.genai.authorization", json!("must-not-pass"))
        .expect("extension");
    let error = adapter_model
        .complete(ChatRequest::new(vec![Message::user("hello")]).with_extensions(extensions))
        .await
        .expect_err("unknown owned extension");
    assert_eq!(error.kind(), &ModelErrorKind::InvalidRequest);
    assert!(!format!("{error:?}").contains("must-not-pass"));
    drop(server);

    let server = MockServer::start(success_response()).await.expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let error = model
        .complete(
            ChatRequest::new(vec![Message::user("hello")])
                .with_generation(GenerationConfig::new().with_parallel_tool_calls(false)),
        )
        .await
        .expect_err("unsupported control must not be ignored");
    assert_eq!(error.kind(), &ModelErrorKind::InvalidRequest);
    drop(server);
}
