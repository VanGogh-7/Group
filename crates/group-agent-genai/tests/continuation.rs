mod support;

use group_agent_genai::extensions::{PREVIOUS_RESPONSE_ID, STORE, THOUGHT_SIGNATURES};
use group_agent_model::{
    AssistantMessage, ChatRequest, Extensions, Message, ModelCapabilities, ToolDefinition,
    ToolName, ToolResult,
};
use serde_json::json;
use support::{MockResponse, MockServer, stable_responses_model};

fn capabilities() -> ModelCapabilities {
    ModelCapabilities::new()
        .with_streaming(true)
        .with_tool_calling(true)
        .with_usage_reporting(true)
}

fn lookup_tool() -> ToolDefinition {
    ToolDefinition::new(
        ToolName::new("lookup").expect("tool name"),
        "Lookup data",
        json!({
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }),
    )
}

#[tokio::test]
async fn tool_signature_result_and_response_id_round_trip_end_to_end() {
    let mut first_server = MockServer::start(MockResponse::json(
        r#"{
          "id":"resp-first",
          "status":"completed",
          "model":"provider-model",
          "output":[
            {
              "type":"reasoning",
              "id":"reasoning-first",
              "encrypted_content":"signature-first",
              "summary":[]
            },
            {
              "type":"function_call",
              "call_id":"call-first",
              "name":"lookup",
              "arguments":"{\"query\":\"rust\"}"
            }
          ]
        }"#,
    ))
    .await
    .expect("first server");
    let first_model =
        stable_responses_model(first_server.base_url(), capabilities()).expect("first model");
    let first = first_model
        .complete(
            ChatRequest::new(vec![Message::user("find rust")]).with_tools(vec![lookup_tool()]),
        )
        .await
        .expect("first response");

    let provider_call = &first.message().tool_calls()[0];
    assert_eq!(
        provider_call.extensions().get(THOUGHT_SIGNATURES),
        Some(&json!(["signature-first"]))
    );
    let call = provider_call.clone();
    let response_id = first
        .response_id()
        .expect("response id")
        .as_str()
        .to_owned();
    let first_wire = first_server.request_json().await;
    assert_eq!(first_wire["include"][0], "reasoning.encrypted_content");
    assert_eq!(first_wire["reasoning"]["summary"], "detailed");

    let first_debug = format!("{first:?}");
    assert!(!first_debug.contains("signature-first"));
    assert!(!first_debug.contains(r#"{"query":"rust"}"#));
    assert!(!first_debug.contains("resp-first"));

    let mut second_server = MockServer::start(MockResponse::json(
        r#"{
          "id":"resp-second",
          "status":"completed",
          "model":"provider-model",
          "output":[{
            "type":"message",
            "role":"assistant",
            "content":[{"type":"output_text","text":"finished","annotations":[]}]
          }]
        }"#,
    ))
    .await
    .expect("second server");
    let second_model =
        stable_responses_model(second_server.base_url(), capabilities()).expect("second model");
    let mut extensions = Extensions::new();
    extensions
        .insert(PREVIOUS_RESPONSE_ID, json!(response_id))
        .expect("previous response");
    extensions.insert(STORE, json!(true)).expect("store");
    let second_request = ChatRequest::new(vec![
        Message::user("find rust"),
        Message::Assistant(AssistantMessage::new(Vec::new(), vec![call.clone()])),
        Message::tool(call.id().clone(), ToolResult::text("tool-output-first")),
        Message::user("use that result"),
    ])
    .with_extensions(extensions);
    let second = second_model
        .complete(second_request)
        .await
        .expect("second response");
    assert_eq!(second.message().text_content(), "finished");

    let wire = second_server.request_json().await;
    assert_eq!(wire["previous_response_id"], "resp-first");
    assert_eq!(wire["store"], true);
    assert_eq!(wire["input"][0]["role"], "user");
    assert_eq!(wire["input"][1]["type"], "reasoning");
    assert_eq!(wire["input"][1]["encrypted_content"], "signature-first");
    assert_eq!(wire["input"][2]["type"], "function_call");
    assert_eq!(wire["input"][2]["call_id"], "call-first");
    assert_eq!(wire["input"][2]["name"], "lookup");
    assert_eq!(wire["input"][2]["arguments"], r#"{"query":"rust"}"#);
    assert_eq!(wire["input"][3]["type"], "function_call_output");
    assert_eq!(wire["input"][3]["call_id"], "call-first");
    assert_eq!(wire["input"][3]["output"], "tool-output-first");
    assert_eq!(wire["input"][4]["role"], "user");

    let second_debug = format!("{second:?}");
    assert!(!second_debug.contains("resp-second"));
    assert!(!second_debug.contains("signature-first"));
    assert!(!second_debug.contains("tool-output-first"));
}
