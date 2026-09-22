mod support;

#[path = "openai_chat/failures.rs"]
mod failures;

#[path = "openai_chat/transport.rs"]
mod transport;

#[path = "openai_chat/durable.rs"]
mod durable;

use futures_util::StreamExt;
use genai::adapter::AdapterKind;
use genai::resolver::{AuthData, Endpoint};
use genai::{ClientConfig, ModelIden, ServiceTarget};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig, GenaiStreamingPolicy,
};
use group_agent_model::{
    ChatModel, ChatRequest, ChatStreamEvent, FinishReason, Message, ModelCapabilities, ModelId,
    ProviderId, ToolDefinition, ToolName, collect_chat_stream,
};
use serde_json::{Value, json};
use support::{MockResponse, MockServer};

fn config() -> GenaiAdapterConfig {
    GenaiAdapterConfig::new(
        GenaiModelConfig::new(
            "gpt-4o-mini",
            ProviderId::new("openai").unwrap(),
            ModelId::new("configured-model").unwrap(),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true)
                .with_usage_reporting(true),
        )
        .unwrap(),
    )
    .with_streaming_policy(GenaiStreamingPolicy::OpenAiChat)
}

fn target(base_url: &str) -> ServiceTarget {
    ServiceTarget {
        endpoint: Endpoint::from_owned(base_url.to_owned()),
        auth: AuthData::from_single("local-test-only"),
        model: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
    }
}

fn model(base_url: &str) -> ChatModel {
    ChatModel::from_adapter(
        GenaiChatModelAdapter::new_with_stable_target(
            ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
            target(base_url),
            config(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn request() -> ChatRequest {
    ChatRequest::new(vec![Message::user("look up data")]).with_tools(vec![ToolDefinition::new(
        ToolName::new("lookup").unwrap(),
        "Lookup data",
        json!({"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}),
    )])
}

fn chunk(delta: Value, finish: Value) -> String {
    format!(
        "data: {}\n\n",
        json!({
            "id":"chatcmpl-local", "object":"chat.completion.chunk", "model":"gpt-4o-mini",
            "choices":[{"index":0,"delta":delta,"finish_reason":finish}]
        })
    )
}

#[tokio::test]
async fn multi_call_frames_and_interleaved_fragments_reach_the_public_collector() {
    let body = [
        chunk(json!({"role":"assistant","content":"Checking ","tool_calls":[
            {"index":0,"id":"call-a","type":"function","function":{"name":"lookup","arguments":"{\"q\":\""}},
            {"index":1,"id":"call-b","type":"function","function":{"name":"lookup","arguments":"{\"q\":\""}}
        ]}), Value::Null),
        chunk(json!({"tool_calls":[
            {"index":1,"function":{"arguments":"第二\"}"}},
            {"index":0,"function":{"arguments":"first\"}"}}
        ],"content":"both."}), json!("tool_calls")),
        format!("data: {}\n\n", json!({"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":6,"total_tokens":10}})),
        "data: [DONE]\n\n".into(),
    ].concat();
    let mut server = MockServer::start(MockResponse::sse(body)).await.unwrap();
    let mut stream = model(server.base_url()).stream(request()).await.unwrap();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ChatStreamEvent::ToolCallDelta(_)))
            .count(),
        4
    );
    assert!(matches!(
        events.last(),
        Some(ChatStreamEvent::Finished(FinishReason::ToolCalls))
    ));
    let response = collect_chat_stream(futures_util::stream::iter(events.into_iter().map(Ok)))
        .await
        .unwrap();
    assert_eq!(response.message().text_content(), "Checking both.");
    let calls = response.message().tool_calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id().as_str(), "call-a");
    assert_eq!(calls[0].arguments(), &json!({"q":"first"}));
    assert_eq!(calls[1].id().as_str(), "call-b");
    assert_eq!(calls[1].arguments(), &json!({"q":"第二"}));
    assert_eq!(response.usage().unwrap().total_tokens(), Some(10));
    let captured = server.request_json().await;
    assert_eq!(captured["stream"], true);
}
