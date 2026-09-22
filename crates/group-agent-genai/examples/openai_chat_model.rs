use genai::adapter::AdapterKind;
use genai::resolver::{AuthData, Endpoint};
use genai::{ClientConfig, ModelIden, ServiceTarget};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig, GenaiStreamingLimits,
    GenaiStreamingPolicy,
};
use group_agent_model::{
    ChatModel, ChatRequest, Message, ModelCapabilities, ModelId, ProviderId, ToolDefinition,
    ToolName,
};
use serde_json::json;

/// Configuration and lazy-stream construction only: no credentials or network.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metadata = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("openai")?,
        ModelId::new("gpt-4o-mini")?,
        ModelCapabilities::new()
            .with_streaming(true)
            .with_tool_calling(true)
            .with_usage_reporting(true),
    )?;
    // Applications supply their own endpoint and explicit authentication.
    // This unused loopback address intentionally cannot contact a provider.
    let target = ServiceTarget {
        endpoint: Endpoint::from_static("http://127.0.0.1:1/v1/"),
        auth: AuthData::None,
        model: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
    };
    let adapter = GenaiChatModelAdapter::new_with_stable_target(
        ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
        target,
        GenaiAdapterConfig::new(metadata)
            .with_streaming_policy(GenaiStreamingPolicy::OpenAiChat)
            .with_streaming_limits(
                GenaiStreamingLimits::new()
                    .with_max_sse_event_bytes(1024 * 1024)
                    .with_max_tool_argument_bytes(16 * 1024 * 1024),
            ),
    )?;
    let model = ChatModel::from_adapter(adapter)?;
    let request =
        ChatRequest::new(vec![Message::user("Look up the requested item")]).with_tools(vec![
            ToolDefinition::new(
                ToolName::new("lookup")?,
                "Look up an item",
                json!({"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}),
            ),
        ]);
    let stream = model.stream(request).await?;
    // Polling would dispatch HTTP. Drop here to keep the example fully offline.
    drop(stream);
    println!("Strict OpenAI Chat tool stream configured; no HTTP request dispatched.");
    Ok(())
}
