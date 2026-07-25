use genai::{Client, adapter::AdapterKind};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig, GenaiStreamingPolicy,
};
use group_agent_model::{ChatModel, ModelCapabilities, ModelId, ProviderId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .with_adapter_kind(AdapterKind::OpenAI)
        .build();
    let model_config = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("openai")?,
        ModelId::new("gpt-4o-mini")?,
        ModelCapabilities::new()
            .with_streaming(true)
            .with_tool_calling(true)
            .with_usage_reporting(true),
    )?;
    let adapter = GenaiChatModelAdapter::new(
        client,
        GenaiAdapterConfig::new(model_config)
            .with_response_id_continuation(true)
            .with_streaming_policy(GenaiStreamingPolicy::AuditedTextOnly),
    )?;
    let model = ChatModel::from_adapter(adapter)?;

    println!("{:?}", model.metadata());
    Ok(())
}
