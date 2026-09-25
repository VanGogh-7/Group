#![allow(dead_code)]
#[path = "streaming_agent.rs"]
pub mod fixture;
use async_trait::async_trait;
use futures_util::StreamExt;
use group_agent_model::{
    AssistantMessage, ChatEventStream, ChatModel, ChatModelAdapter, ChatResponse, ChatStreamEvent,
    FinishReason, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId,
    StructuredOutput, ValidatedChatRequest,
};
use group_agent_prebuilt::{AgentConfig, ToolCallingAgent};
use serde_json::json;
use std::sync::Arc;

pub fn output(name: &str) -> StructuredOutput {
    StructuredOutput::new(
        name,
        json!({"type":"object","properties":{"answer":{"type":"string"}},
        "required":["answer"],"additionalProperties":false}),
    )
    .unwrap()
}
struct JsonModel {
    inner: fixture::Scripted,
    invalid: bool,
    pause: Option<std::path::PathBuf>,
}
#[async_trait]
impl ChatModelAdapter for JsonModel {
    fn metadata(&self) -> &ModelMetadata {
        self.inner.metadata()
    }
    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        self.pause_if_final(&request).await;
        let response = self.inner.complete_raw(request).await?;
        if response.message().tool_calls().is_empty() {
            let text = if self.invalid {
                "{}"
            } else {
                r#"{"answer":"ok"}"#
            };
            let mut final_response =
                ChatResponse::new(AssistantMessage::text(text), FinishReason::Stop);
            if let Some(usage) = response.usage() {
                final_response = final_response.with_usage(usage.clone());
            }
            Ok(final_response)
        } else {
            Ok(response)
        }
    }
    async fn stream_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        self.pause_if_final(&request).await;
        let stream = self.inner.stream_raw(request).await?;
        let invalid = self.invalid;
        Ok(Box::pin(stream.map(move |event| {
            event.map(|event| match event {
                ChatStreamEvent::TextDelta(_) => ChatStreamEvent::TextDelta(
                    if invalid { "{}" } else { r#"{"answer":"ok"}"# }.into(),
                ),
                other => other,
            })
        })))
    }
}
pub fn agent(
    approval: bool,
    rounds: usize,
    contract: Option<StructuredOutput>,
    invalid: bool,
) -> (ToolCallingAgent, Arc<fixture::Probe>) {
    build(approval, rounds, contract, invalid, None)
}
pub fn build(
    approval: bool,
    rounds: usize,
    contract: Option<StructuredOutput>,
    invalid: bool,
    process: Option<(std::path::PathBuf, bool)>,
) -> (ToolCallingAgent, Arc<fixture::Probe>) {
    let probe = Arc::new(fixture::Probe::default());
    let model = ChatModel::from_adapter(JsonModel {
        inner: fixture::Scripted {
            metadata: ModelMetadata::new(
                ProviderId::new("offline").unwrap(),
                ModelId::new("json").unwrap(),
                ModelCapabilities::new()
                    .with_streaming(true)
                    .with_tool_calling(true)
                    .with_structured_output(true),
            ),
            probe: probe.clone(),
        },
        invalid,
        pause: process
            .as_ref()
            .filter(|(_, pause)| *pause)
            .map(|(directory, _)| directory.clone()),
    })
    .unwrap();
    let runtime = if let Some((directory, _)) = process {
        let mut registry = group_agent_tool::ToolRegistry::builder();
        registry
            .register(JournalTool {
                inner: fixture::CountingTool {
                    definition: group_agent_model::ToolDefinition::new(
                        group_agent_model::ToolName::new("lookup").unwrap(),
                        "journal",
                        json!({"type":"object"}),
                    ),
                    probe: probe.clone(),
                },
                directory,
            })
            .unwrap();
        group_agent_tool::ToolRuntime::new(registry.build())
    } else {
        fixture::runtime(&probe)
    };
    let config = AgentConfig::new(rounds)
        .unwrap()
        .with_tool_approval(approval);
    let agent = if let Some(output) = contract {
        ToolCallingAgent::new_with_output(model, runtime, config, output)
    } else {
        ToolCallingAgent::new(model, runtime, config)
    }
    .unwrap();
    (agent, probe)
}

impl JsonModel {
    async fn pause_if_final(&self, request: &ValidatedChatRequest) {
        if let Some(directory) = &self.pause
            && request.messages().iter().any(|m| m.as_tool().is_some())
        {
            std::fs::write(directory.join("ready"), b"saved-tool").unwrap();
            std::future::pending::<()>().await;
        }
    }
}
struct JournalTool {
    inner: fixture::CountingTool,
    directory: std::path::PathBuf,
}
#[async_trait]
impl group_agent_tool::Tool for JournalTool {
    fn name(&self) -> &group_agent_model::ToolName {
        self.inner.name()
    }
    fn definition(&self) -> &group_agent_model::ToolDefinition {
        self.inner.definition()
    }
    fn behavior(&self) -> group_agent_tool::ToolBehavior {
        group_agent_tool::ToolBehavior::non_idempotent_write()
    }
    async fn execute(
        &self,
        input: group_agent_tool::ToolInput<'_>,
    ) -> Result<group_agent_tool::ToolOutput, group_agent_tool::ToolError> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("executions"))
            .unwrap();
        writeln!(file, "executed").unwrap();
        file.sync_all().unwrap();
        self.inner.execute(input).await
    }
}
