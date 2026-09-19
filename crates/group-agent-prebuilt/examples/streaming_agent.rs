use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_core::Stream;
use futures_util::StreamExt;
use group_agent_model::{
    AssistantMessage, ChatEventStream, ChatModel, ChatModelAdapter, ChatResponse, ChatStreamEvent,
    FinishReason, Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId,
    ToolCallDelta, ToolCallId, ToolDefinition, ToolName, ValidatedChatRequest,
};
use group_agent_prebuilt::{AgentConfig, AgentStopReason, AgentStreamEvent, ToolCallingAgent};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::json;

struct VectorStream {
    items: std::vec::IntoIter<Result<ChatStreamEvent, ModelError>>,
}

impl Stream for VectorStream {
    type Item = Result<ChatStreamEvent, ModelError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.items.next())
    }
}

type StreamResult = Result<ChatStreamEvent, ModelError>;
type QueuedTurns = Arc<Mutex<VecDeque<Vec<StreamResult>>>>;

struct ScriptedStreamAdapter {
    metadata: ModelMetadata,
    turns: QueuedTurns,
}

#[async_trait]
impl ChatModelAdapter for ScriptedStreamAdapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(
        &self,
        _request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        Ok(ChatResponse::new(
            AssistantMessage::text("complete fallback"),
            FinishReason::Stop,
        ))
    }

    async fn stream_raw(
        &self,
        _request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        let items = self.turns.lock().unwrap().pop_front().unwrap_or_default();
        Ok(Box::pin(VectorStream {
            items: items.into_iter(),
        }))
    }
}

struct SearchTool;

#[async_trait]
impl Tool for SearchTool {
    fn name(&self) -> &ToolName {
        static NAME: std::sync::OnceLock<ToolName> = std::sync::OnceLock::new();
        NAME.get_or_init(|| ToolName::new("search").unwrap())
    }

    fn definition(&self) -> &ToolDefinition {
        static DEF: std::sync::OnceLock<ToolDefinition> = std::sync::OnceLock::new();
        DEF.get_or_init(|| {
            ToolDefinition::new(
                ToolName::new("search").unwrap(),
                "Search offline records",
                json!({
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"]
                }),
            )
        })
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        Ok(ToolOutput::success_text(
            "Rust 2024 edition release details",
        ))
    }
}

fn build_tools() -> Result<ToolRuntime, Box<dyn std::error::Error>> {
    let mut builder = ToolRegistry::builder();
    builder.register(SearchTool)?;
    Ok(ToolRuntime::new(builder.build()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let call_id = ToolCallId::new("call-1")?;
    let tool_name = ToolName::new("search")?;

    // Turn 1: model streams a tool call
    let turn1 = vec![
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: group_agent_model::Extensions::new(),
        }),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(call_id.clone())
                .with_name(tool_name.clone())
                .with_arguments_fragment(r#"{"query":"Rust"}"#),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::ToolCalls)),
    ];

    // Turn 2: model streams text tokens for the final answer
    let turn2 = vec![
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: group_agent_model::Extensions::new(),
        }),
        Ok(ChatStreamEvent::TextDelta("Here is ".to_string())),
        Ok(ChatStreamEvent::TextDelta("the search ".to_string())),
        Ok(ChatStreamEvent::TextDelta("result.".to_string())),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ];

    let adapter = ScriptedStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline")?,
            ModelId::new("offline-stream")?,
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true),
        ),
        turns: Arc::new(Mutex::new(VecDeque::from(vec![turn1, turn2]))),
    };

    let model = ChatModel::from_adapter(adapter)?;
    let tools = build_tools()?;
    let agent = ToolCallingAgent::new(model, tools, AgentConfig::new(2)?)?;

    println!("Starting streaming invocation...");
    let mut stream = agent.stream(vec![Message::user("Search for Rust info")]);

    while let Some(event) = stream.next().await {
        match event? {
            AgentStreamEvent::ModelStarted { round } => {
                println!("==> Model round {round} started");
            }
            AgentStreamEvent::TextDelta { round, delta } => {
                print!("[R{round} text] {delta}");
            }
            AgentStreamEvent::ToolCallDelta { round, .. } => {
                println!("==> Model round {round} emitted a tool call fragment");
            }
            AgentStreamEvent::ModelCompleted { round } => {
                println!("\n==> Model round {round} completed");
            }
            AgentStreamEvent::ToolStarted { name, round, .. } => {
                println!("==> Tool `{name}` started (round {round})");
            }
            AgentStreamEvent::ToolCompleted {
                name,
                is_error,
                round,
                ..
            } => {
                println!("==> Tool `{name}` completed (is_error: {is_error}, round {round})");
            }
            AgentStreamEvent::ApprovalRequired { .. } => {
                println!("==> Approval required before execution");
            }
            AgentStreamEvent::Completed(outcome) => {
                println!(
                    "==> Invocation finished with stop reason: {:?}",
                    outcome.stop_reason()
                );
                assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
                assert_eq!(outcome.model_rounds(), 2);
                assert_eq!(
                    outcome.final_message().unwrap().text_content(),
                    "Here is the search result."
                );
            }
            _ => {}
        }
    }

    println!("\nStreaming agent example completed successfully.");
    Ok(())
}
