#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream;
use group_agent_core::{CheckpointConfig, CheckpointPolicy, InMemoryCheckpointer};
use group_agent_model::{
    AssistantMessage, ChatEventStream, ChatModel, ChatModelAdapter, ChatResponse, ChatStreamEvent,
    FinishReason, Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId,
    TokenUsage, ToolCall, ToolCallDelta, ToolCallId, ToolDefinition, ToolName,
    ValidatedChatRequest,
};
use group_agent_prebuilt::{
    AgentConfig, AgentEventSink, AgentSnapshot, AgentSnapshotCodec, AgentStreamEvent,
    ToolCallingAgent,
};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};

#[derive(Default)]
pub struct Probe {
    pub fail_final_once: AtomicBool,
    pub streams: AtomicUsize,
    pub completions: AtomicUsize,
    pub executions: AtomicUsize,
    pub transcripts: Mutex<Vec<Vec<Message>>>,
}

pub struct Scripted {
    pub metadata: ModelMetadata,
    pub probe: Arc<Probe>,
}

impl Scripted {
    fn response(&self, request: &ValidatedChatRequest) -> ChatResponse {
        self.probe
            .transcripts
            .lock()
            .unwrap()
            .push(request.messages().to_vec());
        let message = if request.messages().iter().any(|m| m.as_tool().is_some()) {
            AssistantMessage::text("SECRET_FINAL")
        } else {
            AssistantMessage::new(
                Vec::new(),
                vec![ToolCall::new(
                    ToolCallId::new("call-1").unwrap(),
                    ToolName::new("lookup").unwrap(),
                    serde_json::json!({}),
                )],
            )
        };
        let finish = if message.tool_calls().is_empty() {
            FinishReason::Stop
        } else {
            FinishReason::ToolCalls
        };
        ChatResponse::new(message, finish)
            .with_usage(TokenUsage::from_parts(Some(3), Some(2), Some(5)).unwrap())
    }
}

#[async_trait]
impl ChatModelAdapter for Scripted {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }
    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        self.probe.completions.fetch_add(1, Ordering::SeqCst);
        Ok(self.response(&request))
    }
    async fn stream_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        self.probe.streams.fetch_add(1, Ordering::SeqCst);
        let response = self.response(&request);
        let mut events = Vec::new();
        if response.message().tool_calls().is_empty() {
            events.push(Ok(ChatStreamEvent::TextDelta(
                response.message().text_content(),
            )));
        } else {
            events.push(Ok(ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0)
                    .with_id(ToolCallId::new("call-1").unwrap())
                    .with_name(ToolName::new("lookup").unwrap())
                    .with_arguments_fragment("{}"),
            )));
        }
        if response.message().tool_calls().is_empty()
            && self.probe.fail_final_once.swap(false, Ordering::SeqCst)
        {
            events.push(Err(ModelError::new(
                group_agent_model::ModelErrorKind::Other,
                "scripted final stream failure",
            )));
            return Ok(Box::pin(stream::iter(events)));
        }
        events.push(Ok(ChatStreamEvent::Usage(
            response.usage().unwrap().clone(),
        )));
        events.push(Ok(ChatStreamEvent::Finished(
            response.finish_reason().clone(),
        )));
        Ok(Box::pin(stream::iter(events)))
    }
}

pub struct CountingTool {
    pub definition: ToolDefinition,
    pub probe: Arc<Probe>,
}
#[async_trait]
impl Tool for CountingTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }
    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }
    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }
    async fn execute(&self, _: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        self.probe.executions.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::success_text("SECRET_RESULT"))
    }
}

pub fn model(probe: &Arc<Probe>) -> ChatModel {
    ChatModel::from_adapter(Scripted {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("durable-stream").unwrap(),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true),
        ),
        probe: probe.clone(),
    })
    .unwrap()
}
pub fn runtime(probe: &Arc<Probe>) -> ToolRuntime {
    let mut registry = ToolRegistry::builder();
    registry
        .register(CountingTool {
            definition: ToolDefinition::new(
                ToolName::new("lookup").unwrap(),
                "Offline lookup",
                serde_json::json!({"type":"object"}),
            ),
            probe: probe.clone(),
        })
        .unwrap();
    ToolRuntime::new(registry.build())
}
pub fn agent(approval: bool, rounds: usize) -> (ToolCallingAgent, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    (
        ToolCallingAgent::new(
            model(&probe),
            runtime(&probe),
            AgentConfig::new(rounds)
                .unwrap()
                .with_tool_approval(approval),
        )
        .unwrap(),
        probe,
    )
}
pub fn store() -> Arc<InMemoryCheckpointer<AgentSnapshot>> {
    Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec))
}
pub fn config(
    thread: &str,
    store: &Arc<InMemoryCheckpointer<AgentSnapshot>>,
) -> CheckpointConfig<AgentSnapshot> {
    CheckpointConfig::new(thread, store.clone(), CheckpointPolicy::EverySuperstep)
}
#[derive(Default)]
pub struct Events(pub Mutex<Vec<AgentStreamEvent>>);
impl Events {
    pub fn snapshot(&self) -> Vec<AgentStreamEvent> {
        self.0.lock().unwrap().clone()
    }
}
impl AgentEventSink for Events {
    fn on_event(&self, event: &AgentStreamEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}
