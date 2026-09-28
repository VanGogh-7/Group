//! Independent public-API consumer; no repository test-support imports.
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use group_agent_core::{CheckpointConfig, CheckpointPolicy, InMemoryCheckpointer, ResumeConfig};
use group_agent_model::*;
use group_agent_prebuilt::*;
use group_agent_tool::*;
use serde::Deserialize;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

#[derive(Default)]
struct Counts {
    a: AtomicUsize,
    b: AtomicUsize,
    tool: AtomicUsize,
    mapper: AtomicUsize,
}
#[derive(Deserialize)]
struct Task {
    instruction: String,
}
#[derive(Deserialize)]
struct Answer {
    answer: String,
}

struct OfflineModel {
    metadata: ModelMetadata,
    first: bool,
    counts: Arc<Counts>,
}
impl OfflineModel {
    fn response(&self, request: &ValidatedChatRequest) -> ChatResponse {
        let message = if self.first {
            self.counts.a.fetch_add(1, SeqCst);
            AssistantMessage::text(r#"{"instruction":"mapped-task"}"#)
        } else {
            self.counts.b.fetch_add(1, SeqCst);
            assert_eq!(request.messages()[0], Message::user("mapped-task"));
            if request.messages().iter().any(|m| m.as_tool().is_some()) {
                AssistantMessage::text(r#"{"answer":"done"}"#)
            } else {
                AssistantMessage::new(
                    vec![],
                    vec![ToolCall::new(
                        ToolCallId::new("lookup-1").unwrap(),
                        ToolName::new("lookup").unwrap(),
                        json!({}),
                    )],
                )
            }
        };
        let reason = if message.tool_calls().is_empty() {
            FinishReason::Stop
        } else {
            FinishReason::ToolCalls
        };
        ChatResponse::new(message, reason)
    }
}
#[async_trait]
impl ChatModelAdapter for OfflineModel {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }
    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        Ok(self.response(&request))
    }
    async fn stream_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        let response = self.response(&request);
        let delta = if response.message().tool_calls().is_empty() {
            ChatStreamEvent::TextDelta(response.message().text_content())
        } else {
            ChatStreamEvent::ToolCallDelta(
                ToolCallDelta::new(0)
                    .with_id(ToolCallId::new("lookup-1").unwrap())
                    .with_name(ToolName::new("lookup").unwrap())
                    .with_arguments_fragment("{}"),
            )
        };
        Ok(Box::pin(stream::iter([
            Ok(delta),
            Ok(ChatStreamEvent::Finished(response.finish_reason().clone())),
        ])))
    }
}
struct Lookup {
    definition: ToolDefinition,
    counts: Arc<Counts>,
}
#[async_trait]
impl Tool for Lookup {
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
        self.counts.tool.fetch_add(1, SeqCst);
        Ok(ToolOutput::success_text("found"))
    }
}
fn stage(first: bool, counts: Arc<Counts>) -> AgentStage {
    let mut registry = ToolRegistry::builder();
    if !first {
        registry
            .register(Lookup {
                definition: ToolDefinition::new(
                    ToolName::new("lookup").unwrap(),
                    "Offline lookup",
                    json!({"type":"object"}),
                ),
                counts: counts.clone(),
            })
            .unwrap();
    }
    let key = if first { "instruction" } else { "answer" };
    let output = StructuredOutput::new(
        key,
        json!({
            "type":"object", "properties":{(key):{"type":"string"}},
            "required":[key], "additionalProperties":false,
        }),
    )
    .unwrap();
    let model = ChatModel::from_adapter(OfflineModel {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("consumer").unwrap(),
            ModelCapabilities::new()
                .with_structured_output(true)
                .with_streaming(true)
                .with_tool_calling(true),
        ),
        first,
        counts,
    })
    .unwrap();
    AgentStage::new(
        AgentStageId::new(if first { "a" } else { "b" }).unwrap(),
        model,
        ToolRuntime::new(registry.build()),
        AgentConfig::new(2).unwrap().with_tool_approval(!first),
        output,
    )
    .unwrap()
}
fn sequence(counts: Arc<Counts>) -> AgentSequence {
    let a = stage(true, counts.clone());
    let b = stage(false, counts.clone());
    AgentSequence::new(
        "consumer-v1",
        a,
        b,
        Arc::new(move |output: &ValidatedJsonOutput| {
            counts.mapper.fetch_add(1, SeqCst);
            let task: Task = output.deserialize().map_err(HandoffError::with_source)?;
            Ok(vec![Message::user(task.instruction)])
        }),
    )
    .unwrap()
}
#[tokio::main]
async fn main() {
    let counts = Arc::new(Counts::default());
    let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
    let first = sequence(counts.clone());
    let mut events = first.stream_with_checkpoint(
        vec![Message::user("original-task")],
        CheckpointConfig::new("consumer", store.clone(), CheckpointPolicy::EverySuperstep),
    );
    let mut saved = None;
    let mut handoffs = 0;
    while let Some(event) = events.next().await {
        match event.unwrap() {
            SequenceStreamEvent::Interrupted(value) => {
                assert!(saved.is_none());
                saved = Some(value);
            }
            SequenceStreamEvent::Handoff { from, to } => {
                assert_eq!(from.as_str(), "a");
                assert_eq!(to.as_str(), "b");
                handoffs += 1;
            }
            SequenceStreamEvent::Completed(_) => panic!("approval must suspend"),
            _ => {}
        }
    }
    let saved = saved.expect("saved approval");
    assert_eq!(handoffs, 1);
    assert_eq!(counts.a.load(SeqCst), 1);
    assert_eq!(counts.b.load(SeqCst), 1);
    assert_eq!(counts.mapper.load(SeqCst), 1);
    assert_eq!(counts.tool.load(SeqCst), 0);
    let approval = saved.approval_request().unwrap();
    assert_eq!(approval.stage().as_str(), "b");
    drop(events);
    drop(first);

    let fresh_counts = Arc::new(Counts::default());
    let fresh = sequence(fresh_counts.clone());
    let mut events = fresh.resume_stream(
        ResumeConfig::new("consumer", store)
            .with_checkpoint_id(saved.checkpoint_id())
            .with_resume_value(SequenceApprovalDecision::new(
                approval.stage().clone(),
                AgentApprovalDecision::Approve,
            )),
    );
    let mut completions = 0;
    let mut resumed_stage_events = 0;
    while let Some(event) = events.next().await {
        match event.unwrap() {
            SequenceStreamEvent::Stage { stage, .. } => {
                assert_eq!(stage.as_str(), "b");
                resumed_stage_events += 1;
            }
            SequenceStreamEvent::Completed(outcome) => {
                let answer: Answer = outcome.final_output().unwrap().deserialize().unwrap();
                assert_eq!(answer.answer, "done");
                completions += 1;
            }
            SequenceStreamEvent::Handoff { .. } | SequenceStreamEvent::Interrupted(_) => {
                panic!("unexpected resumed work")
            }
            _ => {}
        }
    }
    assert_eq!(completions, 1);
    assert!(resumed_stage_events > 0);
    assert_eq!(fresh_counts.a.load(SeqCst), 0);
    assert_eq!(fresh_counts.mapper.load(SeqCst), 0);
    assert_eq!(fresh_counts.b.load(SeqCst), 1);
    assert_eq!(fresh_counts.tool.load(SeqCst), 1);
    println!(
        "PASS: typed handoff, saved approval, fresh-object streaming recovery; A=0 mapper=0 B=1 tool=1 after restore"
    );
}
