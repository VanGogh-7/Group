#![allow(dead_code)]
#[path = "streaming_agent.rs"]
pub mod fixture;
use async_trait::async_trait;
use futures_util::StreamExt;
use group_agent_model::*;
use group_agent_prebuilt::*;
use serde_json::json;
use std::sync::Arc;
pub fn output() -> StructuredOutput {
    StructuredOutput::new("answer",json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false})).unwrap()
}
struct JsonModel {
    inner: fixture::Scripted,
    id: String,
    process: Option<(std::path::PathBuf, String)>,
}
impl JsonModel {
    async fn before(&self, request: &ValidatedChatRequest) {
        if let Some((dir, pause)) = &self.process {
            let final_round = request.messages().iter().any(|m| m.as_tool().is_some());
            if self.id == "b"
                && ((pause == "handoff" && !final_round) || (pause == "tool" && final_round))
            {
                std::fs::write(dir.join("ready"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            journal(dir, &format!("model-{}", self.id));
        }
    }
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
        self.before(&request).await;
        let result = self.inner.complete_raw(request).await?;
        if result.message().tool_calls().is_empty() {
            Ok(ChatResponse::new(
                AssistantMessage::text(r#"{"answer":"ok"}"#),
                FinishReason::Stop,
            ))
        } else {
            Ok(result)
        }
    }
    async fn stream_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        self.before(&request).await;
        let stream = self.inner.stream_raw(request).await?;
        Ok(Box::pin(stream.map(|e| {
            e.map(|e| {
                if matches!(e, ChatStreamEvent::TextDelta(_)) {
                    ChatStreamEvent::TextDelta(r#"{"answer":"ok"}"#.into())
                } else {
                    e
                }
            })
        })))
    }
}
pub fn stage(id: &str, approval: bool, rounds: usize) -> (AgentStage, Arc<fixture::Probe>) {
    stage_process(id, approval, rounds, None)
}
pub fn stage_process(
    id: &str,
    approval: bool,
    rounds: usize,
    process: Option<(std::path::PathBuf, String)>,
) -> (AgentStage, Arc<fixture::Probe>) {
    stage_process_output(id, approval, rounds, process, output())
}
pub fn stage_with_output(
    id: &str,
    approval: bool,
    rounds: usize,
    contract: StructuredOutput,
) -> (AgentStage, Arc<fixture::Probe>) {
    stage_process_output(id, approval, rounds, None, contract)
}
fn stage_process_output(
    id: &str,
    approval: bool,
    rounds: usize,
    process: Option<(std::path::PathBuf, String)>,
    contract: StructuredOutput,
) -> (AgentStage, Arc<fixture::Probe>) {
    let probe = Arc::new(fixture::Probe::default());
    let model = ChatModel::from_adapter(JsonModel {
        inner: fixture::Scripted {
            metadata: ModelMetadata::new(
                ProviderId::new("offline").unwrap(),
                ModelId::new("json").unwrap(),
                ModelCapabilities::new()
                    .with_tool_calling(true)
                    .with_streaming(true)
                    .with_structured_output(true),
            ),
            probe: probe.clone(),
        },
        id: id.into(),
        process: process.clone(),
    })
    .unwrap();
    let runtime = if let Some((dir, _)) = process {
        let mut registry = group_agent_tool::ToolRegistry::builder();
        registry
            .register(JournalTool {
                inner: fixture::CountingTool {
                    definition: ToolDefinition::new(
                        ToolName::new("lookup").unwrap(),
                        "offline",
                        json!({"type":"object"}),
                    ),
                    probe: probe.clone(),
                },
                dir,
                id: id.into(),
            })
            .unwrap();
        group_agent_tool::ToolRuntime::new(registry.build())
    } else {
        fixture::runtime(&probe)
    };
    let stage = AgentStage::new(
        AgentStageId::new(id).unwrap(),
        model,
        runtime,
        AgentConfig::new(rounds)
            .unwrap()
            .with_tool_approval(approval),
        contract,
    )
    .unwrap();
    (stage, probe)
}
pub fn sequence(
    approval: [bool; 2],
    rounds: [usize; 2],
) -> (AgentSequence, [Arc<fixture::Probe>; 2]) {
    let (a, pa) = stage("a", approval[0], rounds[0]);
    let (b, pb) = stage("b", approval[1], rounds[1]);
    let mapper = Arc::new(
        |_: &ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> {
            Ok(vec![Message::user("mapped")])
        },
    );
    (AgentSequence::new("v1", a, b, mapper).unwrap(), [pa, pb])
}

pub fn journal(dir: &std::path::Path, item: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("executions"))
        .unwrap();
    writeln!(file, "{item}").unwrap();
    file.sync_all().unwrap();
}
struct JournalTool {
    inner: fixture::CountingTool,
    dir: std::path::PathBuf,
    id: String,
}
#[async_trait]
impl group_agent_tool::Tool for JournalTool {
    fn name(&self) -> &ToolName {
        group_agent_tool::Tool::name(&self.inner)
    }
    fn definition(&self) -> &ToolDefinition {
        group_agent_tool::Tool::definition(&self.inner)
    }
    fn behavior(&self) -> group_agent_tool::ToolBehavior {
        group_agent_tool::ToolBehavior::non_idempotent_write()
    }
    async fn execute(
        &self,
        input: group_agent_tool::ToolInput<'_>,
    ) -> Result<group_agent_tool::ToolOutput, group_agent_tool::ToolError> {
        journal(&self.dir, &format!("tool-{}", self.id));
        group_agent_tool::Tool::execute(&self.inner, input).await
    }
}
pub fn process_sequence(
    dir: &std::path::Path,
    pause: &str,
    approval: bool,
    revision: &str,
) -> AgentSequence {
    let (a, _) = stage_process("a", false, 2, Some((dir.into(), pause.into())));
    let (b, _) = stage_process("b", approval, 2, Some((dir.into(), pause.into())));
    let dir = dir.to_owned();
    let pause = pause.to_owned();
    let mapper = Arc::new(
        move |_: &ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> {
            journal(&dir, "mapper");
            if pause == "first" {
                std::fs::write(dir.join("ready"), b"ready").unwrap();
                loop {
                    std::thread::park();
                }
            }
            Ok(vec![Message::user("mapped")])
        },
    );
    AgentSequence::new(revision, a, b, mapper).unwrap()
}
