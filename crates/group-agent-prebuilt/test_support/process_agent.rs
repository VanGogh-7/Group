use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{Checkpointer, ThreadId};
use group_agent_model::{
    ChatEventStream, ChatModel, ChatModelAdapter, ChatResponse, ModelCapabilities, ModelError,
    ModelId, ModelMetadata, ProviderId, ToolDefinition, ToolName, ValidatedChatRequest,
};
use group_agent_prebuilt::{AgentConfig, AgentSnapshot, ToolCallingAgent};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};

use crate::fixture::{Probe, Scripted};

struct PausingModel {
    inner: Scripted,
    directory: PathBuf,
    pause: bool,
    store: Arc<dyn Checkpointer<AgentSnapshot>>,
}

#[async_trait]
impl ChatModelAdapter for PausingModel {
    fn metadata(&self) -> &ModelMetadata {
        self.inner.metadata()
    }

    async fn complete_raw(&self, _: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
        panic!("process recovery must use streaming");
    }

    async fn stream_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        if self.pause
            && request
                .messages()
                .iter()
                .any(|message| message.as_tool().is_some())
        {
            let head = self
                .store
                .latest(&ThreadId::from(crate::THREAD))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                head.superstep(),
                2,
                "model and tool commits must be persisted"
            );
            assert!(!head.completed());
            assert!(!head.interrupted());
            fs::write(self.directory.join("ready"), b"tool result persisted").unwrap();
            std::future::pending::<()>().await;
        }
        self.inner.stream_raw(request).await
    }
}

struct JournalTool {
    definition: ToolDefinition,
    directory: PathBuf,
    probe: Arc<Probe>,
}

#[async_trait]
impl Tool for JournalTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }
    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }
    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::non_idempotent_write()
    }
    async fn execute(&self, _: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        // A real external effect survives both worker processes; it is deliberately
        // not part of the SQLite transaction and is not an exactly-once mechanism.
        let mut journal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("tool-executions"))
            .unwrap();
        writeln!(journal, "executed").unwrap();
        journal.sync_all().unwrap();
        self.probe
            .executions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(ToolOutput::success_text("SECRET_RESULT"))
    }
}

pub fn agent(
    directory: PathBuf,
    approval: bool,
    pause: bool,
    store: Arc<dyn Checkpointer<AgentSnapshot>>,
) -> (ToolCallingAgent, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let model = ChatModel::from_adapter(PausingModel {
        inner: Scripted {
            metadata: ModelMetadata::new(
                ProviderId::new("offline").unwrap(),
                ModelId::new("process-recovery").unwrap(),
                ModelCapabilities::new()
                    .with_streaming(true)
                    .with_tool_calling(true),
            ),
            probe: probe.clone(),
        },
        directory: directory.clone(),
        pause,
        store,
    })
    .unwrap();
    let mut registry = ToolRegistry::builder();
    registry
        .register(JournalTool {
            definition: ToolDefinition::new(
                ToolName::new("lookup").unwrap(),
                "Offline journal",
                serde_json::json!({"type":"object"}),
            ),
            directory,
            probe: probe.clone(),
        })
        .unwrap();
    let agent = ToolCallingAgent::new(
        model,
        ToolRuntime::new(registry.build()),
        AgentConfig::new(2).unwrap().with_tool_approval(approval),
    )
    .unwrap();
    (agent, probe)
}
