use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy, CheckpointState,
    Checkpointer, CodecDescriptor, END, EncodedValue, EventConfig, ExecutionOutcome, GraphState,
    InMemoryCheckpointer, InterruptPayload, InterruptibleNode, NodeContext, NodeError, NodeOutcome,
    ResumeConfig, RunConfig, RunControl, SnapshotError, StateError, StateGraph,
};

#[derive(Debug)]
struct ApprovalState {
    approved_by: Option<String>,
}

#[derive(Debug)]
struct ApprovalSnapshot {
    approved_by: Option<String>,
}

impl GraphState for ApprovalState {
    type Update = String;

    fn apply(&mut self, approved_by: Self::Update) -> Result<(), StateError> {
        self.approved_by = Some(approved_by);
        Ok(())
    }
}

impl CheckpointState for ApprovalState {
    type Snapshot = ApprovalSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(ApprovalSnapshot {
            approved_by: self.approved_by.clone(),
        })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            approved_by: snapshot.approved_by.clone(),
        })
    }
}

#[derive(Debug)]
struct ApprovalPrompt {
    summary: &'static str,
}

struct ApprovalCodec;

impl CheckpointCodec<ApprovalSnapshot> for ApprovalCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new(
            "group.example.approval.snapshot",
            1,
            "group.example.approval.raw-v1",
        )
    }

    fn encode_snapshot(
        &self,
        snapshot: &ApprovalSnapshot,
    ) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot
            .approved_by
            .as_deref()
            .unwrap_or_default()
            .as_bytes()
            .to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<ApprovalSnapshot, CheckpointCodecError> {
        let value = std::str::from_utf8(bytes)
            .map_err(|source| CheckpointCodecError::with_source("invalid approver", source))?;
        Ok(ApprovalSnapshot {
            approved_by: (!value.is_empty()).then(|| value.to_owned()),
        })
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let prompt = payload
            .downcast_ref::<ApprovalPrompt>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        Ok(EncodedValue::new(
            CodecDescriptor::new(
                "group.example.approval.prompt",
                1,
                "group.example.approval.raw-v1",
            ),
            prompt.summary.as_bytes(),
        ))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        if value.descriptor()
            != &CodecDescriptor::new(
                "group.example.approval.prompt",
                1,
                "group.example.approval.raw-v1",
            )
            || value.bytes() != b"Approve publishing the draft?"
        {
            return Err(CheckpointCodecError::message("unsupported approval prompt"));
        }
        Ok(InterruptPayload::new(ApprovalPrompt {
            summary: "Approve publishing the draft?",
        }))
    }
}

struct RequireApproval;

#[async_trait]
impl InterruptibleNode<ApprovalState> for RequireApproval {
    async fn run(
        &self,
        _state: &ApprovalState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<String>, NodeError> {
        if context.has_resume_value() {
            let approved_by = context
                .require_resume_value::<String>()
                .map_err(|source| NodeError::with_source("invalid approval value", source))?;
            return Ok(NodeOutcome::update(approved_by.clone()));
        }
        Ok(NodeOutcome::interrupt(ApprovalPrompt {
            summary: "Approve publishing the draft?",
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.set_version("approval-v1");
    graph.add_interruptible_node("approval", RequireApproval)?;
    graph
        .add_edge(group_agent_core::START, "approval")
        .add_edge("approval", END);
    let graph = graph.compile()?;

    let store = Arc::new(InMemoryCheckpointer::new(ApprovalCodec));
    let outcome = graph
        .invoke_with_checkpoint(
            ApprovalState { approved_by: None },
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "draft-42",
                Arc::clone(&store) as Arc<dyn Checkpointer<ApprovalSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await?;
    let interrupted = match outcome {
        ExecutionOutcome::Interrupted(report) => report,
        ExecutionOutcome::Completed(_) => return Err("graph unexpectedly completed".into()),
        _ => return Err("unknown execution outcome".into()),
    };
    let prompt = interrupted
        .interrupt()
        .payload()
        .downcast_ref::<ApprovalPrompt>()
        .expect("approval payload type should match");
    println!(
        "interrupt {}: {}",
        interrupted.interrupt().id(),
        prompt.summary
    );

    let outcome = graph
        .resume(
            ResumeConfig::new("draft-42", store as Arc<dyn Checkpointer<ApprovalSnapshot>>)
                .with_resume_value(String::from("human-reviewer")),
        )
        .await?;
    let completed = outcome
        .as_completed()
        .expect("resume value should complete the graph");
    println!(
        "approved by: {}",
        completed
            .final_state()
            .approved_by
            .as_deref()
            .expect("approval should be committed")
    );
    Ok(())
}
