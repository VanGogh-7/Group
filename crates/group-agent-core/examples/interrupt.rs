use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointState, Checkpointer, END, EventConfig,
    ExecutionOutcome, GraphState, InMemoryCheckpointer, InterruptibleNode, NodeContext, NodeError,
    NodeOutcome, ResumeConfig, RunConfig, RunControl, SnapshotError, StateError, StateGraph,
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

struct RequireApproval;

#[async_trait]
impl InterruptibleNode<ApprovalState> for RequireApproval {
    async fn run(
        &self,
        _state: &ApprovalState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<String>, NodeError> {
        if let Some(approved_by) = context.resume_value::<String>() {
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

    let store = Arc::new(InMemoryCheckpointer::new());
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
