//! Offline fixed-order composition with approval and fresh-sequence recovery.
#[path = "../test_support/sequence_agent.rs"]
mod support;
use group_agent_core::{CheckpointConfig, CheckpointPolicy, InMemoryCheckpointer, ResumeConfig};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentApprovalDecision, SequenceApprovalDecision, SequenceSnapshotCodec,
};
use std::sync::Arc;
#[derive(serde::Deserialize)]
struct Answer {
    answer: String,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
    let (sequence, _) = support::sequence([false, true], [2, 2]);
    let interrupted = sequence
        .invoke_with_checkpoint(
            vec![Message::user("Prepare a task for the second Agent.")],
            CheckpointConfig::new("demo", store.clone(), CheckpointPolicy::EverySuperstep),
        )
        .await?;
    let saved = interrupted
        .as_interrupted()
        .expect("second Agent asks for approval");
    let stage = saved.approval_request().unwrap().stage().clone();
    println!("Saved approval for stage {}", stage.as_str());
    // In an application, obtain this decision through its authorized user workflow.
    let (fresh, _) = support::sequence([false, true], [2, 2]);
    let result = fresh
        .resume(
            ResumeConfig::new("demo", store)
                .with_checkpoint_id(saved.checkpoint_id())
                .with_resume_value(SequenceApprovalDecision::new(
                    stage,
                    AgentApprovalDecision::Approve,
                )),
        )
        .await?;
    let completed = result.as_completed().expect("sequence completed");
    let answer: Answer = completed.final_output().unwrap().deserialize()?;
    println!("Final typed result: {}", answer.answer);
    Ok(())
}
