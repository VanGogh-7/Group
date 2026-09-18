#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use std::sync::Arc;

use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, Checkpointer, InMemoryCheckpointer, ResumeConfig, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentConfig, AgentSnapshot, AgentSnapshotCodec, ToolCallingAgent,
};
use offline_agent::{ScriptedModel, local_runtime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let checkpointer = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
    let thread_id = ThreadId::from("approval-example");

    // The approval-enabled agent durably suspends before any Tool side
    // effect and records the pending ToolCalls as an interrupt payload.
    let agent = ToolCallingAgent::new(
        ScriptedModel::one_tool_round()?,
        local_runtime()?,
        AgentConfig::new(2)?.with_tool_approval(true),
    )?;

    let outcome = agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                checkpointer.clone() as Arc<dyn Checkpointer<AgentSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await?;
    let interrupted = outcome
        .as_interrupted()
        .expect("the approval-enabled agent suspends before Tool execution");
    let request = interrupted
        .approval_request()
        .expect("the suspension carries the pending approval request");
    println!(
        "suspended for approval: {} pending ToolCall(s)",
        request.pending_calls().len()
    );
    for call in request.pending_calls() {
        println!(
            "pending call: id={} tool={}",
            call.id().as_str(),
            call.name().as_str()
        );
    }

    // A human approves the batch; the decision resumes the durable run.
    let outcome = agent
        .resume(
            ResumeConfig::new(
                thread_id,
                checkpointer as Arc<dyn Checkpointer<AgentSnapshot>>,
            )
            .with_resume_value(AgentApprovalDecision::Approve),
        )
        .await?;
    let completed = outcome.as_completed().expect("the approved run completes");
    let answer = completed
        .final_message()
        .expect("FinalAnswer has a final assistant message")
        .text_content();

    println!("resumed stop_reason: {:?}", outcome.stop_reason());
    println!("resumed model_rounds: {}", completed.model_rounds());
    println!("final_answer: {answer}");
    Ok(())
}
