//! Offline streaming approval followed by an explicit durable Resume.

#[path = "../test_support/streaming_agent.rs"]
mod fixture;

use futures_util::StreamExt;
use group_agent_core::ResumeConfig;
use group_agent_model::Message;
use group_agent_prebuilt::{AgentApprovalDecision, AgentStreamEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (agent, _) = fixture::agent(true, 2);
    let store = fixture::store();
    let mut stream = agent.stream_with_checkpoint(
        vec![Message::user("Use the offline lookup tool.")],
        fixture::config("streaming-approval", &store),
    );
    let mut interrupted = None;
    while let Some(event) = stream.next().await {
        match event? {
            AgentStreamEvent::ApprovalRequired { .. } => {
                println!("Approval requested; waiting for checkpoint confirmation.");
            }
            AgentStreamEvent::Interrupted(saved) => {
                println!("Approval checkpoint saved: {:?}", saved.checkpoint_id());
                interrupted = Some(saved);
            }
            event => println!("{event:?}"),
        }
    }
    let saved = interrupted.expect("offline script must durably suspend");
    // An application obtains this decision from its authorized human workflow.
    // This offline demonstration deliberately approves its harmless local tool.
    let mut resumed = agent.resume_stream(
        ResumeConfig::new(saved.thread_id().clone(), store)
            .with_checkpoint_id(saved.checkpoint_id())
            .with_resume_value(AgentApprovalDecision::Approve),
    );
    while let Some(event) = resumed.next().await {
        println!("{:?}", event?);
    }
    Ok(())
}
