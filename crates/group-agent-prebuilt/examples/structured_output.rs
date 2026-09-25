//! Offline typed output across ordinary, streaming and durable approval calls.
#[path = "../test_support/structured_agent.rs"]
mod support;
use futures_util::StreamExt;
use group_agent_core::ResumeConfig;
use group_agent_model::Message;
use group_agent_prebuilt::{AgentApprovalDecision, AgentOutcome, AgentStreamEvent};

#[derive(serde::Deserialize)]
struct Answer {
    answer: String,
}
fn display(outcome: &AgentOutcome) -> Result<(), Box<dyn std::error::Error>> {
    let value = outcome
        .structured_output()
        .expect("final structured answer");
    let answer: Answer = value.deserialize()?;
    println!("Typed answer: {}", answer.answer);
    // A Rust-type mismatch is local extraction; it cannot repeat model or tools.
    assert!(value.deserialize::<Vec<String>>().is_err());
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let contract = support::output("answer");
    let (agent, _) = support::agent(false, 2, Some(contract.clone()), false);
    display(&agent.invoke(vec![Message::user("question")]).await?)?;
    let mut stream = agent.stream(vec![Message::user("question")]);
    while let Some(event) = stream.next().await {
        if let AgentStreamEvent::Completed(outcome) = event? {
            display(&outcome)?;
        }
    }
    let store = support::fixture::store();
    let (approval, _) = support::agent(true, 2, Some(contract.clone()), false);
    let interrupted = approval
        .invoke_with_checkpoint(
            vec![Message::user("question")],
            support::fixture::config("typed", &store),
        )
        .await?;
    assert!(interrupted.as_interrupted().is_some());
    let (fresh, _) = support::agent(true, 2, Some(contract), false);
    let resumed = fresh
        .resume(ResumeConfig::new("typed", store).with_resume_value(AgentApprovalDecision::Approve))
        .await?;
    display(resumed.as_completed().expect("approved completion"))?;
    Ok(())
}
