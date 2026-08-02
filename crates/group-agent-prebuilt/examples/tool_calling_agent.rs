#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use group_agent_model::Message;
use group_agent_prebuilt::{AgentConfig, AgentStopReason, ToolCallingAgent};
use offline_agent::{ScriptedModel, local_runtime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let agent = ToolCallingAgent::new(
        ScriptedModel::one_tool_round()?,
        local_runtime()?,
        AgentConfig::new(2)?,
    )?;

    // The scripted adapter issues one ToolCall. The local ToolRuntime executes
    // it, and the adapter verifies that the next facade-validated request
    // contains the paired ToolMessage before returning the final answer.
    let outcome = agent
        .invoke(vec![Message::user("Use the offline label tool.")])
        .await?;
    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(outcome.model_rounds(), 2);

    let answer = outcome
        .final_message()
        .expect("FinalAnswer has a final assistant message")
        .text_content();
    println!("stop_reason: {:?}", outcome.stop_reason());
    println!("model_rounds: {}", outcome.model_rounds());
    println!("final_answer: {answer}");
    Ok(())
}
