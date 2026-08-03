#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use group_agent_model::Message;
use group_agent_prebuilt::{AgentConfig, AgentStopReason, ToolCallingAgent};
use offline_agent::{ScriptedModel, local_runtime};

#[tokio::test]
async fn tool_round_agent_completes_offline_through_the_public_api() {
    let agent = ToolCallingAgent::new(
        ScriptedModel::one_tool_round().expect("offline scripted model is valid"),
        local_runtime().expect("offline local ToolRuntime is valid"),
        AgentConfig::new(2).expect("two model rounds are valid"),
    )
    .expect("offline agent construction succeeds");

    let outcome = agent
        .invoke(vec![Message::user("Use the offline label tool.")])
        .await
        .expect("offline tool-round invocation succeeds");

    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(outcome.model_rounds(), 2);
    assert_eq!(
        outcome
            .final_message()
            .expect("FinalAnswer includes a final assistant message")
            .text_content(),
        "Offline tool-assisted answer."
    );
}
