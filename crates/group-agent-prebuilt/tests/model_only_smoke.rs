#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use group_agent_model::Message;
use group_agent_prebuilt::{AgentConfig, AgentStopReason, ToolCallingAgent};
use offline_agent::{ScriptedModel, empty_runtime};

#[tokio::test]
async fn model_only_agent_completes_offline_through_the_public_api() {
    let agent = ToolCallingAgent::new(
        ScriptedModel::model_only().expect("offline scripted model is valid"),
        empty_runtime(),
        AgentConfig::new(1).expect("one model round is valid"),
    )
    .expect("offline agent construction succeeds");

    let outcome = agent
        .invoke(vec![Message::user("Answer using the offline model.")])
        .await
        .expect("offline model-only invocation succeeds");

    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(outcome.model_rounds(), 1);
    assert_eq!(
        outcome
            .final_message()
            .expect("FinalAnswer includes a final assistant message")
            .text_content(),
        "Offline model-only answer."
    );
}
