#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use std::sync::Arc;

use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, Checkpointer, InMemoryCheckpointer, ResumeConfig, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{AgentConfig, AgentSnapshot, AgentSnapshotCodec, ToolCallingAgent};
use offline_agent::{Script, ScriptedModel, local_runtime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let checkpointer = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
    let thread_id = ThreadId::from("durable-example");

    // The scripted model fails exactly once on its second call, after the
    // first model round and the Tool batch have committed durable checkpoints.
    let (model, transcripts) = ScriptedModel::fail_once(Script::OneToolRound, 2)?;
    let agent = ToolCallingAgent::new(model, local_runtime()?, AgentConfig::new(2)?)?;

    let error = agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                checkpointer.clone() as Arc<dyn Checkpointer<AgentSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("the scripted model fails once on its second call");
    println!("first run failed as scripted: {error}");
    println!(
        "committed checkpoints after failure: {}",
        checkpointer.history(&thread_id).await?.len()
    );

    let outcome = agent
        .resume(ResumeConfig::new(
            thread_id,
            checkpointer as Arc<dyn Checkpointer<AgentSnapshot>>,
        ))
        .await?;
    let completed = outcome.as_completed().expect("resume completes the run");
    let answer = completed
        .final_message()
        .expect("FinalAnswer has a final assistant message")
        .text_content();

    println!("resumed stop_reason: {:?}", outcome.stop_reason());
    println!("resumed model_rounds: {}", completed.model_rounds());
    println!("model calls observed: {}", transcripts.all().len());
    println!("final_answer: {answer}");
    Ok(())
}
