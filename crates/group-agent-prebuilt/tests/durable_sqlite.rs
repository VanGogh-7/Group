#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use std::path::PathBuf;
use std::sync::Arc;

use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointStore, RecordCheckpointer, ResumeConfig, RunId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentConfig, AgentSnapshot, AgentSnapshotCodec, AgentStopReason, ToolCallingAgent,
};
use offline_agent::{Script, ScriptedModel, local_runtime};

fn database() -> (PathBuf, String) {
    let directory = std::env::temp_dir().join(format!("group-prebuilt-durable-{}", RunId::new()));
    std::fs::create_dir_all(&directory).expect("temporary database directory");
    let url = format!("sqlite://{}", directory.join("runtime.sqlite3").display());
    (directory, url)
}

async fn store_at(database_url: &str) -> Arc<RecordCheckpointer<AgentSnapshot>> {
    let store = Arc::new(
        SqliteCheckpointStore::connect(database_url)
            .await
            .expect("SQLite should connect"),
    );
    store.migrate().await.expect("migration should succeed");
    let store: Arc<dyn CheckpointStore> = store;
    Arc::new(RecordCheckpointer::new(store, Arc::new(AgentSnapshotCodec)))
}

#[tokio::test]
async fn sqlite_restart_resumes_from_the_committed_head() {
    let (directory, database_url) = database();
    let (model, transcripts) =
        ScriptedModel::fail_once(Script::OneToolRound, 2).expect("fail-once model is valid");
    let agent = ToolCallingAgent::new(
        model,
        local_runtime().expect("offline local ToolRuntime is valid"),
        AgentConfig::new(2).expect("two model rounds are valid"),
    )
    .expect("offline agent construction succeeds");

    let first_store = store_at(&database_url).await;
    agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                "sqlite-restart-thread",
                first_store,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("the scripted model fails once on its second call");
    // Dropping the store simulates a process restart; only the database file survives.

    let restarted_store = store_at(&database_url).await;
    let outcome = agent
        .resume(ResumeConfig::new("sqlite-restart-thread", restarted_store))
        .await
        .expect("resume completes from the persisted head after a restart");

    assert!(outcome.is_completed());
    assert_eq!(outcome.stop_reason(), Some(AgentStopReason::FinalAnswer));
    let completed = outcome.as_completed().expect("completed outcome");
    assert_eq!(completed.model_rounds(), 2);
    assert_eq!(
        completed
            .final_message()
            .expect("FinalAnswer includes a final assistant message")
            .text_content(),
        "Offline tool-assisted answer."
    );
    assert_eq!(
        transcripts.all().len(),
        3,
        "original two calls plus resumed call"
    );

    std::fs::remove_dir_all(&directory).expect("temporary database cleanup");
}
