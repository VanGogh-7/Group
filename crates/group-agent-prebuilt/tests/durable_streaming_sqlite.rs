#[path = "../test_support/streaming_agent.rs"]
mod fixture;

use fixture::{Events, agent};
use futures_util::StreamExt;
use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointStore, Checkpointer, RecordCheckpointer,
    ResumeConfig, RunId, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentSnapshot, AgentSnapshotCodec, AgentStreamEvent,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;

async fn connect(url: &str) -> Arc<RecordCheckpointer<AgentSnapshot>> {
    let store = Arc::new(SqliteCheckpointStore::connect(url).await.unwrap());
    store.migrate().await.unwrap();
    let store: Arc<dyn CheckpointStore> = store;
    Arc::new(RecordCheckpointer::new(store, Arc::new(AgentSnapshotCodec)))
}

#[tokio::test]
async fn reopened_sqlite_and_new_agent_resume_streamed_approval() {
    for use_sink in [false, true] {
        for decision in [
            AgentApprovalDecision::Approve,
            AgentApprovalDecision::Reject,
        ] {
            let directory = std::env::temp_dir().join(format!("group-streaming-{}", RunId::new()));
            std::fs::create_dir_all(&directory).unwrap();
            let url = format!(
                "sqlite://{}",
                directory.join("checkpoint.sqlite3").display()
            );
            let id = {
                let (agent, probe) = agent(true, 2);
                let store = connect(&url).await;
                let mut stream = agent.stream_with_checkpoint(
                    vec![Message::user("SECRET_PROMPT")],
                    CheckpointConfig::new("restart", store.clone(), CheckpointPolicy::FinalOnly),
                );
                let mut saved = None;
                while let Some(event) = stream.next().await {
                    if let AgentStreamEvent::Interrupted(interrupted) = event.unwrap() {
                        let checkpoint = store
                            .get(interrupted.thread_id(), interrupted.checkpoint_id())
                            .await
                            .unwrap()
                            .unwrap();
                        assert!(
                            checkpoint.interrupted(),
                            "event follows persisted interrupt even with FinalOnly"
                        );
                        saved = Some(interrupted.checkpoint_id());
                    }
                }
                assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
                saved.unwrap()
            };
            {
                let (agent, probe) = agent(true, 2);
                let store = connect(&url).await;
                let cfg = ResumeConfig::new("restart", store.clone())
                    .with_checkpoint_id(id)
                    .with_resume_value(decision);
                let events = if use_sink {
                    let sink = Arc::new(Events::default());
                    agent
                        .resume_with_stream_sink(cfg, sink.clone())
                        .await
                        .unwrap();
                    sink.snapshot()
                } else {
                    agent
                        .resume_stream(cfg)
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .map(Result::unwrap)
                        .collect()
                };
                let AgentStreamEvent::Completed(outcome) = events.last().unwrap() else {
                    panic!("completion required")
                };
                assert_eq!(outcome.model_rounds(), 2);
                assert_eq!(outcome.usage_by_round().len(), 2);
                assert_eq!(
                    outcome.final_message().unwrap().text_content(),
                    "SECRET_FINAL"
                );
                assert_eq!(
                    probe.streams.load(Ordering::SeqCst),
                    1,
                    "only new model work streams"
                );
                assert_eq!(
                    probe.executions.load(Ordering::SeqCst),
                    usize::from(decision == AgentApprovalDecision::Approve)
                );
                assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
                let transcript = probe.transcripts.lock().unwrap().clone();
                assert_eq!(
                    transcript[0].len(),
                    3,
                    "restored prompt and assistant plus new ToolMessage"
                );
                assert_eq!(
                    transcript[0][2].as_tool().unwrap().result().is_error(),
                    decision == AgentApprovalDecision::Reject
                );
                assert!(
                    store
                        .latest(&ThreadId::from("restart"))
                        .await
                        .unwrap()
                        .unwrap()
                        .completed()
                );
            }
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
