#![cfg(feature = "agent-sequence")]
#[path = "../test_support/sequence_agent.rs"]
mod support;
use futures_util::StreamExt;
use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointStore, Checkpointer, RecordCheckpointer,
    ResumeConfig, RunId, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentStageId, SequenceApprovalDecision, SequenceSnapshot,
    SequenceSnapshotCodec, SequenceStreamEvent,
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
const THREAD: &str = "sequence-process";
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Worker {
    fn new(directory: &Path, phase: &str) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "sequence_worker", "--nocapture"])
                .env("GROUP_SEQUENCE_DIR", directory)
                .env("GROUP_SEQUENCE_PHASE", phase)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
    fn ready(&mut self, directory: &Path) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "worker exited before kill"
            );
            if directory.join("ready").exists() {
                return;
            }
            assert!(Instant::now() < deadline, "readiness timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn kill(&mut self) {
        self.0.kill().unwrap();
        let status = self.0.wait().unwrap();
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
    }
    fn finish(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "worker failure: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "recovery timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
async fn store(directory: &Path) -> Arc<RecordCheckpointer<SequenceSnapshot>> {
    let store = Arc::new(
        SqliteCheckpointStore::connect(&format!(
            "sqlite://{}",
            directory.join("state.sqlite3").display()
        ))
        .await
        .unwrap(),
    );
    store.migrate().await.unwrap();
    let store: Arc<dyn CheckpointStore> = store;
    Arc::new(RecordCheckpointer::new(
        store,
        Arc::new(SequenceSnapshotCodec),
    ))
}
#[tokio::test]
async fn sequence_worker() {
    let Some(dir) = std::env::var_os("GROUP_SEQUENCE_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let phase = std::env::var("GROUP_SEQUENCE_PHASE").unwrap();
    let (mode, boundary) = phase.split_once(':').unwrap();
    let approval = boundary == "approval";
    let mismatch = mode == "mismatch";
    let reject = mode == "reject";
    let seq = support::process_sequence(
        &dir,
        if mode == "start" { boundary } else { "" },
        approval,
        if mismatch { "v2" } else { "v1" },
    );
    let store = store(&dir).await;
    if mode == "start" {
        let events = seq
            .stream_with_checkpoint(
                vec![Message::user("q")],
                CheckpointConfig::new(THREAD, store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .collect::<Vec<_>>()
            .await;
        if approval {
            assert!(matches!(
                events.last(),
                Some(Ok(SequenceStreamEvent::Interrupted(_)))
            ));
        } else {
            assert!(matches!(
                events.last(),
                Some(Ok(SequenceStreamEvent::Completed(_)))
            ));
        }
        fs::write(dir.join("ready"), b"ready").unwrap();
        std::future::pending::<()>().await;
    } else {
        let head = store
            .latest(&ThreadId::from(THREAD))
            .await
            .unwrap()
            .unwrap();
        let mut config = ResumeConfig::new(THREAD, store.clone()).with_checkpoint_id(head.id());
        if approval {
            config = config.with_resume_value(SequenceApprovalDecision::new(
                AgentStageId::new("b").unwrap(),
                if reject {
                    AgentApprovalDecision::Reject
                } else {
                    AgentApprovalDecision::Approve
                },
            ));
        }
        let events = seq.resume_stream(config).collect::<Vec<_>>().await;
        if mismatch {
            assert_eq!(events.len(), 1);
            assert!(events[0].is_err());
            assert_eq!(
                store
                    .latest(&ThreadId::from(THREAD))
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                head.id()
            );
        } else {
            assert!(
                matches!(events.last(),Some(Ok(SequenceStreamEvent::Completed(o))) if o.final_output().is_some())
            );
        }
    }
}
#[test]
fn process_kill_recovers_each_saved_sequence_boundary() {
    for (boundary, recovery) in [
        ("first", "resume"),
        ("handoff", "resume"),
        ("approval", "resume"),
        ("approval", "reject"),
        ("tool", "resume"),
        ("complete", "resume"),
        ("approval", "mismatch"),
    ] {
        let dir = Directory(std::env::temp_dir().join(format!("group-sequence-{}", RunId::new())));
        fs::create_dir_all(&dir.0).unwrap();
        let mut worker = Worker::new(&dir.0, &format!("start:{boundary}"));
        worker.ready(&dir.0);
        worker.kill();
        let before = fs::read_to_string(dir.0.join("executions")).unwrap();
        assert_eq!(before.lines().filter(|l| *l == "tool-a").count(), 1);
        let mut worker = Worker::new(&dir.0, &format!("{recovery}:{boundary}"));
        worker.finish();
        let after = fs::read_to_string(dir.0.join("executions")).unwrap();
        assert_eq!(after.lines().filter(|l| *l == "tool-a").count(), 1);
        assert_eq!(after.lines().filter(|l| *l == "model-a").count(), 2);
        if recovery == "mismatch" {
            assert_eq!(before, after);
        } else {
            assert_eq!(
                after.lines().filter(|l| *l == "tool-b").count(),
                usize::from(recovery != "reject")
            );
            assert_eq!(
                after.lines().filter(|l| *l == "mapper").count(),
                if boundary == "first" { 2 } else { 1 }
            );
        }
    }
}
