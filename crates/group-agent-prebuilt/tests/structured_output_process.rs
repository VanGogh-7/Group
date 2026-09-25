#![cfg(feature = "structured-output")]
#[path = "../test_support/structured_agent.rs"]
mod support;
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
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
const THREAD: &str = "structured-process";
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
                .args(["--exact", "structured_worker", "--nocapture"])
                .env("GROUP_STRUCTURED_DIR", directory)
                .env("GROUP_STRUCTURED_PHASE", phase)
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
async fn store(directory: &Path) -> Arc<RecordCheckpointer<AgentSnapshot>> {
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
    Arc::new(RecordCheckpointer::new(store, Arc::new(AgentSnapshotCodec)))
}
#[tokio::test]
async fn structured_worker() {
    let Some(directory) = std::env::var_os("GROUP_STRUCTURED_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let phase = std::env::var("GROUP_STRUCTURED_PHASE").unwrap();
    let plain = phase == "plain";
    let mismatch = matches!(phase.as_str(), "changed" | "plain" | "changed-schema");
    let no_approval = matches!(
        phase.as_str(),
        "save-tool" | "resume-tool" | "complete" | "resume-complete"
    );
    let contract = if plain {
        None
    } else {
        let base = support::output(if phase == "changed" {
            "other"
        } else {
            "answer"
        });
        if phase == "changed-schema" {
            let mut schema = base.schema().clone();
            schema["description"] = serde_json::json!("different");
            Some(group_agent_model::StructuredOutput::new("answer", schema).unwrap())
        } else {
            Some(base)
        }
    };
    let (agent, probe) = support::build(
        !no_approval,
        2,
        contract,
        false,
        Some((directory.clone(), phase == "save-tool")),
    );
    let store = store(&directory).await;
    if matches!(phase.as_str(), "interrupt" | "save-tool" | "complete") {
        let mut stream = agent.stream_with_checkpoint(
            vec![Message::user("question")],
            CheckpointConfig::new(THREAD, store.clone(), CheckpointPolicy::EverySuperstep),
        );
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                AgentStreamEvent::Interrupted(_) => {
                    assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
                    fs::write(directory.join("ready"), b"interrupt").unwrap();
                    std::future::pending::<()>().await;
                }
                AgentStreamEvent::Completed(_) => {
                    assert_eq!(phase, "complete");
                    fs::write(directory.join("ready"), b"complete").unwrap();
                    std::future::pending::<()>().await;
                }
                _ => {}
            }
        }
        panic!("missing pause");
    }
    let head = store
        .latest(&ThreadId::from(THREAD))
        .await
        .unwrap()
        .unwrap();
    if phase == "resume-tool" {
        assert_eq!(head.superstep(), 2);
        assert!(!head.completed());
        assert!(!head.interrupted());
    }
    let config = ResumeConfig::new(THREAD, store.clone());
    let config = if no_approval {
        config
    } else {
        config.with_resume_value(if phase == "reject" {
            AgentApprovalDecision::Reject
        } else {
            AgentApprovalDecision::Approve
        })
    };
    let result = agent.resume(config).await;
    if mismatch {
        assert!(result.is_err());
        assert_eq!(
            probe.completions.load(Ordering::SeqCst)
                + probe.streams.load(Ordering::SeqCst)
                + probe.executions.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            store
                .latest(&ThreadId::from(THREAD))
                .await
                .unwrap()
                .unwrap()
                .id(),
            head.id()
        );
        return;
    }
    let result = result.unwrap();
    assert_eq!(
        result
            .as_completed()
            .unwrap()
            .structured_output()
            .unwrap()
            .value(),
        &serde_json::json!({"answer":"ok"})
    );
    assert_eq!(
        probe.executions.load(Ordering::SeqCst),
        usize::from(phase == "approve")
    );
    assert_eq!(
        probe.completions.load(Ordering::SeqCst),
        usize::from(phase != "resume-complete")
    );
    assert!(
        store
            .latest(&ThreadId::from(THREAD))
            .await
            .unwrap()
            .unwrap()
            .completed()
    );
}
#[test]
fn killed_process_restores_only_matching_output_contract() {
    for (first, recover, expected) in [
        ("interrupt", "approve", 1),
        ("interrupt", "reject", 0),
        ("interrupt", "changed", 0),
        ("interrupt", "changed-schema", 0),
        ("interrupt", "plain", 0),
        ("save-tool", "resume-tool", 1),
        ("complete", "resume-complete", 1),
    ] {
        let directory =
            Directory(std::env::temp_dir().join(format!("group-structured-{}", RunId::new())));
        fs::create_dir(&directory.0).unwrap();
        let mut child = Worker::new(&directory.0, first);
        child.ready(&directory.0);
        child.kill();
        let mut resumed = Worker::new(&directory.0, recover);
        resumed.finish();
        let journal = match fs::read_to_string(directory.0.join("executions")) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => panic!("cannot read execution journal: {error}"),
        };
        assert_eq!(journal.lines().count(), expected, "{first}/{recover}");
    }
}
