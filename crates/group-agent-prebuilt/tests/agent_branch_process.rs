#![cfg(feature = "agent-branch")]
#[path = "../test_support/branch_agent.rs"]
mod support;
use futures_util::StreamExt;
use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointStore, Checkpointer, RecordCheckpointer,
    ResumeConfig, RunId, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentStageId, BranchApprovalDecision, BranchSnapshot,
    BranchSnapshotCodec, BranchStreamEvent, BranchTarget,
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
const THREAD: &str = "branch-process";
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
                .args(["--exact", "branch_worker", "--nocapture"])
                .env("GROUP_BRANCH_DIR", directory)
                .env("GROUP_BRANCH_PHASE", phase)
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
async fn store(directory: &Path) -> Arc<RecordCheckpointer<BranchSnapshot>> {
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
        Arc::new(BranchSnapshotCodec),
    ))
}
#[tokio::test]
async fn branch_worker() {
    let Some(dir) = std::env::var_os("GROUP_BRANCH_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let phase = std::env::var("GROUP_BRANCH_PHASE").unwrap();
    let mut parts = phase.split(':');
    let mode = parts.next().unwrap();
    let boundary = parts.next().unwrap();
    let target = match parts.next().unwrap() {
        "b" => BranchTarget::B,
        "c" => BranchTarget::C,
        "direct" => BranchTarget::Complete,
        _ => unreachable!(),
    };
    let approval = boundary == "approval";
    let mismatch = mode == "mismatch";
    let reject = mode == "reject";
    let seq = support::process_branch(
        &dir,
        if mode == "start" { boundary } else { "" },
        approval,
        if mismatch { "v2" } else { "v1" },
        target,
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
                Some(Ok(BranchStreamEvent::Interrupted(_)))
            ));
        } else {
            assert!(matches!(
                events.last(),
                Some(Ok(BranchStreamEvent::Completed(_)))
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
            config = config.with_resume_value(BranchApprovalDecision::new(
                AgentStageId::new(if target == BranchTarget::C { "c" } else { "b" }).unwrap(),
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
            let Some(Ok(BranchStreamEvent::Completed(outcome))) = events.last() else {
                panic!("missing completion")
            };
            assert_eq!(outcome.selected(), Some(target));
            assert_eq!(
                outcome.final_output().unwrap().value(),
                &serde_json::json!({"answer":"ok"})
            );
            if let Some(downstream) = outcome.downstream() {
                let tool = downstream
                    .messages()
                    .iter()
                    .find_map(Message::as_tool)
                    .unwrap();
                assert_eq!(tool.tool_call_id().as_str(), "call-1");
                assert_eq!(
                    tool.result(),
                    &if reject {
                        group_agent_model::ToolResult::error_text("tool call rejected by approval")
                    } else {
                        group_agent_model::ToolResult::text("SECRET_RESULT")
                    }
                );
            }
            assert!(
                store
                    .latest(&ThreadId::from(THREAD))
                    .await
                    .unwrap()
                    .unwrap()
                    .completed()
            );
        }
    }
}
#[test]
fn process_kill_recovers_each_saved_branch_boundary() {
    for target in ["b", "c", "direct"] {
        for (boundary, recovery) in [
            ("first", "resume"),
            ("handoff", "resume"),
            ("approval", "resume"),
            ("approval", "reject"),
            ("tool", "resume"),
            ("complete", "resume"),
            ("approval", "mismatch"),
        ] {
            if target == "direct" && !["first", "complete"].contains(&boundary) {
                continue;
            }
            let dir =
                Directory(std::env::temp_dir().join(format!("group-branch-{}", RunId::new())));
            fs::create_dir_all(&dir.0).unwrap();
            let mut worker = Worker::new(&dir.0, &format!("start:{boundary}:{target}"));
            worker.ready(&dir.0);
            worker.kill();
            let before = fs::read_to_string(dir.0.join("executions")).unwrap();
            assert_eq!(before.lines().filter(|l| *l == "tool-a").count(), 1);
            let mut worker = Worker::new(&dir.0, &format!("{recovery}:{boundary}:{target}"));
            worker.finish();
            let after = fs::read_to_string(dir.0.join("executions")).unwrap();
            assert_eq!(after.lines().filter(|l| *l == "tool-a").count(), 1);
            assert_eq!(after.lines().filter(|l| *l == "model-a").count(), 2);
            for unselected in ["b", "c"].into_iter().filter(|id| *id != target) {
                assert!(!after.lines().any(
                    |l| l == format!("tool-{unselected}") || l == format!("model-{unselected}")
                ));
            }
            if recovery == "mismatch" {
                assert_eq!(before, after);
            } else {
                assert_eq!(
                    after
                        .lines()
                        .filter(|l| *l == format!("tool-{target}"))
                        .count(),
                    usize::from(recovery != "reject" && target != "direct")
                );
                assert_eq!(
                    after.lines().filter(|l| *l == "mapper").count(),
                    if boundary == "first" { 2 } else { 1 }
                );
            }
        }
    }
}
