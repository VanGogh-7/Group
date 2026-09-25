#[path = "../test_support/streaming_agent.rs"]
mod fixture;
#[path = "../test_support/process_agent.rs"]
mod process_agent;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointStore, Checkpointer, RecordCheckpointer,
    ResumeConfig, RunId, ThreadId,
};
use group_agent_model::{Message, ToolResult};
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentSnapshot, AgentSnapshotCodec, AgentStreamEvent,
};

const THREAD: &str = "process-recovery";
const TIMEOUT: Duration = Duration::from_secs(30);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("group-process-{}", RunId::new()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// Drop kills and reaps even if an assertion or readiness timeout panics.
struct Worker(Child);
impl Worker {
    fn spawn(directory: &Path, phase: &str) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "process_worker", "--nocapture"])
                .env("GROUP_RECOVERY_DIRECTORY", directory)
                .env("GROUP_RECOVERY_PHASE", phase)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        )
    }

    fn wait_ready(&mut self, directory: &Path) {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "worker exited before kill"
            );
            if directory.join("ready").exists() {
                return;
            }
            assert!(Instant::now() < deadline, "worker readiness timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn kill(&mut self) {
        self.0.kill().unwrap();
        let status = self.0.wait().unwrap();
        assert!(!status.success(), "worker must be forcibly terminated");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
    }

    fn wait_success(&mut self) {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "recovery worker failed: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "recovery worker timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn connect(directory: &Path) -> Arc<RecordCheckpointer<AgentSnapshot>> {
    let url = format!(
        "sqlite://{}",
        directory.join("checkpoint.sqlite3").display()
    );
    let store = Arc::new(SqliteCheckpointStore::connect(&url).await.unwrap());
    store.migrate().await.unwrap();
    let store: Arc<dyn CheckpointStore> = store;
    Arc::new(RecordCheckpointer::new(store, Arc::new(AgentSnapshotCodec)))
}

// Invoked normally this is inert; only explicitly spawned workers receive a phase.
#[tokio::test]
async fn process_worker() {
    let Some(directory) = std::env::var_os("GROUP_RECOVERY_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let phase = std::env::var("GROUP_RECOVERY_PHASE").unwrap();
    let store = connect(&directory).await;
    let saved_tool = matches!(phase.as_str(), "save-tool" | "resume-tool");
    let (agent, probe) = process_agent::agent(
        directory.clone(),
        !saved_tool,
        phase == "save-tool",
        store.clone(),
    );
    if matches!(phase.as_str(), "interrupt" | "save-tool") {
        let mut stream = agent.stream_with_checkpoint(
            vec![Message::user("SECRET_PROMPT")],
            CheckpointConfig::new(THREAD, store.clone(), CheckpointPolicy::EverySuperstep),
        );
        while let Some(event) = stream.next().await {
            if let AgentStreamEvent::Interrupted(interrupted) = event.unwrap() {
                let head = store
                    .latest(&ThreadId::from(THREAD))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(head.interrupted());
                assert_eq!(head.id(), interrupted.checkpoint_id());
                assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
                fs::write(directory.join("ready"), b"interrupt persisted").unwrap();
                // Keep the live stream, checkpointer, and pool in this process.
                std::future::pending::<()>().await;
            }
        }
        panic!("missing interruption");
    }
    let decision = match phase.as_str() {
        "approve" | "resume-tool" => AgentApprovalDecision::Approve,
        "reject" => AgentApprovalDecision::Reject,
        _ => panic!("unknown worker phase"),
    };
    let config = ResumeConfig::new(THREAD, store.clone());
    let config = if saved_tool {
        config
    } else {
        config.with_resume_value(decision)
    };
    let mut stream = agent.resume_stream(config);
    let mut completions = 0;
    while let Some(event) = stream.next().await {
        if let AgentStreamEvent::Completed(outcome) = event.unwrap() {
            completions += 1;
            assert_eq!(outcome.model_rounds(), 2);
            assert_eq!(outcome.usage_by_round().len(), 2);
            assert_eq!(
                outcome.final_message().unwrap().text_content(),
                "SECRET_FINAL"
            );
        }
    }
    assert_eq!(completions, 1);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.executions.load(Ordering::SeqCst),
        usize::from(!saved_tool && decision == AgentApprovalDecision::Approve)
    );
    {
        let transcripts = probe.transcripts.lock().unwrap();
        assert_eq!(transcripts[0].len(), 3);
        assert_eq!(transcripts[0][0], Message::user("SECRET_PROMPT"));
        assert_eq!(
            transcripts[0][1].as_assistant().unwrap().tool_calls().len(),
            1
        );
        let tool_message = transcripts[0][2].as_tool().unwrap();
        assert_eq!(
            tool_message.tool_call_id(),
            transcripts[0][1].as_assistant().unwrap().tool_calls()[0].id()
        );
        assert_eq!(
            tool_message.result().is_error(),
            decision == AgentApprovalDecision::Reject
        );
        if decision == AgentApprovalDecision::Approve {
            assert_eq!(tool_message.result(), &ToolResult::text("SECRET_RESULT"));
        }
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

#[test]
fn killed_approval_process_resumes_with_approval() {
    recover("interrupt", "approve", 1);
}

#[test]
fn killed_approval_process_resumes_with_rejection() {
    recover("interrupt", "reject", 0);
}

#[test]
fn killed_process_resumes_saved_tool_result_without_reexecution() {
    recover("save-tool", "resume-tool", 1);
}

fn executions(directory: &Path) -> usize {
    match fs::read_to_string(directory.join("tool-executions")) {
        Ok(contents) => contents
            .lines()
            .map(|line| {
                assert_eq!(line, "executed");
                1
            })
            .sum(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!("cannot read tool journal: {error}"),
    }
}

fn recover(phase: &str, decision: &str, expected: usize) {
    let directory = Directory::new();
    let mut first = Worker::spawn(&directory.0, phase);
    first.wait_ready(&directory.0);
    first.kill();
    assert_eq!(executions(&directory.0), usize::from(phase == "save-tool"));
    let mut recovered = Worker::spawn(&directory.0, decision);
    recovered.wait_success();
    assert_eq!(executions(&directory.0), expected);
}
