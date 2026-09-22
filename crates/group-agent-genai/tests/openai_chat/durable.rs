use super::*;
use async_trait::async_trait;
use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointStore, Checkpointer, EventConfig,
    InMemoryCheckpointer, RecordCheckpointer, ResumeConfig, RunControl, RunId, ThreadId,
};
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentConfig, AgentEventSink, AgentSnapshot, AgentSnapshotCodec,
    AgentStreamEvent, ToolCallingAgent,
};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::HangingRequestServer;
use support::wire::{WireResponse, WireServer};
use tokio_util::sync::CancellationToken;

struct Lookup {
    definition: ToolDefinition,
    executions: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for Lookup {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }
    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }
    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }
    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::success_text(format!(
            "result:{}",
            input.arguments()["q"].as_str().unwrap()
        )))
    }
}

#[derive(Default)]
struct Events(Mutex<Vec<AgentStreamEvent>>);
impl AgentEventSink for Events {
    fn on_event(&self, event: &AgentStreamEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

fn agent(base_url: &str, executions: &Arc<AtomicUsize>, approval: bool) -> ToolCallingAgent {
    let mut registry = ToolRegistry::builder();
    registry
        .register(Lookup {
            definition: request().tools()[0].clone(),
            executions: executions.clone(),
        })
        .unwrap();
    ToolCallingAgent::new(
        model(base_url),
        ToolRuntime::new(registry.build()),
        AgentConfig::new(2).unwrap().with_tool_approval(approval),
    )
    .unwrap()
}

async fn connect(url: &str) -> (sqlx::SqlitePool, Arc<RecordCheckpointer<AgentSnapshot>>) {
    use std::str::FromStr;
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(url)
        .unwrap()
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    let sqlite = Arc::new(SqliteCheckpointStore::from_pool(pool.clone()));
    sqlite.migrate().await.unwrap();
    let store: Arc<dyn CheckpointStore> = sqlite.clone();
    (
        pool,
        Arc::new(RecordCheckpointer::new(store, Arc::new(AgentSnapshotCodec))),
    )
}

fn tool_response(arguments: &str) -> String {
    chunk(
        json!({"content":"checking", "tool_calls":[
            {"index":0,"id":"call-a","type":"function","function":{"name":"lookup","arguments":arguments}},
            {"index":1,"id":"call-b","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"second\"}"}}
        ]}),
        json!("tool_calls"),
    ) + "data: [DONE]\n\n"
}

fn answer() -> String {
    chunk(json!({"content":"all done"}), json!("stop")) + "data: [DONE]\n\n"
}

#[tokio::test]
async fn http_tool_stream_sqlite_reopen_approval_and_rejection_complete_with_new_agent() {
    for decision in [
        AgentApprovalDecision::Approve,
        AgentApprovalDecision::Reject,
    ] {
        let mut server = WireServer::start(vec![
            WireResponse::bytewise(tool_response("{\"q\":\"first\"}").as_bytes()),
            WireResponse::sse(answer()),
        ])
        .await;
        let directory =
            std::env::temp_dir().join(format!("group-provider-stream-{}", RunId::new()));
        std::fs::create_dir_all(&directory).unwrap();
        let url = format!(
            "sqlite://{}",
            directory.join("checkpoint.sqlite3").display()
        );
        let executions = Arc::new(AtomicUsize::new(0));
        let id = {
            let (sqlite, store) = connect(&url).await;
            let agent = agent(server.base_url(), &executions, true);
            let events = agent
                .stream_with_checkpoint(
                    vec![Message::user("SECRET_PROMPT")],
                    CheckpointConfig::new(
                        "restart",
                        store.clone(),
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            let AgentStreamEvent::Interrupted(interrupted) = events.last().unwrap() else {
                panic!("persisted interruption");
            };
            let pending = interrupted.approval_request().unwrap().pending_calls();
            assert_eq!(pending.len(), 2);
            assert_eq!(pending[0].arguments(), &json!({"q":"first"}));
            assert_eq!(pending[1].arguments(), &json!({"q":"second"}));
            assert_eq!(executions.load(Ordering::SeqCst), 0);
            assert!(
                store
                    .latest(&ThreadId::from("restart"))
                    .await
                    .unwrap()
                    .unwrap()
                    .interrupted()
            );
            let id = interrupted.checkpoint_id();
            drop(agent);
            drop(store);
            sqlite.close().await;
            id
        };
        {
            let (sqlite, store) = connect(&url).await;
            let agent = agent(server.base_url(), &executions, true);
            let sink = Arc::new(Events::default());
            let outcome = agent
                .resume_with_stream_sink(
                    ResumeConfig::new("restart", store.clone())
                        .with_checkpoint_id(id)
                        .with_resume_value(decision),
                    sink.clone(),
                )
                .await
                .unwrap();
            let outcome = outcome.as_completed().unwrap();
            assert_eq!(outcome.final_message().unwrap().text_content(), "all done");
            assert_eq!(
                executions.load(Ordering::SeqCst),
                if decision == AgentApprovalDecision::Approve {
                    2
                } else {
                    0
                }
            );
            assert_eq!(
                outcome
                    .messages()
                    .iter()
                    .filter(|m| m.as_tool().is_some())
                    .count(),
                2
            );
            assert!(
                outcome
                    .messages()
                    .iter()
                    .filter_map(Message::as_tool)
                    .all(|m| m.result().is_error() == (decision == AgentApprovalDecision::Reject))
            );
            let events = sink.0.lock().unwrap().clone();
            assert!(matches!(
                events.last(),
                Some(AgentStreamEvent::Completed(_))
            ));
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, AgentStreamEvent::ApprovalRequired { .. }))
            );
            let again = agent
                .resume_stream(ResumeConfig::new("restart", store.clone()))
                .collect::<Vec<_>>()
                .await;
            assert!(matches!(
                again.as_slice(),
                [Ok(AgentStreamEvent::Completed(_))]
            ));
            assert_eq!(
                server.hits(),
                2,
                "completed resume does not dispatch a model"
            );
            drop(agent);
            drop(store);
            sqlite.close().await;
        }
        let (_, first) = server.request().await;
        assert_eq!(first["stream"], true);
        assert_eq!(first["messages"].as_array().unwrap().len(), 1);
        let (_, resumed) = server.request().await;
        let messages = resumed["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["arguments"],
            "{\"q\":\"first\"}"
        );
        assert_eq!(messages[2]["tool_call_id"], "call-a");
        assert_eq!(messages[3]["tool_call_id"], "call-b");
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn saved_tool_results_survive_later_http_failure_without_reexecution() {
    let server = WireServer::start(vec![
        WireResponse::sse(tool_response("{\"q\":\"first\"}")),
        WireResponse::sse(chunk(json!({"content":"unfinished"}), Value::Null)),
        WireResponse::sse(answer()),
    ])
    .await;
    let directory = std::env::temp_dir().join(format!("group-provider-recovery-{}", RunId::new()));
    std::fs::create_dir_all(&directory).unwrap();
    let url = format!(
        "sqlite://{}",
        directory.join("checkpoint.sqlite3").display()
    );
    let executions = Arc::new(AtomicUsize::new(0));
    {
        let (sqlite, store) = connect(&url).await;
        let agent = agent(server.base_url(), &executions, false);
        let events = agent
            .stream_with_checkpoint(
                vec![Message::user("hello")],
                CheckpointConfig::new("recover", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .collect::<Vec<_>>()
            .await;
        assert!(events.last().unwrap().is_err());
        assert_eq!(executions.load(Ordering::SeqCst), 2);
        drop(agent);
        drop(store);
        sqlite.close().await;
    }
    {
        let (sqlite, store) = connect(&url).await;
        let agent = agent(server.base_url(), &executions, false);
        let events = agent
            .resume_stream(ResumeConfig::new("recover", store.clone()))
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            events.last(),
            Some(Ok(AgentStreamEvent::Completed(_)))
        ));
        assert_eq!(executions.load(Ordering::SeqCst), 2);
        assert_eq!(server.hits(), 3);
        drop(agent);
        drop(store);
        sqlite.close().await;
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn invalid_protocol_or_arguments_never_execute_tools() {
    for (body, approval) in [
        (tool_response("{broken"), true),
        (
            tool_response("{\"q\":\"first\"}").replace("data: [DONE]\n\n", ""),
            true,
        ),
        (
            chunk(
                json!({"tool_calls":[{"index":0,"id":"call-a","type":"function","function":{"name":"lookup","arguments":"{\"q\":17}"}}]}),
                json!("tool_calls"),
            ) + "data: [DONE]\n\n",
            false,
        ),
    ] {
        let server = WireServer::start(vec![WireResponse::sse(body)]).await;
        let executions = Arc::new(AtomicUsize::new(0));
        let agent = agent(server.base_url(), &executions, approval);
        let store = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
        let events = agent
            .stream_with_checkpoint(
                vec![Message::user("hello")],
                CheckpointConfig::new("invalid", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .collect::<Vec<_>>()
            .await;
        assert!(events.last().unwrap().is_err());
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert!(!events.iter().any(|e| matches!(
            e,
            Ok(AgentStreamEvent::Interrupted(_) | AgentStreamEvent::Completed(_))
        )));
        if approval {
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, Ok(AgentStreamEvent::ApprovalRequired { .. })))
            );
            if let Some(saved) = store.latest(&ThreadId::from("invalid")).await.unwrap() {
                assert!(!saved.interrupted());
            }
        }
    }
}

#[tokio::test]
async fn durable_run_cancellation_and_node_timeout_drop_the_actual_http_request() {
    for cancel in [true, false] {
        let mut server = HangingRequestServer::start().await.unwrap();
        let executions = Arc::new(AtomicUsize::new(0));
        let agent = agent(server.base_url(), &executions, true);
        let store = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
        let token = CancellationToken::new();
        let control = if cancel {
            RunControl::new().with_cancellation_token(token.clone())
        } else {
            RunControl::new().with_node_timeout(Duration::from_millis(200))
        };
        let stream = agent.stream_with_checkpoint_control(
            vec![Message::user("hello")],
            EventConfig::default(),
            control,
            CheckpointConfig::new(
                "controlled",
                store.clone(),
                CheckpointPolicy::EverySuperstep,
            ),
        );
        let (events, ()) = tokio::join!(stream.collect::<Vec<_>>(), async {
            server.wait_received().await;
            if cancel {
                token.cancel();
            }
        });
        assert!(events.last().unwrap().is_err());
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert!(!events.iter().any(|e| matches!(
            e,
            Ok(AgentStreamEvent::Interrupted(_) | AgentStreamEvent::Completed(_))
        )));
        tokio::time::timeout(Duration::from_secs(5), server.wait_closed())
            .await
            .unwrap();
    }
}
