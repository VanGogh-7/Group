mod support;

use std::error::Error as StdError;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    END, EventConfig, GraphRunError, GraphState, Node, NodeContext, NodeError, RunConfig,
    RunControl, START, StateError, StateGraph,
};
use group_agent_mcp::{
    McpAdapterError, McpAdapterErrorKind, McpClientSession, McpDiscoveryConfig, McpServerConfig,
    McpServerId,
};
use group_agent_model::{Message, ToolCall, ToolCallId, ToolName};
use group_agent_tool::{ToolRuntime, ToolRuntimeError};
use serde_json::json;
use support::{ServerScenario, in_process_session};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct McpNodeState {
    call: ToolCall,
    message: Option<Message>,
}

struct McpNodeUpdate {
    message: Message,
}

impl GraphState for McpNodeState {
    type Update = McpNodeUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.message = Some(update.message);
        Ok(())
    }
}

struct McpNode {
    runtime: ToolRuntime,
}

#[async_trait]
impl Node<McpNodeState> for McpNode {
    async fn run(
        &self,
        state: &McpNodeState,
        _context: &NodeContext,
    ) -> Result<McpNodeUpdate, NodeError> {
        let message = self
            .runtime
            .execute_message(&state.call)
            .await
            .map_err(|source| NodeError::with_source("MCP tool execution failed", source))?;
        Ok(McpNodeUpdate { message })
    }
}

fn graph(runtime: ToolRuntime) -> group_agent_core::CompiledGraph<McpNodeState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("mcp", McpNode { runtime })
        .expect("node registers");
    graph.add_edge(START, "mcp").add_edge("mcp", END);
    graph.compile().expect("graph compiles")
}

fn call(id: &str, name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall::new(
        ToolCallId::new(id).expect("valid call id"),
        ToolName::new(name).expect("valid tool name"),
        arguments,
    )
}

fn source_of<'a, T>(error: &'a (dyn StdError + 'static)) -> Option<&'a T>
where
    T: StdError + 'static,
{
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(concrete) = source.downcast_ref::<T>() {
            return Some(concrete);
        }
        current = source.source();
    }
    None
}

#[tokio::test]
async fn mcp_tool_runtime_runs_as_an_ordinary_group_node_and_preserves_call_id() {
    let (session, _, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();
    let report = graph(runtime)
        .invoke(McpNodeState {
            call: call("group-mcp-call", "echo", json!({"text": "node"})),
            message: None,
        })
        .await
        .expect("graph succeeds");
    let message = report
        .final_state()
        .message
        .as_ref()
        .and_then(Message::as_tool)
        .expect("Tool message stored");
    assert_eq!(message.tool_call_id().as_str(), "group-mcp-call");
    assert_eq!(message.result().content()[0].as_text(), Some("node"));

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn graph_error_chain_reaches_rmcp_service_error_without_default_payload_leak() {
    let (session, _, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();
    let error = graph(runtime)
        .invoke(McpNodeState {
            call: call("group-mcp-error", "protocol_error", json!({})),
            message: None,
        })
        .await
        .expect_err("graph fails");
    assert!(matches!(error, GraphRunError::NodeFailed { .. }));
    assert!(source_of::<NodeError>(&error).is_some());
    assert!(source_of::<ToolRuntimeError>(&error).is_some());
    assert!(source_of::<McpAdapterError>(&error).is_some());
    assert!(source_of::<rmcp::ServiceError>(&error).is_some());
    assert!(!format!("{error}").contains("SECRET_REMOTE_PROTOCOL_ERROR"));
    assert!(!format!("{error:?}").contains("SECRET_PROTOCOL_PAYLOAD"));

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn group_timeout_and_cancellation_drop_real_pending_mcp_call_futures() {
    let (timeout_session, timeout_state, timeout_server) =
        in_process_session(ServerScenario::Standard).await;
    let timeout_runtime = timeout_session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();
    let timeout_error = graph(timeout_runtime)
        .invoke_with_control(
            McpNodeState {
                call: call("group-timeout", "pending", json!({})),
                message: None,
            },
            RunConfig::default(),
            EventConfig::default(),
            RunControl::new().with_node_timeout(Duration::from_millis(20)),
        )
        .await
        .expect_err("node timeout drops the pending MCP call future");
    assert!(matches!(timeout_error, GraphRunError::NodeTimedOut { .. }));
    assert_eq!(timeout_state.tool_calls.load(Ordering::SeqCst), 1);
    timeout_state.pending_release.add_permits(1);
    timeout_session.shutdown().await.expect("shutdown succeeds");
    timeout_server.await.expect("server joins");

    let (cancel_session, cancel_state, cancel_server) =
        in_process_session(ServerScenario::Standard).await;
    let cancel_runtime = cancel_session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();
    let token = CancellationToken::new();
    let cancel_task = {
        let token = token.clone();
        tokio::spawn(async move {
            graph(cancel_runtime)
                .invoke_with_control(
                    McpNodeState {
                        call: call("group-cancel", "pending", json!({})),
                        message: None,
                    },
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };
    while cancel_state.tool_calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    token.cancel();
    let cancel_error = cancel_task
        .await
        .expect("graph task joins")
        .expect_err("cancellation drops the pending MCP call future");
    assert!(matches!(cancel_error, GraphRunError::Cancelled { .. }));
    assert_eq!(cancel_state.tool_calls.load(Ordering::SeqCst), 1);
    cancel_state.pending_release.add_permits(1);
    cancel_session.shutdown().await.expect("shutdown succeeds");
    cancel_server.await.expect("server joins");
}

#[cfg(unix)]
#[tokio::test]
async fn child_transport_source_reaches_graph_run_error_with_exact_classification() {
    let session = McpClientSession::connect_stdio(
        McpServerConfig::new(
            McpServerId::new("group-child-source").expect("valid id"),
            env!("CARGO_BIN_EXE_group-agent-mcp-test-server"),
        )
        .expect("valid child config")
        .with_arg("--disconnect-on-call"),
    )
    .await
    .expect("child initializes");
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("child discovery")
        .runtime();
    let error = graph(runtime)
        .invoke(McpNodeState {
            call: call("group-child-error", "echo", json!({"text": "x"})),
            message: None,
        })
        .await
        .expect_err("child disconnect fails the Group node");
    let adapter = source_of::<McpAdapterError>(&error).expect("adapter source");
    assert_eq!(adapter.kind(), McpAdapterErrorKind::Transport);
    assert!(source_of::<rmcp::ServiceError>(&error).is_some());
    session.shutdown().await.expect("shutdown remains safe");
}
