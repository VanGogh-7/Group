#[path = "../tests/support/server.rs"]
mod server;

use async_trait::async_trait;
use group_agent_core::{
    END, GraphState, Node, NodeContext, NodeError, START, StateError, StateGraph,
};
use group_agent_mcp::{McpClientSession, McpDiscoveryConfig, McpServerConfig, McpServerId};
use group_agent_model::{Message, ToolCall, ToolCallId, ToolName};
use group_agent_tool::ToolRuntime;
use serde_json::json;

#[derive(Debug)]
struct McpState {
    call: ToolCall,
    message: Option<Message>,
}

struct McpUpdate {
    message: Message,
}

impl GraphState for McpState {
    type Update = McpUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.message = Some(update.message);
        Ok(())
    }
}

struct McpToolNode {
    runtime: ToolRuntime,
}

#[async_trait]
impl Node<McpState> for McpToolNode {
    async fn run(&self, state: &McpState, _context: &NodeContext) -> Result<McpUpdate, NodeError> {
        let message = self
            .runtime
            .execute_message(&state.call)
            .await
            .map_err(|source| NodeError::with_source("MCP Tool execution failed", source))?;
        Ok(McpUpdate { message })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|argument| argument == "--server") {
        server::serve_stdio(server::ServerScenario::Standard).await;
        return Ok(());
    }

    let session = McpClientSession::connect_stdio(
        McpServerConfig::new(McpServerId::new("node-example")?, std::env::current_exe()?)?
            .with_arg("--server"),
    )
    .await?;
    let runtime = session.discover(McpDiscoveryConfig::new()).await?.runtime();

    let mut graph = StateGraph::new();
    graph.add_node("mcp_tool", McpToolNode { runtime })?;
    graph.add_edge(START, "mcp_tool").add_edge("mcp_tool", END);
    let report = graph
        .compile()?
        .invoke(McpState {
            call: ToolCall::new(
                ToolCallId::new("node-mcp-call-1")?,
                ToolName::new("calculator")?,
                json!({"a": 20, "b": 22}),
            ),
            message: None,
        })
        .await?;
    let message = report
        .final_state()
        .message
        .as_ref()
        .and_then(Message::as_tool)
        .expect("Node stores a Tool message");
    println!(
        "node call_id={}, is_error={}, parts={}",
        message.tool_call_id(),
        message.result().is_error(),
        message.result().content().len()
    );
    session.shutdown().await?;
    Ok(())
}
