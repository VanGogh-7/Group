#[path = "../tests/support/server.rs"]
mod server;

use group_agent_mcp::{McpClientSession, McpDiscoveryConfig, McpServerConfig, McpServerId};
use group_agent_model::{Message, ToolCall, ToolCallId, ToolName};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|argument| argument == "--server") {
        server::serve_stdio(server::ServerScenario::Standard).await;
        return Ok(());
    }

    let config = McpServerConfig::new(McpServerId::new("example")?, std::env::current_exe()?)?
        .with_arg("--server");
    let session = McpClientSession::connect_stdio(config).await?;
    let tools = session.discover(McpDiscoveryConfig::new()).await?;
    let runtime = tools.runtime();
    let call = ToolCall::new(
        ToolCallId::new("example-call-1")?,
        ToolName::new("echo")?,
        json!({"text": "offline MCP"}),
    );
    let message = runtime.execute_message(&call).await?;
    let Message::Tool(message) = message else {
        return Err("expected Tool message".into());
    };
    println!(
        "server={}, tools={}, call_id={}, is_error={}",
        session.server_id(),
        tools.len(),
        message.tool_call_id(),
        message.result().is_error()
    );
    session.shutdown().await?;
    Ok(())
}
