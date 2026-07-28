pub mod server;

use std::sync::Arc;

use group_agent_mcp::{McpClientSession, McpServerId};
use tokio::task::JoinHandle;

pub use server::{ServerScenario, ServerState};

pub async fn in_process_session(
    scenario: ServerScenario,
) -> (McpClientSession, Arc<ServerState>, JoinHandle<()>) {
    in_process_session_with_id("offline", scenario).await
}

pub async fn in_process_session_with_id(
    server_id: &str,
    scenario: ServerScenario,
) -> (McpClientSession, Arc<ServerState>, JoinHandle<()>) {
    let state = Arc::new(ServerState::default());
    let (client, server) = tokio::io::duplex(256 * 1024);
    let (server_read, server_write) = tokio::io::split(server);
    let server_state = Arc::clone(&state);
    let handle = tokio::spawn(async move {
        server::serve(server_read, server_write, scenario, server_state).await;
    });
    let session = McpClientSession::connect(
        McpServerId::new(server_id).expect("valid server id"),
        client,
    )
    .await
    .expect("offline session initializes");
    (session, state, handle)
}
