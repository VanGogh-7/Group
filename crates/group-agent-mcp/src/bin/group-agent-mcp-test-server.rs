#[path = "../../tests/support/server.rs"]
mod server;

#[tokio::main]
async fn main() {
    let mut scenario = server::ServerScenario::Standard;
    let mut pending_marker = None;
    let mut shutdown_marker = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--disconnect-on-call" => scenario = server::ServerScenario::DisconnectOnCall,
            "--stubborn" => scenario = server::ServerScenario::Stubborn,
            "--pending-marker" => pending_marker = arguments.next().map(Into::into),
            "--shutdown-marker" => shutdown_marker = arguments.next().map(Into::into),
            _ => {}
        }
    }
    server::serve_stdio_with_markers(scenario, pending_marker, shutdown_marker).await;
}
