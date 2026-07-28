#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ServerScenario {
    #[default]
    Standard,
    CapabilityMissing,
    InitializationProtocolError,
    InvalidSchema,
    InvalidName,
    DisconnectOnCall,
    SameCursor,
    TwoCursorCycle,
    MultiCursorCycle,
    EndlessPagination,
    SecondPageProtocolError,
    SecondPageDisconnect,
    DuplicateRemoteTool,
    Stubborn,
}

pub struct ServerState {
    pub connections: AtomicUsize,
    pub list_calls: AtomicUsize,
    pub tool_calls: AtomicUsize,
    pub active_calls: AtomicUsize,
    pub max_active_calls: AtomicUsize,
    pub pending_started: Notify,
    pub pending_release: Semaphore,
    pending_marker: Option<PathBuf>,
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            connections: AtomicUsize::new(0),
            list_calls: AtomicUsize::new(0),
            tool_calls: AtomicUsize::new(0),
            active_calls: AtomicUsize::new(0),
            max_active_calls: AtomicUsize::new(0),
            pending_started: Notify::new(),
            pending_release: Semaphore::new(0),
            pending_marker: None,
        }
    }
}

impl ServerState {
    fn enter_call(&self) {
        self.tool_calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active_calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active_calls.fetch_max(active, Ordering::SeqCst);
    }

    fn leave_call(&self) {
        self.active_calls.fetch_sub(1, Ordering::SeqCst);
    }
}

pub async fn serve<R, W>(read: R, write: W, scenario: ServerScenario, state: Arc<ServerState>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Send + Unpin + 'static,
{
    state.connections.fetch_add(1, Ordering::SeqCst);
    let mut lines = BufReader::new(read).lines();
    let writer = Arc::new(Mutex::new(write));
    let mut calls = JoinSet::new();

    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            continue;
        };
        let id = message.get("id").cloned();
        match method {
            "initialize" => {
                if scenario == ServerScenario::InitializationProtocolError {
                    respond_error(&writer, id).await;
                    continue;
                }
                let protocol_version = message
                    .pointer("/params/protocolVersion")
                    .cloned()
                    .unwrap_or_else(|| json!("2025-11-25"));
                let capabilities = if scenario == ServerScenario::CapabilityMissing {
                    json!({})
                } else {
                    json!({"tools": {"listChanged": true}})
                };
                respond(
                    &writer,
                    id,
                    json!({
                        "protocolVersion": protocol_version,
                        "capabilities": capabilities,
                        "serverInfo": {
                            "name": "group-agent-mcp-offline-test",
                            "version": "1"
                        }
                    }),
                )
                .await;
            }
            "notifications/initialized" => {}
            "tools/list" => {
                state.list_calls.fetch_add(1, Ordering::SeqCst);
                let cursor = message.pointer("/params/cursor").and_then(Value::as_str);
                if cursor.is_some() && scenario == ServerScenario::SecondPageDisconnect {
                    break;
                }
                if cursor.is_some() && scenario == ServerScenario::SecondPageProtocolError {
                    respond_error(&writer, id).await;
                    continue;
                }
                let result = list_tools(scenario, cursor);
                respond(&writer, id, result).await;
            }
            "tools/call" if scenario == ServerScenario::DisconnectOnCall => break,
            "tools/call" => {
                let writer = Arc::clone(&writer);
                let state = Arc::clone(&state);
                let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
                calls.spawn(async move {
                    state.enter_call();
                    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                    let arguments = params
                        .get("arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    if name == "protocol_error" {
                        respond_error(&writer, id).await;
                    } else {
                        let result = call_result(name, arguments, &state).await;
                        respond(&writer, id, result).await;
                    }
                    state.leave_call();
                });
            }
            _ => {
                if id.is_some() {
                    let mut writer = writer.lock().await;
                    let response = json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32601, "message": "method unavailable"}
                    });
                    let _ = writer.write_all(response.to_string().as_bytes()).await;
                    let _ = writer.write_all(b"\n").await;
                    let _ = writer.flush().await;
                }
            }
        }
    }

    calls.abort_all();
    while calls.join_next().await.is_some() {}
}

pub async fn serve_stdio(scenario: ServerScenario) {
    serve_stdio_with_markers(scenario, None, None).await;
}

pub async fn serve_stdio_with_markers(
    scenario: ServerScenario,
    pending_marker: Option<PathBuf>,
    shutdown_marker: Option<PathBuf>,
) {
    let state = ServerState {
        pending_marker,
        ..ServerState::default()
    };
    serve(
        tokio::io::stdin(),
        tokio::io::stdout(),
        scenario,
        Arc::new(state),
    )
    .await;
    if let Some(marker) = shutdown_marker {
        use std::io::Write as _;

        if let Ok(mut marker) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(marker)
        {
            let _ = writeln!(marker, "{}", std::process::id());
        }
    }
    if scenario == ServerScenario::Stubborn {
        std::future::pending::<()>().await;
    }
}

async fn respond<W>(writer: &Arc<Mutex<W>>, id: Option<Value>, result: Value)
where
    W: AsyncWrite + Unpin,
{
    let Some(id) = id else {
        return;
    };
    let response = json!({"jsonrpc": "2.0", "id": id, "result": result});
    let mut writer = writer.lock().await;
    let _ = writer.write_all(response.to_string().as_bytes()).await;
    let _ = writer.write_all(b"\n").await;
    let _ = writer.flush().await;
}

async fn respond_error<W>(writer: &Arc<Mutex<W>>, id: Option<Value>)
where
    W: AsyncWrite + Unpin,
{
    let Some(id) = id else {
        return;
    };
    let response = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32603,
            "message": "SECRET_REMOTE_PROTOCOL_ERROR",
            "data": {"raw": "SECRET_PROTOCOL_PAYLOAD"}
        }
    });
    let mut writer = writer.lock().await;
    let _ = writer.write_all(response.to_string().as_bytes()).await;
    let _ = writer.write_all(b"\n").await;
    let _ = writer.flush().await;
}

fn list_tools(scenario: ServerScenario, cursor: Option<&str>) -> Value {
    match scenario {
        ServerScenario::CapabilityMissing | ServerScenario::InitializationProtocolError => {
            json!({"tools": []})
        }
        ServerScenario::InvalidSchema => json!({
            "tools": [{
                "name": "invalid_schema",
                "description": "Invalid schema fixture",
                "inputSchema": {"type": 17}
            }]
        }),
        ServerScenario::InvalidName => json!({
            "tools": [{
                "name": " invalid",
                "description": "Invalid name fixture",
                "inputSchema": {"type": "object"}
            }]
        }),
        ServerScenario::SameCursor => {
            json!({
                "tools": [tool("same_cursor_tool", empty_schema())],
                "nextCursor": "same"
            })
        }
        ServerScenario::TwoCursorCycle => cycle_page(cursor, &["cursor-a", "cursor-b"]),
        ServerScenario::MultiCursorCycle => {
            cycle_page(cursor, &["cursor-a", "cursor-b", "cursor-c"])
        }
        ServerScenario::EndlessPagination => {
            let page = cursor
                .and_then(|cursor| cursor.strip_prefix("cursor-"))
                .and_then(|cursor| cursor.parse::<usize>().ok())
                .map_or(0, |page| page + 1);
            json!({
                "tools": [tool(&format!("page_{page}"), empty_schema())],
                "nextCursor": format!("cursor-{page}")
            })
        }
        ServerScenario::SecondPageProtocolError | ServerScenario::SecondPageDisconnect => {
            json!({
                "tools": [tool("first_page_only", empty_schema())],
                "nextCursor": "page-2"
            })
        }
        ServerScenario::DuplicateRemoteTool => {
            if cursor == Some("page-2") {
                json!({"tools": [tool("duplicate", empty_schema())]})
            } else {
                json!({
                    "tools": [tool("duplicate", empty_schema())],
                    "nextCursor": "page-2"
                })
            }
        }
        ServerScenario::Standard | ServerScenario::DisconnectOnCall | ServerScenario::Stubborn => {
            if cursor == Some("page-2") {
                json!({
                    "tools": [
                        tool("unsupported_image", empty_schema()),
                        tool("unsupported_audio", empty_schema()),
                        tool("unsupported_resource", empty_schema()),
                        tool("unsupported_resource_link", empty_schema()),
                        tool("pending", empty_schema()),
                        tool("multi_text", empty_schema()),
                        tool("text_and_structured", empty_schema()),
                        tool("malformed", empty_schema()),
                        tool("protocol_error", empty_schema()),
                        tool("child_pid", empty_schema())
                    ]
                })
            } else {
                json!({
                    "tools": [
                        tool("structured", empty_schema()),
                        tool("echo", echo_schema()),
                        tool("business_error", empty_schema()),
                        tool("calculator", calculator_schema())
                    ],
                    "nextCursor": "page-2"
                })
            }
        }
    }
}

fn cycle_page(cursor: Option<&str>, cursors: &[&str]) -> Value {
    let page = match cursor {
        None => 0,
        Some(cursor) => cursors
            .iter()
            .position(|candidate| *candidate == cursor)
            .map_or(0, |index| index + 1),
    };
    let next_cursor = cursors[page % cursors.len()];
    json!({
        "tools": [tool(&format!("cycle_page_{page}"), empty_schema())],
        "nextCursor": next_cursor
    })
}

fn tool(name: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "description": format!("Offline {name} fixture"),
        "inputSchema": input_schema
    })
}

fn empty_schema() -> Value {
    json!({"type": "object", "additionalProperties": false})
}

fn echo_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
        "additionalProperties": false
    })
}

fn calculator_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "a": {"type": "number"},
            "b": {"type": "number"}
        },
        "required": ["a", "b"],
        "additionalProperties": false
    })
}

async fn call_result(name: &str, arguments: Value, state: &ServerState) -> Value {
    match name {
        "echo" => json!({
            "content": [{
                "type": "text",
                "text": arguments.get("text").and_then(Value::as_str).unwrap_or("")
            }],
            "isError": false
        }),
        "calculator" => {
            let a = arguments.get("a").and_then(Value::as_f64).unwrap_or(0.0);
            let b = arguments.get("b").and_then(Value::as_f64).unwrap_or(0.0);
            json!({"content": [{"type": "text", "text": (a + b).to_string()}]})
        }
        "business_error" => json!({
            "content": [{"type": "text", "text": "business rejected"}],
            "isError": true
        }),
        "structured" => json!({
            "content": [],
            "structuredContent": {"answer": 42, "stable": true},
            "isError": false
        }),
        "multi_text" => json!({
            "content": [
                {"type": "text", "text": "first"},
                {"type": "text", "text": "second"}
            ]
        }),
        "text_and_structured" => json!({
            "content": [{"type": "text", "text": "text-first"}],
            "structuredContent": {"answer": 42, "stable": true}
        }),
        "unsupported_image" => json!({
            "content": [{"type": "image", "data": "SECRET_IMAGE_DATA", "mimeType": "image/png"}]
        }),
        "unsupported_audio" => json!({
            "content": [{
                "type": "audio",
                "data": "SECRET_AUDIO_DATA",
                "mimeType": "audio/wav"
            }]
        }),
        "unsupported_resource" => json!({
            "content": [{
                "type": "resource",
                "resource": {
                    "uri": "file:///SECRET_RESOURCE_URI",
                    "mimeType": "text/plain",
                    "text": "SECRET_RESOURCE_TEXT"
                }
            }]
        }),
        "unsupported_resource_link" => json!({
            "content": [{
                "type": "resource_link",
                "uri": "file:///SECRET_RESOURCE_LINK",
                "name": "SECRET_RESOURCE_NAME",
                "mimeType": "text/plain"
            }]
        }),
        "child_pid" => json!({
            "content": [{"type": "text", "text": std::process::id().to_string()}]
        }),
        "pending" => {
            if let Some(marker) = &state.pending_marker {
                let _ = std::fs::write(marker, std::process::id().to_string());
            }
            state.pending_started.notify_waiters();
            let permit = state
                .pending_release
                .acquire()
                .await
                .expect("test gate remains open");
            permit.forget();
            json!({"content": [{"type": "text", "text": "released"}]})
        }
        "malformed" => json!({"unexpected": "SECRET_PROTOCOL_PAYLOAD"}),
        _ => json!({"content": [{"type": "text", "text": "unknown"}], "isError": true}),
    }
}
