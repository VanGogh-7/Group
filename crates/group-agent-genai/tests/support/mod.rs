#![allow(dead_code)]

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use genai::adapter::AdapterKind;
use genai::resolver::{AuthData, Endpoint, ServiceTargetResolver};
use genai::{Client, ClientConfig, ModelIden, ServiceTarget};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig, GenaiStreamingPolicy,
};
use group_agent_model::{ChatModel, ModelCapabilities, ModelId, ProviderId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct MockResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
    pub headers: Vec<(&'static str, &'static str)>,
}

impl MockResponse {
    pub fn json(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body: body.into(),
            headers: Vec::new(),
        }
    }

    pub fn sse(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            content_type: "text/event-stream",
            body: body.into(),
            headers: Vec::new(),
        }
    }

    pub fn status(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: body.into(),
            headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.push((name, value));
        self
    }
}

pub struct MockServer {
    base_url: String,
    request_rx: Option<oneshot::Receiver<Vec<u8>>>,
    task: JoinHandle<io::Result<()>>,
    hits: Arc<AtomicUsize>,
    accepted_connections: Arc<AtomicUsize>,
}

pub struct HangingSseServer {
    base_url: String,
    closed_rx: Option<oneshot::Receiver<()>>,
    task: JoinHandle<io::Result<()>>,
}

pub struct HangingRequestServer {
    base_url: String,
    received_rx: Option<oneshot::Receiver<()>>,
    closed_rx: Option<oneshot::Receiver<()>>,
    task: JoinHandle<io::Result<()>>,
}

impl HangingRequestServer {
    pub async fn start() -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let (received_tx, received_rx) = oneshot::channel();
        let (closed_tx, closed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let _request = read_request(&mut socket).await?;
            let _ = received_tx.send(());
            let mut byte = [0_u8; 1];
            loop {
                match socket.read(&mut byte).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = closed_tx.send(());
            Ok(())
        });
        Ok(Self {
            base_url: format!("http://{address}/v1/"),
            received_rx: Some(received_rx),
            closed_rx: Some(closed_rx),
            task,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn wait_received(&mut self) {
        self.received_rx
            .take()
            .expect("request can be awaited once")
            .await
            .expect("server reports request");
    }

    pub async fn wait_closed(&mut self) {
        self.closed_rx
            .take()
            .expect("close can be awaited once")
            .await
            .expect("server reports client close");
    }
}

impl Drop for HangingRequestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HangingSseServer {
    pub async fn start(first_event: &'static str) -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let (closed_tx, closed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let _request = read_request(&mut socket).await?;
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
                )
                .await?;
            socket.write_all(first_event.as_bytes()).await?;
            socket.flush().await?;
            let mut byte = [0_u8; 1];
            loop {
                match socket.read(&mut byte).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = closed_tx.send(());
            Ok(())
        });
        Ok(Self {
            base_url: format!("http://{address}/v1/"),
            closed_rx: Some(closed_rx),
            task,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn wait_closed(&mut self) {
        self.closed_rx
            .take()
            .expect("close can be awaited once")
            .await
            .expect("server reports client close");
    }
}

impl Drop for HangingSseServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockServer {
    pub async fn start(response: MockResponse) -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let (request_tx, request_rx) = oneshot::channel();
        let hits = Arc::new(AtomicUsize::new(0));
        let task_hits = Arc::clone(&hits);
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let task_accepted_connections = Arc::clone(&accepted_connections);
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            task_accepted_connections.fetch_add(1, Ordering::Relaxed);
            let request = read_request(&mut socket).await?;
            task_hits.fetch_add(1, Ordering::Relaxed);
            let _ = request_tx.send(request);
            write_response(&mut socket, response).await
        });
        Ok(Self {
            base_url: format!("http://{address}/v1/"),
            request_rx: Some(request_rx),
            task,
            hits,
            accepted_connections,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn hit_count(&self) -> usize {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn accepted_connection_count(&self) -> usize {
        self.accepted_connections.load(Ordering::Relaxed)
    }

    pub async fn request_json(&mut self) -> serde_json::Value {
        let bytes = self
            .request_rx
            .take()
            .expect("request can be inspected once")
            .await
            .expect("mock server sends request bytes");
        let body = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| &bytes[index + 4..])
            .expect("valid HTTP request");
        serde_json::from_slice(body).expect("request body is JSON")
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(socket: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let header_end;
    loop {
        let mut chunk = [0_u8; 4096];
        let count = socket.read(&mut chunk).await?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "HTTP request ended before headers",
            ));
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            header_end = index + 4;
            break;
        }
    }

    let content_length = std::str::from_utf8(&request[..header_end])
        .ok()
        .and_then(|headers| {
            headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        })
        .unwrap_or_default();
    while request.len() < header_end + content_length {
        let mut chunk = [0_u8; 4096];
        let count = socket.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..count]);
    }
    Ok(request)
}

async fn write_response(socket: &mut TcpStream, response: MockResponse) -> io::Result<()> {
    let reason = match response.status {
        200 => "OK",
        401 => "Unauthorized",
        403 => "Forbidden",
        408 => "Request Timeout",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Status",
    };
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        reason,
        response.content_type,
        response.body.len()
    );
    for (name, value) in response.headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(response.body.as_bytes()).await?;
    socket.shutdown().await
}

pub fn openai_client(base_url: impl Into<String>) -> Client {
    client_for_adapter(base_url, AdapterKind::OpenAI)
}

pub fn openai_responses_client(base_url: impl Into<String>) -> Client {
    client_for_adapter(base_url, AdapterKind::OpenAIResp)
}

pub fn stable_openai_model(
    base_url: impl Into<String>,
    capabilities: ModelCapabilities,
) -> Result<ChatModel, Box<dyn std::error::Error>> {
    stable_model(base_url, AdapterKind::OpenAI, capabilities)
}

pub fn stable_responses_model(
    base_url: impl Into<String>,
    capabilities: ModelCapabilities,
) -> Result<ChatModel, Box<dyn std::error::Error>> {
    stable_model(base_url, AdapterKind::OpenAIResp, capabilities)
}

fn client_for_adapter(base_url: impl Into<String>, adapter_kind: AdapterKind) -> Client {
    let base_url = base_url.into();
    let resolver = ServiceTargetResolver::from_resolver_fn(
        move |target: ServiceTarget| -> Result<ServiceTarget, genai::resolver::Error> {
            Ok(ServiceTarget {
                endpoint: Endpoint::from_owned(base_url.clone()),
                auth: AuthData::from_single("local-test-only"),
                model: ModelIden::new(adapter_kind, target.model.model_name),
            })
        },
    );
    Client::builder()
        .with_adapter_kind(adapter_kind)
        .with_service_target_resolver(resolver)
        .build()
}

pub fn model(
    client: Client,
    capabilities: ModelCapabilities,
) -> Result<ChatModel, Box<dyn std::error::Error>> {
    let streaming = capabilities.streaming();
    let model = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("local-openai")?,
        ModelId::new("configured-model")?,
        capabilities,
    )?;
    let config = GenaiAdapterConfig::new(model)
        .with_response_id_continuation(true)
        .with_reasoning_content(true)
        .with_streaming_policy(if streaming {
            GenaiStreamingPolicy::AuditedTextOnly
        } else {
            GenaiStreamingPolicy::Disabled
        });
    let adapter = GenaiChatModelAdapter::new(client, config)?;
    Ok(ChatModel::from_adapter(adapter)?)
}

pub fn responses_model(
    client: Client,
    capabilities: ModelCapabilities,
) -> Result<ChatModel, Box<dyn std::error::Error>> {
    let model = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("local-openai")?,
        ModelId::new("configured-model")?,
        capabilities,
    )?;
    let config = GenaiAdapterConfig::new(model)
        .with_response_id_continuation(true)
        .with_reasoning_content(true)
        .with_streaming_policy(GenaiStreamingPolicy::Disabled);
    let adapter = GenaiChatModelAdapter::new(client, config)?;
    Ok(ChatModel::from_adapter(adapter)?)
}

fn stable_model(
    base_url: impl Into<String>,
    adapter_kind: AdapterKind,
    capabilities: ModelCapabilities,
) -> Result<ChatModel, Box<dyn std::error::Error>> {
    let model = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("local-openai")?,
        ModelId::new("configured-model")?,
        capabilities,
    )?;
    let config = GenaiAdapterConfig::new(model)
        .with_response_id_continuation(true)
        .with_reasoning_content(true)
        .with_streaming_policy(
            if capabilities.streaming() && matches!(adapter_kind, AdapterKind::OpenAI) {
                GenaiStreamingPolicy::AuditedTextOnly
            } else {
                GenaiStreamingPolicy::Disabled
            },
        );
    let client_config = ClientConfig::default().with_adapter_kind(adapter_kind);
    let target = ServiceTarget {
        endpoint: Endpoint::from_owned(base_url.into()),
        auth: AuthData::from_single("local-test-only"),
        model: ModelIden::new(adapter_kind, "gpt-4o-mini"),
    };
    let adapter = GenaiChatModelAdapter::new_with_stable_target(client_config, target, config)?;
    Ok(ChatModel::from_adapter(adapter)?)
}
