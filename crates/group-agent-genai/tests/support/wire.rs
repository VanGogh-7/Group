use super::read_request;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct WireResponse {
    pub chunks: Vec<Vec<u8>>,
    content_type: &'static str,
}

impl WireResponse {
    pub fn sse(body: impl Into<String>) -> Self {
        Self {
            chunks: vec![body.into().into_bytes()],
            content_type: "text/event-stream",
        }
    }

    pub fn json(body: impl Into<String>) -> Self {
        Self {
            chunks: vec![body.into().into_bytes()],
            content_type: "application/json",
        }
    }

    pub fn bytewise(body: &[u8]) -> Self {
        Self {
            chunks: body.iter().map(|byte| vec![*byte]).collect(),
            content_type: "text/event-stream",
        }
    }
}

/// Sequential HTTP responses with exact chunk boundaries and captured requests.
pub struct WireServer {
    base_url: String,
    requests: mpsc::UnboundedReceiver<Vec<u8>>,
    task: JoinHandle<io::Result<()>>,
    hits: Arc<AtomicUsize>,
}

impl WireServer {
    pub async fn start(responses: Vec<WireResponse>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let base_url = format!("http://{}/v1/", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::unbounded_channel();
        let hits = Arc::new(AtomicUsize::new(0));
        let count = hits.clone();
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await?;
                let request = read_request(&mut socket).await?;
                count.fetch_add(1, Ordering::SeqCst);
                tx.send(request).unwrap();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n", response.content_type).as_bytes()).await?;
                for chunk in response.chunks {
                    if chunk.is_empty() {
                        continue;
                    }
                    socket
                        .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                        .await?;
                    socket.write_all(&chunk).await?;
                    socket.write_all(b"\r\n").await?;
                }
                socket.write_all(b"0\r\n\r\n").await?;
            }
            Ok(())
        });
        Self {
            base_url,
            requests,
            task,
            hits,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    pub async fn request(&mut self) -> (String, serde_json::Value) {
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(5), self.requests.recv())
            .await
            .expect("request deadline")
            .expect("captured request");
        let index = bytes.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
        (
            String::from_utf8(bytes[..index].to_vec()).unwrap(),
            serde_json::from_slice(&bytes[index + 4..]).unwrap(),
        )
    }
}

impl Drop for WireServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
