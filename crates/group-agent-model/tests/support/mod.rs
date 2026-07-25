use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream;
use group_agent_model::{
    AssistantMessage, ChatEventStream, ChatModel, ChatModelAdapter, ChatRequest, ChatResponse,
    ChatStreamEvent, FinishReason, ModelCapabilities, ModelError, ModelId, ModelMetadata,
    ProviderId, ValidatedChatRequest,
};
use tokio::sync::Notify;

pub struct PendingControl {
    pub started: Arc<Notify>,
    pub dropped: Arc<AtomicBool>,
}

enum CompleteBehavior {
    Fixed(ChatResponse),
    Error(Mutex<Option<ModelError>>),
    Pending(PendingControl),
}

pub struct ScriptedModel {
    metadata: ModelMetadata,
    behavior: CompleteBehavior,
    stream_events: Mutex<Option<Vec<Result<ChatStreamEvent, ModelError>>>>,
    calls: AtomicUsize,
    captured: Mutex<Vec<ChatRequest>>,
}

impl ScriptedModel {
    pub fn fixed(capabilities: ModelCapabilities, text: &str) -> Arc<Self> {
        let metadata = metadata(capabilities);
        Arc::new(Self {
            behavior: CompleteBehavior::Fixed(
                ChatResponse::new(AssistantMessage::text(text), FinishReason::Stop)
                    .with_model(metadata.model().clone()),
            ),
            metadata,
            stream_events: Mutex::new(None),
            calls: AtomicUsize::new(0),
            captured: Mutex::new(Vec::new()),
        })
    }

    pub fn error(error: ModelError) -> Arc<Self> {
        Arc::new(Self {
            metadata: metadata(ModelCapabilities::new()),
            behavior: CompleteBehavior::Error(Mutex::new(Some(error))),
            stream_events: Mutex::new(None),
            calls: AtomicUsize::new(0),
            captured: Mutex::new(Vec::new()),
        })
    }

    pub fn pending(control: PendingControl) -> Arc<Self> {
        Arc::new(Self {
            metadata: metadata(ModelCapabilities::new()),
            behavior: CompleteBehavior::Pending(control),
            stream_events: Mutex::new(None),
            calls: AtomicUsize::new(0),
            captured: Mutex::new(Vec::new()),
        })
    }

    pub fn with_stream(
        capabilities: ModelCapabilities,
        events: Vec<Result<ChatStreamEvent, ModelError>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            metadata: metadata(capabilities),
            behavior: CompleteBehavior::Fixed(
                ChatResponse::new(AssistantMessage::text("complete"), FinishReason::Stop)
                    .with_model(model_id()),
            ),
            stream_events: Mutex::new(Some(events)),
            calls: AtomicUsize::new(0),
            captured: Mutex::new(Vec::new()),
        })
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn captured(&self) -> Vec<ChatRequest> {
        self.captured
            .lock()
            .expect("capture lock is not poisoned")
            .clone()
    }

    fn record(&self, request: ChatRequest) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.captured
            .lock()
            .expect("capture lock is not poisoned")
            .push(request);
    }
}

pub fn facade(adapter: Arc<ScriptedModel>) -> ChatModel {
    let adapter: Arc<dyn ChatModelAdapter> = adapter;
    ChatModel::new(adapter).expect("scripted metadata is valid")
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ChatModelAdapter for ScriptedModel {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        self.record(request.into_inner());
        match &self.behavior {
            CompleteBehavior::Fixed(response) => Ok(response.clone()),
            CompleteBehavior::Error(error) => Err(error
                .lock()
                .expect("error lock is not poisoned")
                .take()
                .expect("scripted error is consumed once")),
            CompleteBehavior::Pending(control) => {
                let _drop_flag = DropFlag(Arc::clone(&control.dropped));
                control.started.notify_waiters();
                pending().await
            }
        }
    }

    async fn stream_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        self.record(request.into_inner());
        let events = self
            .stream_events
            .lock()
            .expect("stream lock is not poisoned")
            .take()
            .expect("scripted stream is consumed once");
        Ok(Box::pin(stream::iter(events)))
    }
}

pub fn metadata(capabilities: ModelCapabilities) -> ModelMetadata {
    ModelMetadata::new(
        ProviderId::new("mock-provider").expect("valid provider id"),
        model_id(),
        capabilities,
    )
}

pub fn model_id() -> ModelId {
    ModelId::new("mock-model").expect("valid model id")
}
