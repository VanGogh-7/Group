#![cfg(feature = "structured-output")]
use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use group_agent_model::*;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::Notify;
struct Raw {
    step: usize,
    dropped: Arc<AtomicBool>,
    pending: Arc<Notify>,
    fail: bool,
}
impl Drop for Raw {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl Stream for Raw {
    type Item = Result<ChatStreamEvent, ModelError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.step += 1;
        match self.step {
            1 => Poll::Ready(Some(Ok(ChatStreamEvent::TextDelta("{}".into())))),
            2 => Poll::Ready(Some(Ok(ChatStreamEvent::Finished(FinishReason::Stop)))),
            _ if self.fail => Poll::Ready(Some(Err(ModelError::new(
                ModelErrorKind::ProviderUnavailable,
                "offline transport",
            )))),
            _ => {
                self.pending.notify_one();
                Poll::Pending
            }
        }
    }
}
struct Adapter {
    meta: ModelMetadata,
    dropped: Arc<AtomicBool>,
    pending: Arc<Notify>,
    fail: bool,
}
#[async_trait]
impl ChatModelAdapter for Adapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.meta
    }
    async fn complete_raw(&self, _: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
        unreachable!()
    }
    async fn stream_raw(&self, _: ValidatedChatRequest) -> Result<ChatEventStream, ModelError> {
        Ok(Box::pin(Raw {
            step: 0,
            dropped: self.dropped.clone(),
            pending: self.pending.clone(),
            fail: self.fail,
        }))
    }
}
#[tokio::test]
async fn pending_after_finished_is_not_success_and_drop_releases_raw_stream() {
    for fail in [false, true] {
        let dropped = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(Notify::new());
        let model = ChatModel::from_adapter(Adapter {
            meta: ModelMetadata::new(
                ProviderId::new("fake").unwrap(),
                ModelId::new("fake").unwrap(),
                ModelCapabilities::new()
                    .with_streaming(true)
                    .with_structured_output(true),
            ),
            dropped: dropped.clone(),
            pending: pending.clone(),
            fail,
        })
        .unwrap();
        let output=StructuredOutput::new("empty",serde_json::json!({"type":"object","properties":{},"required":[],"additionalProperties":false})).unwrap();
        let mut stream = model
            .stream(ChatRequest::new(vec![Message::user("q")]).with_structured_output(output))
            .await
            .unwrap();
        assert!(matches!(
            stream.next().await,
            Some(Ok(ChatStreamEvent::TextDelta(_)))
        ));
        if fail {
            assert!(
                matches!(stream.next().await,Some(Err(e)) if e.kind()==&ModelErrorKind::ProviderUnavailable)
            );
            assert!(dropped.load(Ordering::SeqCst));
            assert!(stream.next().await.is_none());
        } else {
            tokio::select! {
                value=stream.next()=>panic!("premature completion {value:?}"),
                ()=pending.notified()=>{}
            }
            assert!(!dropped.load(Ordering::SeqCst));
            drop(stream);
            assert!(dropped.load(Ordering::SeqCst));
        }
    }
}
