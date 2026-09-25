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
async fn cancellation_and_timeout_after_raw_finished_do_not_commit_or_complete() {
    use group_agent_core::{
        CheckpointConfig, CheckpointPolicy, Checkpointer, EventConfig, InMemoryCheckpointer,
        RunControl, ThreadId,
    };
    use group_agent_prebuilt::{
        AgentConfig, AgentSnapshotCodec, AgentStreamEvent, ToolCallingAgent,
    };
    use group_agent_tool::{ToolRegistry, ToolRuntime};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    for cancel in [false, true] {
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
            fail: false,
        })
        .unwrap();
        let output = StructuredOutput::new("empty", serde_json::json!({"type":"object","properties":{},"required":[],"additionalProperties":false})).unwrap();
        let agent = ToolCallingAgent::new_with_output(
            model,
            ToolRuntime::new(ToolRegistry::builder().build()),
            AgentConfig::default(),
            output,
        )
        .unwrap();
        let token = CancellationToken::new();
        let control = if cancel {
            RunControl::new().with_cancellation_token(token.clone())
        } else {
            RunControl::new().with_node_timeout(Duration::from_millis(100))
        };
        let store = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
        let stream = agent.stream_with_checkpoint_control(
            vec![Message::user("question")],
            EventConfig::default(),
            control,
            CheckpointConfig::new("pending", store.clone(), CheckpointPolicy::EverySuperstep),
        );
        let (events, ()) = tokio::join!(stream.collect::<Vec<_>>(), async {
            pending.notified().await;
            if cancel {
                token.cancel();
            }
        });
        assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Ok(AgentStreamEvent::Completed(_))))
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert!(
            store
                .latest(&ThreadId::from("pending"))
                .await
                .unwrap()
                .is_none()
        );
    }
}
