#![cfg(feature = "agent-sequence")]
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
    hang: bool,
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
            _ if !self.hang => Poll::Ready(None),
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
    hang: bool,
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
            hang: self.hang,
        }))
    }
}
#[tokio::test]
async fn cancel_timeout_and_drop_release_pending_stage_without_false_completion() {
    use group_agent_core::*;
    use group_agent_prebuilt::*;
    use group_agent_tool::{ToolRegistry, ToolRuntime};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    for active in 0..2 {
        for mode in 0..3 {
            let pending = Arc::new(Notify::new());
            let dropped = Arc::new(AtomicBool::new(false));
            let output=StructuredOutput::new("empty",serde_json::json!({"type":"object","properties":{},"required":[],"additionalProperties":false})).unwrap();
            let mut stages = Vec::new();
            for index in 0..2 {
                let model = ChatModel::from_adapter(Adapter {
                    meta: ModelMetadata::new(
                        ProviderId::new("fake").unwrap(),
                        ModelId::new("fake").unwrap(),
                        ModelCapabilities::new()
                            .with_streaming(true)
                            .with_structured_output(true),
                    ),
                    pending: pending.clone(),
                    dropped: if index == active {
                        dropped.clone()
                    } else {
                        Arc::new(AtomicBool::new(false))
                    },
                    hang: index == active,
                })
                .unwrap();
                stages.push(
                    AgentStage::new(
                        AgentStageId::new(format!("s{index}")).unwrap(),
                        model,
                        ToolRuntime::new(ToolRegistry::builder().build()),
                        AgentConfig::default(),
                        output.clone(),
                    )
                    .unwrap(),
                );
            }
            let second = stages.pop().unwrap();
            let first = stages.pop().unwrap();
            let seq = AgentSequence::new(
                "v1",
                first,
                second,
                Arc::new(
                    |_: &ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> {
                        Ok(vec![Message::user("mapped")])
                    },
                ),
            )
            .unwrap();
            let token = CancellationToken::new();
            let control = if mode == 1 {
                RunControl::new().with_node_timeout(Duration::from_millis(100))
            } else {
                RunControl::new().with_cancellation_token(token.clone())
            };
            let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
            let mut stream = seq.stream_with_checkpoint_control(
                vec![Message::user("q")],
                EventConfig::default(),
                control,
                CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
            );
            loop {
                tokio::select! {biased;()=pending.notified()=>break,e=stream.next()=>assert!(matches!(e,Some(Ok(SequenceStreamEvent::Stage{..}|SequenceStreamEvent::Handoff{..}))))}
            }
            if mode == 2 {
                drop(stream);
            } else {
                if mode == 0 {
                    token.cancel();
                }
                let remaining = stream.collect::<Vec<_>>().await;
                assert_eq!(remaining.iter().filter(|e| e.is_err()).count(), 1);
                assert!(
                    !remaining
                        .iter()
                        .any(|e| matches!(e, Ok(SequenceStreamEvent::Completed(_))))
                );
            }
            assert!(dropped.load(Ordering::SeqCst));
            let head = store.latest(&ThreadId::from("s")).await.unwrap();
            if active == 0 {
                assert!(head.is_none());
            } else {
                let head = head.unwrap();
                assert_eq!(head.superstep(), 2);
                assert!(!head.completed());
            }
        }
    }
}
