use super::{AgentStageId, BranchError, BranchInterrupted, BranchOutcome, BranchRunOutcome};
use crate::{AgentEventSink, AgentStreamEvent};
use futures_core::Stream;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::mpsc;
/// Stage events are provisional; only parent terminal events confirm saved outcomes.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum BranchStreamEvent {
    Stage {
        stage: AgentStageId,
        event: AgentStreamEvent,
    },
    Selection {
        from: AgentStageId,
        target: super::BranchTarget,
    },
    Completed(BranchOutcome),
    Interrupted(BranchInterrupted),
}
/// Synchronous transient observer; callbacks must be bounded and nonblocking.
pub trait BranchEventSink: Send + Sync {
    fn on_event(&self, event: &BranchStreamEvent);
}
impl<F: Fn(&BranchStreamEvent) + Send + Sync> BranchEventSink for F {
    fn on_event(&self, event: &BranchStreamEvent) {
        self(event)
    }
}
pub(crate) fn stage_sink(
    stage: AgentStageId,
    sink: Arc<dyn BranchEventSink>,
) -> Arc<dyn AgentEventSink> {
    Arc::new(move |event: &AgentStreamEvent| {
        if !matches!(
            event,
            AgentStreamEvent::Completed(_) | AgentStreamEvent::Interrupted(_)
        ) {
            sink.on_event(&BranchStreamEvent::Stage {
                stage: stage.clone(),
                event: event.clone(),
            });
        }
    })
}
pub(crate) fn terminal(outcome: &BranchRunOutcome, sink: &dyn BranchEventSink) {
    match outcome {
        BranchRunOutcome::Completed(result) => {
            sink.on_event(&BranchStreamEvent::Completed(result.clone()))
        }
        BranchRunOutcome::Interrupted(saved) => {
            sink.on_event(&BranchStreamEvent::Interrupted(saved.clone()))
        }
    }
}
/// A channel-backed sink that forwards events to an `BranchEventStream`.
pub(crate) struct ChannelEventSink {
    sender: mpsc::UnboundedSender<Result<BranchStreamEvent, BranchError>>,
}

impl ChannelEventSink {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Result<BranchStreamEvent, BranchError>>,
    ) -> Self {
        Self { sender }
    }
}

impl BranchEventSink for ChannelEventSink {
    fn on_event(&self, event: &BranchStreamEvent) {
        let _ = self.sender.send(Ok(event.clone()));
    }
}

type BranchInvocationFuture = Pin<Box<dyn Future<Output = Result<(), BranchError>> + Send>>;

/// An asynchronous stream of events from a running Agent invocation.
///
/// Dropping this stream drops the underlying invocation future, which
/// cancels model streaming and tool execution without leaving detached tasks.
pub struct BranchEventStream {
    receiver: mpsc::UnboundedReceiver<Result<BranchStreamEvent, BranchError>>,
    invocation: Option<BranchInvocationFuture>,
    pending_error: Option<BranchError>,
    is_terminal: bool,
}

impl BranchEventStream {
    pub(crate) fn new(
        receiver: mpsc::UnboundedReceiver<Result<BranchStreamEvent, BranchError>>,
        invocation: BranchInvocationFuture,
    ) -> Self {
        Self {
            receiver,
            invocation: Some(invocation),
            pending_error: None,
            is_terminal: false,
        }
    }
}

impl Stream for BranchEventStream {
    type Item = Result<BranchStreamEvent, BranchError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if this.is_terminal {
            return Poll::Ready(None);
        }

        // 1. Return any buffered events first.
        if let Poll::Ready(Some(item)) = this.receiver.poll_recv(cx) {
            return Poll::Ready(Some(item));
        }

        // 2. If the invocation future is still active, drive it.
        if let Some(mut invocation) = this.invocation.take() {
            match invocation.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // The invocation already dispatched its terminal outcome event.
                }
                Poll::Ready(Err(error)) => {
                    this.pending_error = Some(error);
                }
                Poll::Pending => {
                    this.invocation = Some(invocation);
                    if let Poll::Ready(Some(item)) = this.receiver.poll_recv(cx) {
                        return Poll::Ready(Some(item));
                    }
                    return Poll::Pending;
                }
            }
        }

        // 3. Check if events were buffered into receiver right before invocation completed.
        if let Poll::Ready(Some(item)) = this.receiver.poll_recv(cx) {
            return Poll::Ready(Some(item));
        }

        // 4. Emit terminal error if one occurred, fusing the stream.
        if let Some(error) = this.pending_error.take() {
            this.is_terminal = true;
            this.receiver.close();
            return Poll::Ready(Some(Err(error)));
        }

        // 5. If invocation is completed and receiver is drained, stream is finished.
        this.is_terminal = true;
        this.receiver.close();
        Poll::Ready(None)
    }
}

impl fmt::Debug for BranchEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BranchEventStream")
            .field(
                "is_active",
                &(!self.is_terminal && self.invocation.is_some()),
            )
            .finish()
    }
}
