use super::{
    AgentStageId, SequenceError, SequenceInterrupted, SequenceOutcome, SequenceRunOutcome,
};
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
pub enum SequenceStreamEvent {
    Stage {
        stage: AgentStageId,
        event: AgentStreamEvent,
    },
    Handoff {
        from: AgentStageId,
        to: AgentStageId,
    },
    Completed(SequenceOutcome),
    Interrupted(SequenceInterrupted),
}
/// Synchronous transient observer; callbacks must be bounded and nonblocking.
pub trait SequenceEventSink: Send + Sync {
    fn on_event(&self, event: &SequenceStreamEvent);
}
impl<F: Fn(&SequenceStreamEvent) + Send + Sync> SequenceEventSink for F {
    fn on_event(&self, event: &SequenceStreamEvent) {
        self(event)
    }
}
pub(crate) fn stage_sink(
    stage: AgentStageId,
    sink: Arc<dyn SequenceEventSink>,
) -> Arc<dyn AgentEventSink> {
    Arc::new(move |event: &AgentStreamEvent| {
        if !matches!(
            event,
            AgentStreamEvent::Completed(_) | AgentStreamEvent::Interrupted(_)
        ) {
            sink.on_event(&SequenceStreamEvent::Stage {
                stage: stage.clone(),
                event: event.clone(),
            });
        }
    })
}
pub(crate) fn terminal(outcome: &SequenceRunOutcome, sink: &dyn SequenceEventSink) {
    match outcome {
        SequenceRunOutcome::Completed(result) => {
            sink.on_event(&SequenceStreamEvent::Completed(result.clone()))
        }
        SequenceRunOutcome::Interrupted(saved) => {
            sink.on_event(&SequenceStreamEvent::Interrupted(saved.clone()))
        }
    }
}
/// A channel-backed sink that forwards events to an `SequenceEventStream`.
pub(crate) struct ChannelEventSink {
    sender: mpsc::UnboundedSender<Result<SequenceStreamEvent, SequenceError>>,
}

impl ChannelEventSink {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Result<SequenceStreamEvent, SequenceError>>,
    ) -> Self {
        Self { sender }
    }
}

impl SequenceEventSink for ChannelEventSink {
    fn on_event(&self, event: &SequenceStreamEvent) {
        let _ = self.sender.send(Ok(event.clone()));
    }
}

type SequenceInvocationFuture = Pin<Box<dyn Future<Output = Result<(), SequenceError>> + Send>>;

/// An asynchronous stream of events from a running Agent invocation.
///
/// Dropping this stream drops the underlying invocation future, which
/// cancels model streaming and tool execution without leaving detached tasks.
pub struct SequenceEventStream {
    receiver: mpsc::UnboundedReceiver<Result<SequenceStreamEvent, SequenceError>>,
    invocation: Option<SequenceInvocationFuture>,
    pending_error: Option<SequenceError>,
    is_terminal: bool,
}

impl SequenceEventStream {
    pub(crate) fn new(
        receiver: mpsc::UnboundedReceiver<Result<SequenceStreamEvent, SequenceError>>,
        invocation: SequenceInvocationFuture,
    ) -> Self {
        Self {
            receiver,
            invocation: Some(invocation),
            pending_error: None,
            is_terminal: false,
        }
    }
}

impl Stream for SequenceEventStream {
    type Item = Result<SequenceStreamEvent, SequenceError>;

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

impl fmt::Debug for SequenceEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SequenceEventStream")
            .field(
                "is_active",
                &(!self.is_terminal && self.invocation.is_some()),
            )
            .finish()
    }
}
