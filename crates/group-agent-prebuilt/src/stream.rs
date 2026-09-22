use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use group_agent_model::{ToolCallDelta, ToolCallId, ToolName};
use tokio::sync::mpsc;

use crate::AgentInterrupted;
use crate::agent::AgentOutcome;
use crate::approval::AgentApprovalRequest;
use crate::error::AgentError;

/// A provider-neutral streaming or lifecycle event emitted during an Agent invocation.
#[derive(Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentStreamEvent {
    /// A model reasoning round began.
    ModelStarted {
        /// The one-based model round.
        round: usize,
    },
    /// Incremental assistant text produced by the model.
    TextDelta {
        /// The one-based model round producing this fragment.
        round: usize,
        /// The streamed text fragment.
        delta: String,
    },
    /// Incremental tool call fragment produced by the model.
    ToolCallDelta {
        /// The one-based model round producing this fragment.
        round: usize,
        /// The streamed tool call fragment.
        delta: ToolCallDelta,
    },
    /// A model round finished producing its response.
    ModelCompleted {
        /// The one-based model round that finished.
        round: usize,
    },
    /// Tool execution requires approval before side effects occur.
    ///
    /// This is a provisional notification emitted before checkpoint saving.
    /// Only [`Self::Interrupted`] confirms a durable, resumable suspension.
    ApprovalRequired {
        /// The pending tool calls requiring an approval decision.
        request: AgentApprovalRequest,
    },
    /// Execution began for one tool call.
    ToolStarted {
        /// The call identifier.
        id: ToolCallId,
        /// The tool name.
        name: ToolName,
        /// The one-based model round that requested this tool.
        round: usize,
    },
    /// A started tool call returned a business result, failed, or timed out.
    ///
    /// Validation failures and observer failures that prevent execution do not
    /// emit this event. Dropping or cancelling the enclosing graph can drop a
    /// pending call without a terminal Tool event. Infrastructure failures also
    /// terminate the invocation with a typed [`AgentError`]; a business error
    /// alone does not terminate the invocation.
    ToolCompleted {
        /// The call identifier.
        id: ToolCallId,
        /// The tool name.
        name: ToolName,
        /// The one-based model round that requested this tool.
        round: usize,
        /// Whether the tool execution resulted in a business or execution error.
        is_error: bool,
    },
    /// The agent finished normally with its complete outcome.
    Completed(AgentOutcome),
    /// The run durably suspended and its interrupt checkpoint was saved.
    ///
    /// This terminal event ends the stream. Start a new streaming or ordinary
    /// Resume with an explicit approval decision to continue. No earlier
    /// token events are replayed by Resume.
    Interrupted(AgentInterrupted),
}

impl fmt::Debug for AgentStreamEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModelStarted { round } => formatter
                .debug_struct("ModelStarted")
                .field("round", round)
                .finish(),
            Self::TextDelta { round, delta } => formatter
                .debug_struct("TextDelta")
                .field("round", round)
                .field("bytes", &delta.len())
                .field("chars", &delta.chars().count())
                .finish(),
            Self::ToolCallDelta { round, delta } => formatter
                .debug_struct("ToolCallDelta")
                .field("round", round)
                .field("delta", delta)
                .finish(),
            Self::ModelCompleted { round } => formatter
                .debug_struct("ModelCompleted")
                .field("round", round)
                .finish(),
            Self::ApprovalRequired { request } => formatter
                .debug_struct("ApprovalRequired")
                .field("pending_calls", &request.pending_calls().len())
                .finish(),
            Self::ToolStarted { id, name, round } => formatter
                .debug_struct("ToolStarted")
                .field("id", id)
                .field("name", name)
                .field("round", round)
                .finish(),
            Self::ToolCompleted {
                id,
                name,
                round,
                is_error,
            } => formatter
                .debug_struct("ToolCompleted")
                .field("id", id)
                .field("name", name)
                .field("round", round)
                .field("is_error", is_error)
                .finish(),
            Self::Completed(outcome) => formatter.debug_tuple("Completed").field(outcome).finish(),
            Self::Interrupted(outcome) => {
                formatter.debug_tuple("Interrupted").field(outcome).finish()
            }
        }
    }
}

/// Receives lifecycle and streaming events synchronously as they occur.
///
/// Callbacks run inline and must remain lightweight. Sinks receive typed
/// events with payload access on `TextDelta` and `ToolCallDelta`, but default
/// `Debug` formatting remains payload-safe.
pub trait AgentEventSink: Send + Sync {
    /// Observes one emitted event.
    fn on_event(&self, event: &AgentStreamEvent);
}

impl<F> AgentEventSink for F
where
    F: Fn(&AgentStreamEvent) + Send + Sync,
{
    fn on_event(&self, event: &AgentStreamEvent) {
        self(event);
    }
}

/// A channel-backed sink that forwards events to an `AgentEventStream`.
pub(crate) struct ChannelEventSink {
    sender: mpsc::UnboundedSender<Result<AgentStreamEvent, AgentError>>,
}

impl ChannelEventSink {
    pub(crate) fn new(sender: mpsc::UnboundedSender<Result<AgentStreamEvent, AgentError>>) -> Self {
        Self { sender }
    }
}

impl AgentEventSink for ChannelEventSink {
    fn on_event(&self, event: &AgentStreamEvent) {
        let _ = self.sender.send(Ok(event.clone()));
    }
}

type AgentInvocationFuture = Pin<Box<dyn Future<Output = Result<(), AgentError>> + Send>>;

/// An asynchronous stream of events from a running Agent invocation.
///
/// Dropping this stream drops the underlying invocation future, which
/// cancels model streaming and tool execution without leaving detached tasks.
pub struct AgentEventStream {
    receiver: mpsc::UnboundedReceiver<Result<AgentStreamEvent, AgentError>>,
    invocation: Option<AgentInvocationFuture>,
    pending_error: Option<AgentError>,
    is_terminal: bool,
}

impl AgentEventStream {
    pub(crate) fn new(
        receiver: mpsc::UnboundedReceiver<Result<AgentStreamEvent, AgentError>>,
        invocation: AgentInvocationFuture,
    ) -> Self {
        Self {
            receiver,
            invocation: Some(invocation),
            pending_error: None,
            is_terminal: false,
        }
    }
}

impl Stream for AgentEventStream {
    type Item = Result<AgentStreamEvent, AgentError>;

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

impl fmt::Debug for AgentEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentEventStream")
            .field(
                "is_active",
                &(!self.is_terminal && self.invocation.is_some()),
            )
            .finish()
    }
}

/// Cooperatively yields execution back to the caller once.
pub(crate) struct YieldNow(pub(crate) bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use group_agent_model::{ToolCall, ToolCallDelta, ToolCallId, ToolName};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn stream_event_debug_redacts_text_and_arguments() {
        const SECRET_TEXT: &str = "SECRET_TOKEN_TEXT_12345";
        const SECRET_ARG: &str = "SECRET_TOOL_ARGUMENT_67890";

        let text_event = AgentStreamEvent::TextDelta {
            round: 1,
            delta: SECRET_TEXT.to_string(),
        };
        let rendered_text = format!("{text_event:?}");
        assert!(rendered_text.contains("TextDelta"));
        assert!(rendered_text.contains("bytes"));
        assert!(!rendered_text.contains(SECRET_TEXT));

        let delta = ToolCallDelta::new(0)
            .with_id(ToolCallId::new("call-1").unwrap())
            .with_name(ToolName::new("search").unwrap())
            .with_arguments_fragment(SECRET_ARG);
        let tool_event = AgentStreamEvent::ToolCallDelta { round: 1, delta };
        let rendered_tool = format!("{tool_event:?}");
        assert!(rendered_tool.contains("ToolCallDelta"));
        assert!(!rendered_tool.contains(SECRET_ARG));

        let approval_event = AgentStreamEvent::ApprovalRequired {
            request: AgentApprovalRequest::new(vec![ToolCall::new(
                ToolCallId::new("call-1").unwrap(),
                ToolName::new("dangerous_action").unwrap(),
                serde_json::json!({"arg": SECRET_ARG}),
            )]),
        };
        let rendered_approval = format!("{approval_event:?}");
        assert!(rendered_approval.contains("ApprovalRequired"));
        assert!(rendered_approval.contains("pending_calls: 1"));
        assert!(!rendered_approval.contains(SECRET_ARG));
    }

    #[test]
    fn sink_closure_receives_emitted_events() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink_events = events.clone();
        let sink = move |event: &AgentStreamEvent| {
            sink_events.lock().unwrap().push(event.clone());
        };

        sink.on_event(&AgentStreamEvent::ModelStarted { round: 1 });
        sink.on_event(&AgentStreamEvent::TextDelta {
            round: 1,
            delta: "hello".to_string(),
        });
        sink.on_event(&AgentStreamEvent::ModelCompleted { round: 1 });

        let captured = events.lock().unwrap().clone();
        assert_eq!(captured.len(), 3);
        match &captured[1] {
            AgentStreamEvent::TextDelta { round, delta } => {
                assert_eq!(*round, 1);
                assert_eq!(delta, "hello");
            }
            _ => panic!("unexpected event"),
        }
    }

    #[tokio::test]
    async fn stream_drops_invocation_future_on_drop() {
        struct DropGuard(Arc<AtomicBool>);
        impl Drop for DropGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropGuard(dropped.clone());

        let (_tx, rx) = mpsc::unbounded_channel();
        let invocation = Box::pin(async move {
            let _guard = guard;
            // Pending indefinitely
            std::future::pending::<Result<(), AgentError>>().await
        });

        let stream = AgentEventStream::new(rx, invocation);
        assert!(!dropped.load(Ordering::SeqCst));

        // Dropping stream immediately drops the invocation future
        drop(stream);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
