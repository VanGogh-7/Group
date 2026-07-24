//! Bounded, non-blocking Tokio streams for Group lifecycle events.
//!
//! This adapter bridges Core's synchronous [`EventSink`] callback to Tokio's
//! mature broadcast channel. It is intentionally lossy for slow subscribers:
//! lag is reported explicitly and no durable or reliable delivery is implied.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use group_agent_core::{EventSink, GraphEvent};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Invalid event-broadcast configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum EventBroadcastConfigError {
    /// Tokio broadcast channels require at least one retained slot.
    #[error("event broadcast capacity must be greater than zero")]
    ZeroCapacity,
    /// The requested capacity cannot be represented by Tokio's ring buffer.
    #[error("event broadcast capacity {requested} is too large")]
    CapacityTooLarge { requested: usize },
}

/// A recoverable condition observed while consuming an event stream.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum EventStreamError {
    /// This subscriber fell behind and the oldest events were overwritten.
    #[error("event stream lagged and skipped {skipped} events")]
    Lagged { skipped: u64 },
}

/// Factory for a shared bounded broadcast sink and independent subscribers.
///
/// Tokio broadcast stores events once in a shared bounded ring buffer. The
/// effective capacity is the smallest supported power of two at least as large
/// as the requested capacity and is available through [`Self::capacity`]. A
/// subscriber starts at the point [`Self::subscribe`] is called and receives no
/// earlier event. Dropping this value does not close streams while a sink clone
/// remains alive; streams end after every sender is dropped and buffered events
/// are drained.
#[derive(Clone)]
pub struct EventBroadcast {
    sender: broadcast::Sender<GraphEvent>,
    capacity: usize,
}

impl EventBroadcast {
    /// Creates a bounded event broadcast.
    ///
    /// Capacity zero and values too large for Tokio's shared ring buffer are
    /// rejected rather than delegated to Tokio's panicking constructor.
    pub fn new(capacity: usize) -> Result<Self, EventBroadcastConfigError> {
        if capacity == 0 {
            return Err(EventBroadcastConfigError::ZeroCapacity);
        }
        let effective_capacity = capacity
            .checked_next_power_of_two()
            .filter(|capacity| *capacity <= usize::MAX >> 1)
            .ok_or(EventBroadcastConfigError::CapacityTooLarge {
                requested: capacity,
            })?;
        let (sender, _) = broadcast::channel(effective_capacity);
        Ok(Self {
            sender,
            capacity: effective_capacity,
        })
    }

    /// Creates a synchronous, non-blocking Core event sink.
    ///
    /// Each callback clones only the lightweight [`GraphEvent`] needed to move
    /// it into the channel. Sending never waits for subscribers. No subscriber,
    /// or receivers that have all been dropped, is treated as successful event
    /// disposal and cannot fail graph execution.
    #[must_use]
    pub fn sink(&self) -> Arc<dyn EventSink> {
        Arc::new(BroadcastSink {
            sender: self.sender.clone(),
        })
    }

    /// Subscribes to events emitted after this call.
    ///
    /// Subscribers have independent cursors. If this subscriber is slower than
    /// the bounded buffer, its next item is [`EventStreamError::Lagged`] with
    /// the exact number of overwritten events, after which newer events remain
    /// readable.
    #[must_use]
    pub fn subscribe(&self) -> EventStream {
        EventStream {
            inner: BroadcastStream::new(self.sender.subscribe()),
        }
    }

    /// Returns the effective retained-event capacity of the shared ring buffer.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current number of live receivers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl fmt::Debug for EventBroadcast {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventBroadcast")
            .field("capacity", &self.capacity())
            .field("subscriber_count", &self.subscriber_count())
            .finish()
    }
}

struct BroadcastSink {
    sender: broadcast::Sender<GraphEvent>,
}

impl EventSink for BroadcastSink {
    fn on_event(&self, event: &GraphEvent) {
        let _ = self.sender.send(event.clone());
    }
}

/// An asynchronous stream of events from one broadcast subscription.
///
/// `None` means every sender was dropped and all buffered events were consumed.
/// Lag is an item-level error, not stream termination. Creating a new stream
/// does not replay events that preceded its subscription.
pub struct EventStream {
    inner: BroadcastStream<GraphEvent>,
}

impl Stream for EventStream {
    type Item = Result<GraphEvent, EventStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(context) {
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                Poll::Ready(Some(Err(EventStreamError::Lagged { skipped })))
            }
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for EventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventStream")
            .finish_non_exhaustive()
    }
}
