use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy,
    CheckpointRequest, CheckpointState, CheckpointWriteError, Checkpointer, CheckpointerError,
    CodecDescriptor, END, EncodedValue, EventConfig, EventRetention, ForkConfig, GraphEvent,
    GraphState, InMemoryCheckpointer, InterruptPayload, InterruptibleNode, Node, NodeContext,
    NodeError, NodeOutcome, ReplayConfig, ResumeConfig, RouteError, RunConfig, RunControl,
    RunFailure, RunId, START, SnapshotError, StateError, StateGraph, ThreadId,
};
use group_agent_observability_tokio::{
    EventBroadcast, EventBroadcastConfigError, EventStream, EventStreamError,
};
use tokio::sync::Barrier;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct State {
    value: usize,
}

impl GraphState for State {
    type Update = usize;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }
}

impl CheckpointState for State {
    type Snapshot = usize;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(self.value)
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self { value: *snapshot })
    }
}

struct Codec;

impl CheckpointCodec<usize> for Codec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.tests.stream.snapshot", 1, "raw-usize-le")
    }

    fn encode_snapshot(&self, snapshot: &usize) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<usize, CheckpointCodecError> {
        bytes
            .try_into()
            .map(usize::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid stream snapshot"))
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        payload
            .downcast_ref::<()>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        Ok(EncodedValue::new(
            CodecDescriptor::new("group.tests.stream.interrupt", 1, "raw-usize-le"),
            Vec::<u8>::new(),
        ))
    }

    fn decode_interrupt(
        &self,
        _value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        Ok(InterruptPayload::new(()))
    }
}

struct Add;

#[async_trait]
impl Node<State> for Add {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<usize, NodeError> {
        Ok(1)
    }
}

struct Fail;

#[async_trait]
impl Node<State> for Fail {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<usize, NodeError> {
        Err(NodeError::message("stream node failure"))
    }
}

struct Pending;

#[async_trait]
impl Node<State> for Pending {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<usize, NodeError> {
        std::future::pending().await
    }
}

struct WaitAt(Arc<Barrier>);

#[async_trait]
impl Node<State> for WaitAt {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<usize, NodeError> {
        self.0.wait().await;
        Ok(1)
    }
}

struct Interrupt;

#[async_trait]
impl InterruptibleNode<State> for Interrupt {
    async fn run(
        &self,
        _state: &State,
        _context: &NodeContext,
    ) -> Result<NodeOutcome<usize>, NodeError> {
        Ok(NodeOutcome::interrupt(()))
    }
}

fn fixed_graph<N>(node: N) -> group_agent_core::CompiledGraph<State>
where
    N: Node<State> + 'static,
{
    let mut graph = StateGraph::new();
    graph.add_node("node", node).expect("node");
    graph.add_edge(START, "node").add_edge("node", END);
    graph.compile().expect("graph")
}

fn event_config(broadcast: &EventBroadcast, retention: EventRetention) -> EventConfig {
    EventConfig::new(retention).with_sink(broadcast.sink())
}

async fn receive(stream: &mut EventStream, count: usize) -> Vec<GraphEvent> {
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        events.push(
            stream
                .next()
                .await
                .expect("stream remains open")
                .expect("subscriber should not lag"),
        );
    }
    events
}

#[tokio::test]
async fn replay_started_and_replay_failure_are_delivered_through_the_stream() {
    let mut builder = StateGraph::new();
    builder.set_version("stream-replay-v1");
    builder.add_node("one", Add).expect("one");
    builder.add_node("two", Add).expect("two");
    builder
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    let graph = builder.compile().expect("graph");
    let checkpointer = Arc::new(InMemoryCheckpointer::new(Codec));
    graph
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "stream-replay",
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("seed stops after one node");
    let source = checkpointer
        .latest(&ThreadId::from("stream-replay"))
        .await
        .expect("latest")
        .expect("checkpoint")
        .id();

    let broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut success_stream = broadcast.subscribe();
    let report = graph
        .replay(
            ReplayConfig::new(
                "stream-replay",
                source,
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<usize>>,
            )
            .with_event_config(event_config(&broadcast, EventRetention::None))
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("replay");
    let events = receive(&mut success_stream, 6).await;
    assert_eq!(
        events.iter().map(GraphEvent::run_id).collect::<Vec<_>>(),
        vec![report.run_id(); 6]
    );
    assert!(matches!(events[1], GraphEvent::ReplayStarted { .. }));
    assert!(matches!(events[5], GraphEvent::RunCompleted { .. }));

    let mut failure_stream = broadcast.subscribe();
    let _error = graph
        .replay(
            ReplayConfig::new(
                "stream-replay",
                source,
                checkpointer as Arc<dyn Checkpointer<usize>>,
            )
            .with_event_config(event_config(&broadcast, EventRetention::None))
            .with_run_config(RunConfig::new(0)),
        )
        .await
        .expect_err("zero replay budget");
    let events = receive(&mut failure_stream, 3).await;
    let failed_run_id = events[0].run_id();
    assert_eq!(
        events.iter().map(GraphEvent::run_id).collect::<Vec<_>>(),
        vec![failed_run_id; 3]
    );
    assert!(matches!(events[1], GraphEvent::ReplayStarted { .. }));
    assert!(matches!(
        events[2],
        GraphEvent::RunFailed {
            failure: RunFailure::MaxStepsExceeded { .. },
            ..
        }
    ));
}

#[tokio::test]
async fn fork_branch_resume_and_fork_failure_are_delivered_through_the_stream() {
    let mut builder = StateGraph::new();
    builder.set_version("stream-fork-v1");
    builder.add_node("one", Add).expect("one");
    builder.add_node("two", Add).expect("two");
    builder
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    let graph = builder.compile().expect("graph");
    let checkpointer = Arc::new(InMemoryCheckpointer::new(Codec));
    graph
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "stream-fork",
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("seed stops after one node");
    let source = checkpointer
        .latest(&ThreadId::from("stream-fork"))
        .await
        .expect("latest")
        .expect("source")
        .id();

    let broadcast = EventBroadcast::new(32).expect("broadcast");
    let mut success_stream = broadcast.subscribe();
    let fork = graph
        .fork(
            ForkConfig::new(
                "stream-fork",
                source,
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<usize>>,
            )
            .with_event_config(event_config(&broadcast, EventRetention::None))
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("fork");
    let fork_events = receive(&mut success_stream, 7).await;
    assert!(matches!(
        fork_events[1],
        GraphEvent::ForkStarted {
            branch_id,
            ..
        } if branch_id == fork.branch_id()
    ));
    assert!(matches!(
        fork_events.last(),
        Some(GraphEvent::RunCompleted { .. })
    ));

    let mut resume_stream = broadcast.subscribe();
    let resumed = graph
        .resume(
            ResumeConfig::new(
                "stream-fork",
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<usize>>,
            )
            .with_branch_id(fork.branch_id())
            .with_event_config(event_config(&broadcast, EventRetention::None)),
        )
        .await
        .expect("completed branch resume");
    let resume_events = receive(&mut resume_stream, 4).await;
    assert_eq!(
        resume_events
            .iter()
            .map(GraphEvent::run_id)
            .collect::<Vec<_>>(),
        vec![resumed.run_id(); 4]
    );
    assert!(matches!(
        resume_events[2],
        GraphEvent::BranchResumed {
            branch_id,
            ..
        } if branch_id == fork.branch_id()
    ));

    let mut resume_failure_stream = broadcast.subscribe();
    let _error = graph
        .resume(
            ResumeConfig::new(
                "stream-fork",
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<usize>>,
            )
            .with_branch_id(fork.branch_id())
            .with_resume_value(())
            .with_event_config(event_config(&broadcast, EventRetention::None)),
        )
        .await
        .expect_err("completed branch rejects a resume value");
    let resume_failure_events = receive(&mut resume_failure_stream, 2).await;
    assert!(matches!(
        resume_failure_events.as_slice(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::RunFailed {
                failure: RunFailure::UnexpectedResumeValue { .. },
                ..
            }
        ]
    ));
    assert!(
        !resume_failure_events
            .iter()
            .any(|event| matches!(event, GraphEvent::BranchResumed { .. }))
    );

    let mut failure_stream = broadcast.subscribe();
    let _error = graph
        .fork(
            ForkConfig::new(
                "stream-fork",
                source,
                checkpointer as Arc<dyn Checkpointer<usize>>,
            )
            .with_event_config(event_config(&broadcast, EventRetention::None))
            .with_run_config(RunConfig::new(0)),
        )
        .await
        .expect_err("zero-budget fork");
    let failure_events = receive(&mut failure_stream, 3).await;
    assert!(matches!(failure_events[1], GraphEvent::ForkStarted { .. }));
    assert!(matches!(
        failure_events[2],
        GraphEvent::RunFailed {
            failure: RunFailure::MaxStepsExceeded { .. },
            ..
        }
    ));
}

fn kinds(events: &[GraphEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|event| match event {
            GraphEvent::RunStarted { .. } => "run_started",
            GraphEvent::NodeStarted { .. } => "node_started",
            GraphEvent::NodeInterrupted { .. } => "node_interrupted",
            GraphEvent::NodeCompleted { .. } => "node_completed",
            GraphEvent::StateUpdated { .. } => "state_updated",
            GraphEvent::SubgraphStarted { .. } => "subgraph_started",
            GraphEvent::SubgraphCompleted { .. } => "subgraph_completed",
            GraphEvent::CheckpointSaved { .. } => "checkpoint_saved",
            GraphEvent::RunInterrupted { .. } => "run_interrupted",
            GraphEvent::RunCompleted { .. } => "run_completed",
            GraphEvent::RunFailed { .. } => "run_failed",
            _ => "other",
        })
        .collect()
}

#[tokio::test]
async fn single_subscriber_receives_the_complete_success_sequence() {
    let broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut stream = broadcast.subscribe();
    let report = fixed_graph(Add)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&broadcast, EventRetention::All),
        )
        .await
        .expect("run");
    let events = receive(&mut stream, report.events().len()).await;

    assert_eq!(events, report.events());
    assert_eq!(
        kinds(&events),
        [
            "run_started",
            "node_started",
            "node_completed",
            "state_updated",
            "run_completed"
        ]
    );
}

#[tokio::test]
async fn retention_none_still_delivers_every_event_to_the_stream() {
    let broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut stream = broadcast.subscribe();
    let report = fixed_graph(Add)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&broadcast, EventRetention::None),
        )
        .await
        .expect("run");

    assert!(report.events().is_empty());
    assert_eq!(receive(&mut stream, 5).await.len(), 5);
}

#[tokio::test]
async fn subscribers_are_independent_and_begin_at_subscription_time() {
    let broadcast = EventBroadcast::new(32).expect("broadcast");
    let mut first = broadcast.subscribe();
    let first_report = fixed_graph(Add)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&broadcast, EventRetention::None),
        )
        .await
        .expect("first run");
    let mut late = broadcast.subscribe();
    let second_report = fixed_graph(Add)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&broadcast, EventRetention::None),
        )
        .await
        .expect("second run");

    let first_events = receive(&mut first, 10).await;
    let late_events = receive(&mut late, 5).await;
    assert!(
        first_events[..5]
            .iter()
            .all(|event| event.run_id() == first_report.run_id())
    );
    assert!(
        first_events[5..]
            .iter()
            .all(|event| event.run_id() == second_report.run_id())
    );
    assert!(
        late_events
            .iter()
            .all(|event| event.run_id() == second_report.run_id())
    );
}

#[tokio::test]
async fn four_subscribers_receive_identical_events() {
    let broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut streams = (0..4).map(|_| broadcast.subscribe()).collect::<Vec<_>>();
    fixed_graph(Add)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&broadcast, EventRetention::None),
        )
        .await
        .expect("run");

    let expected = receive(&mut streams[0], 5).await;
    for stream in &mut streams[1..] {
        assert_eq!(receive(stream, 5).await, expected);
    }
}

#[tokio::test]
async fn slow_subscriber_gets_the_exact_lag_count_and_can_continue() {
    let broadcast = EventBroadcast::new(2).expect("broadcast");
    let mut stream = broadcast.subscribe();
    let sink = broadcast.sink();
    let run_id = RunId::new();
    for max_steps in 0..5 {
        sink.on_event(&GraphEvent::RunStarted { run_id, max_steps });
    }

    assert_eq!(
        stream.next().await.expect("lag item"),
        Err(EventStreamError::Lagged { skipped: 3 })
    );
    let retained = receive(&mut stream, 2).await;
    assert!(matches!(
        retained.as_slice(),
        [
            GraphEvent::RunStarted { max_steps: 3, .. },
            GraphEvent::RunStarted { max_steps: 4, .. }
        ]
    ));
}

#[tokio::test]
async fn stream_closes_only_after_all_senders_drop_and_buffer_is_drained() {
    let broadcast = EventBroadcast::new(4).expect("broadcast");
    let mut stream = broadcast.subscribe();
    let sink = broadcast.sink();
    let run_id = RunId::new();
    for max_steps in 1..=3 {
        sink.on_event(&GraphEvent::RunStarted { run_id, max_steps });
    }
    drop(sink);
    drop(broadcast);

    let drained = receive(&mut stream, 3).await;
    assert_eq!(
        drained
            .iter()
            .map(|event| match event {
                GraphEvent::RunStarted { max_steps, .. } => *max_steps,
                other => panic!("unexpected buffered event: {other:?}"),
            })
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn capacity_is_checked_and_reports_the_effective_ring_size() {
    assert_eq!(
        EventBroadcast::new(0).expect_err("zero capacity"),
        EventBroadcastConfigError::ZeroCapacity
    );
    assert_eq!(
        EventBroadcast::new(usize::MAX).expect_err("oversized capacity"),
        EventBroadcastConfigError::CapacityTooLarge {
            requested: usize::MAX,
        }
    );
    assert_eq!(
        EventBroadcast::new(3)
            .expect("capacity should round safely")
            .capacity(),
        4
    );
}

#[tokio::test]
async fn absent_or_dropped_subscribers_do_not_fail_runs() {
    let no_subscribers = EventBroadcast::new(4).expect("broadcast");
    fixed_graph(Add)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&no_subscribers, EventRetention::None),
        )
        .await
        .expect("no subscribers must not fail");

    let dropped = EventBroadcast::new(4).expect("broadcast");
    drop(dropped.subscribe());
    fixed_graph(Add)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&dropped, EventRetention::None),
        )
        .await
        .expect("dropped subscriber must not fail");
}

#[tokio::test]
async fn node_and_route_failures_stream_partial_events_then_run_failed() {
    let node_broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut node_stream = node_broadcast.subscribe();
    fixed_graph(Fail)
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&node_broadcast, EventRetention::None),
        )
        .await
        .expect_err("node failure");
    let node_events = receive(&mut node_stream, 3).await;
    assert_eq!(
        kinds(&node_events),
        ["run_started", "node_started", "run_failed"]
    );

    let mut route_graph = StateGraph::new();
    route_graph.add_node("node", Add).expect("node");
    route_graph.add_edge(START, "node");
    route_graph
        .add_conditional_edges("node", [END], |_| Err(RouteError::message("route failure")))
        .expect("router");
    let route_broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut route_stream = route_broadcast.subscribe();
    route_graph
        .compile()
        .expect("graph")
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&route_broadcast, EventRetention::None),
        )
        .await
        .expect_err("route failure");
    let route_events = receive(&mut route_stream, 5).await;
    assert_eq!(
        kinds(&route_events),
        [
            "run_started",
            "node_started",
            "node_completed",
            "state_updated",
            "run_failed"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn timeout_and_cancellation_failures_are_streamed() {
    let timeout_broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut timeout_stream = timeout_broadcast.subscribe();
    fixed_graph(Pending)
        .invoke_with_control(
            State::default(),
            RunConfig::default(),
            event_config(&timeout_broadcast, EventRetention::None),
            RunControl::new().with_node_timeout(Duration::from_secs(1)),
        )
        .await
        .expect_err("timeout");
    let timeout_events = receive(&mut timeout_stream, 3).await;
    assert!(matches!(
        timeout_events.last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::NodeTimedOut { .. },
            ..
        })
    ));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancel_broadcast = EventBroadcast::new(8).expect("broadcast");
    let mut cancel_stream = cancel_broadcast.subscribe();
    fixed_graph(Add)
        .invoke_with_control(
            State::default(),
            RunConfig::default(),
            event_config(&cancel_broadcast, EventRetention::None),
            RunControl::new().with_cancellation_token(cancellation),
        )
        .await
        .expect_err("cancelled");
    let cancelled = receive(&mut cancel_stream, 2).await;
    assert!(matches!(
        cancelled.last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::Cancelled { .. },
            ..
        })
    ));
}

struct FailingCheckpointer;

#[async_trait]
impl Checkpointer<usize> for FailingCheckpointer {
    async fn save(
        &self,
        _request: CheckpointRequest<usize>,
    ) -> Result<Arc<Checkpoint<usize>>, CheckpointWriteError> {
        Err(CheckpointWriteError::Failed(CheckpointerError::message(
            "intentional checkpoint failure",
        )))
    }

    async fn latest(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(None)
    }

    async fn history(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn checkpoint_failure_streams_partial_events_and_run_failed() {
    let broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut stream = broadcast.subscribe();
    fixed_graph(Add)
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::default(),
            event_config(&broadcast, EventRetention::None),
            RunControl::default(),
            CheckpointConfig::new(
                "stream-failure",
                Arc::new(FailingCheckpointer) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("checkpoint failure");
    let events = receive(&mut stream, 5).await;
    assert!(matches!(
        events.last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::CheckpointSaveFailed { .. },
            ..
        })
    ));
}

#[tokio::test]
async fn interrupt_and_subgraph_boundaries_are_streamed() {
    let mut interrupt_graph = StateGraph::new();
    interrupt_graph.set_version("stream-interrupt-v1");
    interrupt_graph
        .add_interruptible_node("interrupt", Interrupt)
        .expect("interrupt");
    interrupt_graph
        .add_edge(START, "interrupt")
        .add_edge("interrupt", END);
    let broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut stream = broadcast.subscribe();
    interrupt_graph
        .compile()
        .expect("graph")
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::default(),
            event_config(&broadcast, EventRetention::None),
            RunControl::default(),
            CheckpointConfig::new(
                "stream-interrupt",
                Arc::new(InMemoryCheckpointer::new(Codec)) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("interrupt");
    assert_eq!(
        kinds(&receive(&mut stream, 5).await),
        [
            "run_started",
            "node_started",
            "node_interrupted",
            "checkpoint_saved",
            "run_interrupted"
        ]
    );

    let mut child = StateGraph::new();
    child.add_node("inside", Add).expect("inside");
    child.add_edge(START, "inside").add_edge("inside", END);
    let mut root = StateGraph::new();
    root.add_subgraph("child", child.compile().expect("child"))
        .expect("mount");
    root.add_edge(START, "child").add_edge("child", END);
    let subgraph_broadcast = EventBroadcast::new(16).expect("broadcast");
    let mut subgraph_stream = subgraph_broadcast.subscribe();
    root.compile()
        .expect("root")
        .invoke_with_events(
            State::default(),
            RunConfig::default(),
            event_config(&subgraph_broadcast, EventRetention::None),
        )
        .await
        .expect("subgraph run");
    assert_eq!(
        kinds(&receive(&mut subgraph_stream, 7).await),
        [
            "run_started",
            "subgraph_started",
            "node_started",
            "node_completed",
            "state_updated",
            "subgraph_completed",
            "run_completed"
        ]
    );
}

#[tokio::test]
async fn concurrent_runs_interleave_but_each_run_keeps_its_event_order() {
    let barrier = Arc::new(Barrier::new(2));
    let graph = fixed_graph(WaitAt(barrier));
    let broadcast = EventBroadcast::new(32).expect("broadcast");
    let mut stream = broadcast.subscribe();
    let first_events = event_config(&broadcast, EventRetention::None);
    let second_events = event_config(&broadcast, EventRetention::None);
    let (first, second) = tokio::join!(
        graph.invoke_with_events(State::default(), RunConfig::default(), first_events),
        graph.invoke_with_events(State::default(), RunConfig::default(), second_events),
    );
    let first = first.expect("first run");
    let second = second.expect("second run");
    assert_ne!(first.run_id(), second.run_id());

    let events = receive(&mut stream, 10).await;
    let mut by_run = HashMap::<RunId, Vec<GraphEvent>>::new();
    for event in &events {
        by_run
            .entry(event.run_id())
            .or_default()
            .push(event.clone());
    }
    assert_eq!(by_run.len(), 2);
    for run_events in by_run.values() {
        assert_eq!(
            kinds(run_events),
            [
                "run_started",
                "node_started",
                "node_completed",
                "state_updated",
                "run_completed"
            ]
        );
    }
    let first_terminal = events
        .iter()
        .position(|event| matches!(event, GraphEvent::RunCompleted { .. }))
        .expect("terminal event");
    assert!(
        events[..first_terminal]
            .iter()
            .any(|event| event.run_id() != events[0].run_id()),
        "barrier guarantees another run emits before the first can complete"
    );
}
