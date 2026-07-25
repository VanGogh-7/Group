use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointIncompatibility,
    CheckpointPolicy, CheckpointState, Checkpointer, CodecDescriptor, END, EncodedValue,
    EventConfig, EventRetention, EventSink, ForkConfig, GraphEvent, GraphRunError, GraphState,
    InMemoryCheckpointer, InterruptPayload, InterruptibleNode, Node, NodeContext, NodeError,
    NodeId, NodeOutcome, NodePath, NodeUpdate, ResumeConfig, RunConfig, RunControl, RunFailure,
    START, SnapshotError, StateError, StateGraph,
};
use tokio::sync::Barrier;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default, Eq, PartialEq)]
struct State {
    value: u64,
}

impl GraphState for State {
    type Update = u64;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        for update in updates {
            self.value += update.into_parts().1;
        }
        Ok(())
    }
}

impl CheckpointState for State {
    type Snapshot = u64;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(self.value)
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self { value: *snapshot })
    }
}

struct Codec;

impl CheckpointCodec<u64> for Codec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.tests.fork", 1, "u64-le")
    }

    fn encode_snapshot(&self, snapshot: &u64) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<u64, CheckpointCodecError> {
        let bytes = <[u8; 8]>::try_from(bytes)
            .map_err(|_| CheckpointCodecError::message("invalid fork snapshot"))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let value = payload
            .downcast_ref::<String>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        Ok(EncodedValue::new(
            CodecDescriptor::new("group.tests.fork.interrupt", 1, "u64-le"),
            value.as_bytes(),
        ))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        let text = std::str::from_utf8(value.bytes())
            .map_err(|source| CheckpointCodecError::with_source("invalid interrupt", source))?;
        Ok(InterruptPayload::new(text.to_owned()))
    }
}

struct Add(u64);

#[async_trait]
impl Node<State> for Add {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<u64, NodeError> {
        Ok(self.0)
    }
}

struct WaitAdd(u64, Arc<Barrier>);

#[async_trait]
impl Node<State> for WaitAdd {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<u64, NodeError> {
        self.1.wait().await;
        Ok(self.0)
    }
}

struct Approval;

#[async_trait]
impl InterruptibleNode<State> for Approval {
    async fn run(
        &self,
        _state: &State,
        context: &NodeContext,
    ) -> Result<NodeOutcome<u64>, NodeError> {
        match context.resume_value::<u64>() {
            Some(value) => Ok(NodeOutcome::update(*value)),
            None => Ok(NodeOutcome::interrupt(String::from("approve fork"))),
        }
    }
}

struct RepeatApproval;

#[async_trait]
impl InterruptibleNode<State> for RepeatApproval {
    async fn run(
        &self,
        _state: &State,
        context: &NodeContext,
    ) -> Result<NodeOutcome<u64>, NodeError> {
        match context.resume_value::<u64>() {
            Some(1) | None => Ok(NodeOutcome::interrupt(String::from("approve nested fork"))),
            Some(value) => Ok(NodeOutcome::update(*value)),
        }
    }
}

struct Pending;

#[async_trait]
impl Node<State> for Pending {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<u64, NodeError> {
        pending().await
    }
}

fn graph() -> group_agent_core::CompiledGraph<State> {
    let mut graph = StateGraph::new();
    graph.set_version("fork-v1");
    graph.add_node("one", Add(1)).expect("one");
    graph.add_node("two", Add(2)).expect("two");
    graph.add_node("three", Add(3)).expect("three");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    graph.compile().expect("graph")
}

fn graph_with_final_barrier(barrier: Arc<Barrier>) -> group_agent_core::CompiledGraph<State> {
    let mut graph = StateGraph::new();
    graph.set_version("fork-v1");
    graph.add_node("one", Add(1)).expect("one");
    graph.add_node("two", Add(2)).expect("two");
    graph.add_node("three", WaitAdd(3, barrier)).expect("three");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    graph.compile().expect("graph")
}

fn fan_out_graph() -> group_agent_core::CompiledGraph<State> {
    let mut graph = StateGraph::new();
    graph.set_version("fork-fan-out-v1");
    graph.add_node("router", Add(1)).expect("router");
    graph.add_node("alpha", Add(10)).expect("alpha");
    graph.add_node("beta", Add(20)).expect("beta");
    graph.add_node("join", Add(100)).expect("join");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["alpha", "beta"], |_| {
            Ok(vec![NodeId::from("beta"), NodeId::from("alpha")])
        })
        .expect("fan-out");
    graph
        .add_edge("alpha", "join")
        .add_edge("beta", "join")
        .add_edge("join", END);
    graph.compile().expect("fan-out graph")
}

fn subgraph_graph() -> group_agent_core::CompiledGraph<State> {
    let mut child = StateGraph::new();
    child.add_node("one", Add(2)).expect("one");
    child.add_node("two", Add(3)).expect("two");
    child
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    let mut root = StateGraph::new();
    root.set_version("fork-subgraph-v1");
    root.add_node("prep", Add(1)).expect("prep");
    root.add_subgraph("child", child.compile().expect("child"))
        .expect("mount");
    root.add_edge(START, "prep");
    root.add_edge("prep", "child").add_edge("child", END);
    root.compile().expect("subgraph")
}

fn interrupt_graph() -> group_agent_core::CompiledGraph<State> {
    let mut graph = StateGraph::new();
    graph.set_version("fork-interrupt-v1");
    graph.add_node("prep", Add(1)).expect("prep");
    graph
        .add_interruptible_node("approval", Approval)
        .expect("approval");
    graph
        .add_edge(START, "prep")
        .add_edge("prep", "approval")
        .add_edge("approval", END);
    graph.compile().expect("interrupt graph")
}

fn repeated_nested_interrupt_graph() -> group_agent_core::CompiledGraph<State> {
    let mut child = StateGraph::new();
    child
        .add_interruptible_node("approval", RepeatApproval)
        .expect("approval");
    child.add_edge(START, "approval").add_edge("approval", END);
    let mut root = StateGraph::new();
    root.set_version("fork-nested-interrupt-v1");
    root.add_subgraph("child", child.compile().expect("child"))
        .expect("mount");
    root.add_edge(START, "child").add_edge("child", END);
    root.compile().expect("nested interrupt graph")
}

fn control_graph() -> group_agent_core::CompiledGraph<State> {
    let mut graph = StateGraph::new();
    graph.set_version("fork-control-v1");
    graph.add_node("seed", Add(1)).expect("seed");
    graph.add_node("pending", Pending).expect("pending");
    graph
        .add_edge(START, "seed")
        .add_edge("seed", "pending")
        .add_edge("pending", END);
    graph.compile().expect("control graph")
}

#[derive(Default)]
struct RecordingSink(Mutex<Vec<GraphEvent>>);

impl EventSink for RecordingSink {
    fn on_event(&self, event: &GraphEvent) {
        self.0.lock().expect("events").push(event.clone());
    }
}

fn typed(store: &Arc<InMemoryCheckpointer<u64>>) -> Arc<dyn Checkpointer<u64>> {
    Arc::clone(store) as Arc<dyn Checkpointer<u64>>
}

async fn seed(
    graph: &group_agent_core::CompiledGraph<State>,
    store: &Arc<InMemoryCheckpointer<u64>>,
    thread: &str,
) {
    graph
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::default(),
            group_agent_core::EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(thread, typed(store), CheckpointPolicy::EverySuperstep),
        )
        .await
        .expect("seed");
}

#[tokio::test]
async fn non_latest_fork_keeps_default_lineage_unchanged_and_uses_branch_cas() {
    let graph = graph();
    let store = Arc::new(InMemoryCheckpointer::new(Codec));
    seed(&graph, &store, "thread").await;
    let before = store.history(&"thread".into()).await.expect("history");
    let source = before[0].id();
    let original_latest = before.last().expect("latest").id();

    let config = ForkConfig::new("thread", source, typed(&store));
    let branch_id = config.branch_id();
    let report = graph.fork(config).await.expect("fork");
    assert_eq!(report.branch_id(), branch_id);
    assert_eq!(
        report
            .outcome()
            .as_completed()
            .expect("completed")
            .final_state()
            .value,
        6
    );
    assert!(matches!(
        report.outcome().events().get(1),
        Some(GraphEvent::ForkStarted {
            source_checkpoint_id,
            branch_id: event_branch,
            step: 1,
            superstep: 1,
            ..
        }) if *source_checkpoint_id == source && *event_branch == branch_id
    ));

    let after = store.history(&"thread".into()).await.expect("history");
    assert_eq!(
        after
            .iter()
            .map(|checkpoint| checkpoint.id())
            .collect::<Vec<_>>(),
        before
            .iter()
            .map(|checkpoint| checkpoint.id())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        store
            .latest(&"thread".into())
            .await
            .expect("latest")
            .expect("record")
            .id(),
        original_latest
    );

    let branch = store
        .branch_history(&"thread".into(), branch_id)
        .await
        .expect("branch history");
    assert_eq!(branch.len(), 3);
    assert_eq!(branch[0].id(), source);
    assert_eq!(branch[1].parent_id(), Some(source));
    assert_eq!(branch[2].parent_id(), Some(branch[1].id()));
    assert!(branch[2].completed());
}

#[tokio::test]
async fn failed_fork_can_resume_its_branch_and_concurrent_resumes_cannot_fork_it() {
    let graph = graph();
    let store = Arc::new(InMemoryCheckpointer::new(Codec));
    seed(&graph, &store, "thread").await;
    let source = store.history(&"thread".into()).await.expect("history")[0].id();

    let config =
        ForkConfig::new("thread", source, typed(&store)).with_run_config(RunConfig::new(1));
    let branch_id = config.branch_id();
    let error = graph.fork(config).await.expect_err("budget failure");
    assert!(
        matches!(error, GraphRunError::MaxStepsExceeded { step: 3, .. }),
        "{error:?}"
    );
    let branch = store
        .branch_history(&"thread".into(), branch_id)
        .await
        .expect("branch history");
    assert_eq!(branch.len(), 2);
    assert_eq!(branch[1].parent_id(), Some(source));

    let graph = graph_with_final_barrier(Arc::new(Barrier::new(2)));
    let left = graph.resume(ResumeConfig::new("thread", typed(&store)).with_branch_id(branch_id));
    let right = graph.resume(ResumeConfig::new("thread", typed(&store)).with_branch_id(branch_id));
    let (left, right) = tokio::join!(left, right);
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let error = left.err().or_else(|| right.err()).expect("one conflict");
    assert!(matches!(
        error,
        GraphRunError::BranchCheckpointConflict {
            branch_id: actual,
            ..
        } if actual == branch_id
    ));

    let branch = store
        .branch_history(&"thread".into(), branch_id)
        .await
        .expect("branch history");
    assert_eq!(branch.len(), 3);
    assert!(branch.last().expect("head").completed());
    assert_eq!(
        store
            .history(&"thread".into())
            .await
            .expect("history")
            .len(),
        3
    );
}

#[tokio::test]
async fn completed_checkpoint_fork_is_a_noop_but_creates_an_isolated_head() {
    let graph = graph();
    let store = Arc::new(InMemoryCheckpointer::new(Codec));
    seed(&graph, &store, "thread").await;
    let source = store
        .latest(&"thread".into())
        .await
        .expect("latest")
        .expect("checkpoint")
        .id();

    let config = ForkConfig::new("thread", source, typed(&store));
    let branch_id = config.branch_id();
    let report = graph.fork(config).await.expect("completed fork");
    let completed = report.outcome().as_completed().expect("completed");
    assert!(completed.visited_nodes().is_empty());
    let branch = store
        .branch_history(&"thread".into(), branch_id)
        .await
        .expect("branch history");
    assert_eq!(branch.len(), 1);
    assert_eq!(branch[0].id(), source);
}

#[tokio::test]
async fn branch_execution_preserves_conditional_fan_out_and_nested_subgraph_frontiers() {
    for (thread, graph, expected) in [
        ("fan-out", fan_out_graph(), 131_u64),
        ("subgraph", subgraph_graph(), 6_u64),
    ] {
        let store = Arc::new(InMemoryCheckpointer::new(Codec));
        graph
            .invoke_with_checkpoint(
                State::default(),
                RunConfig::new(1),
                group_agent_core::EventConfig::default(),
                RunControl::default(),
                CheckpointConfig::new(thread, typed(&store), CheckpointPolicy::EverySuperstep),
            )
            .await
            .expect_err("seed stops at saved frontier");
        let source = store
            .latest(&thread.into())
            .await
            .expect("latest")
            .expect("source")
            .id();
        let report = graph
            .fork(ForkConfig::new(thread, source, typed(&store)))
            .await
            .expect("fork");
        let completed = report.outcome().as_completed().expect("completed");
        assert_eq!(completed.final_state().value, expected);
        if thread == "fan-out" {
            assert_eq!(
                completed
                    .visited_nodes()
                    .iter()
                    .filter(|path| path.as_str() == "join")
                    .count(),
                1
            );
        }
        let branch = store
            .branch_history(&thread.into(), report.branch_id())
            .await
            .expect("branch history");
        for pair in branch.windows(2) {
            assert_eq!(pair[1].parent_id(), Some(pair[0].id()));
        }
    }
}

#[tokio::test]
async fn branch_interrupt_is_saved_and_branch_resume_consumes_the_value() {
    let graph = interrupt_graph();
    let store = Arc::new(InMemoryCheckpointer::new(Codec));
    graph
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::new(1),
            group_agent_core::EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new("interrupt", typed(&store), CheckpointPolicy::EverySuperstep),
        )
        .await
        .expect_err("seed stops before approval");
    let source = store
        .latest(&"interrupt".into())
        .await
        .expect("latest")
        .expect("source")
        .id();
    let config = ForkConfig::new("interrupt", source, typed(&store));
    let branch_id = config.branch_id();
    let fork = graph.fork(config).await.expect("fork interrupt");
    assert!(fork.outcome().as_interrupted().is_some());
    let interrupted = store
        .branch_head(&"interrupt".into(), branch_id)
        .await
        .expect("branch head")
        .expect("interrupted");
    assert!(interrupted.interrupted());
    assert_eq!(interrupted.parent_id(), Some(source));

    let resumed = graph
        .resume(
            ResumeConfig::new("interrupt", typed(&store))
                .with_branch_id(branch_id)
                .with_resume_value(7_u64),
        )
        .await
        .expect("branch resume");
    assert_eq!(resumed.final_state().value, 8);
    assert!(matches!(
        resumed.events().get(2),
        Some(GraphEvent::BranchResumed {
            branch_id: actual,
            ..
        }) if *actual == branch_id
    ));
    let history = store
        .branch_history(&"interrupt".into(), branch_id)
        .await
        .expect("branch history");
    assert_eq!(history.len(), 3);
    assert_eq!(history[2].parent_id(), Some(interrupted.id()));
    assert!(history[2].completed());
}

#[tokio::test]
async fn incompatible_graph_version_fails_before_branch_creation() {
    let graph = graph();
    let store = Arc::new(InMemoryCheckpointer::new(Codec));
    seed(&graph, &store, "version").await;
    let source = store.history(&"version".into()).await.expect("history")[0].id();

    let mut incompatible = StateGraph::new();
    incompatible.set_version("fork-v2");
    incompatible.add_node("one", Add(1)).expect("one");
    incompatible.add_node("two", Add(2)).expect("two");
    incompatible.add_node("three", Add(3)).expect("three");
    incompatible
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    let incompatible = incompatible.compile().expect("incompatible graph");
    let config = ForkConfig::new("version", source, typed(&store));
    let branch_id = config.branch_id();
    let error = incompatible
        .fork(config)
        .await
        .expect_err("version mismatch");
    assert!(matches!(
        error,
        GraphRunError::CheckpointIncompatible {
            reason: CheckpointIncompatibility::GraphVersionMismatch { .. },
            ..
        }
    ));
    assert!(
        store
            .branch_head(&"version".into(), branch_id)
            .await
            .expect("branch query")
            .is_none()
    );
}

#[tokio::test]
async fn fork_control_failures_have_one_terminal_event_and_preserve_branch_contract() {
    let graph = control_graph();
    let store = Arc::new(InMemoryCheckpointer::new(Codec));
    graph
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new("control", typed(&store), CheckpointPolicy::EverySuperstep),
        )
        .await
        .expect_err("seed stops before pending");
    let source = store
        .latest(&"control".into())
        .await
        .expect("latest")
        .expect("source")
        .id();

    let token = CancellationToken::new();
    token.cancel();
    let cancelled = ForkConfig::new("control", source, typed(&store))
        .with_control(RunControl::new().with_cancellation_token(token));
    let cancelled_branch = cancelled.branch_id();
    assert!(matches!(
        graph.fork(cancelled).await,
        Err(GraphRunError::Cancelled { .. })
    ));
    assert!(
        store
            .branch_head(&"control".into(), cancelled_branch)
            .await
            .expect("cancelled branch")
            .is_none(),
        "failure before create must not create a branch"
    );

    for control in [
        RunControl::new().with_run_timeout(Duration::from_millis(10)),
        RunControl::new().with_node_timeout(Duration::from_millis(10)),
    ] {
        let sink = Arc::new(RecordingSink::default());
        let config = ForkConfig::new("control", source, typed(&store))
            .with_control(control)
            .with_event_config(
                EventConfig::new(EventRetention::None)
                    .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
            );
        let branch_id = config.branch_id();
        let error = graph.fork(config).await.expect_err("control failure");
        assert!(matches!(
            error,
            GraphRunError::RunTimedOut { .. } | GraphRunError::NodeTimedOut { .. }
        ));
        assert_eq!(
            store
                .branch_head(&"control".into(), branch_id)
                .await
                .expect("branch head")
                .expect("created branch")
                .id(),
            source,
            "execution failure retains the source head"
        );
        let events = sink.0.lock().expect("events");
        assert!(events.iter().any(|event| matches!(
            event,
            GraphEvent::ForkStarted {
                branch_id: actual,
                ..
            } if *actual == branch_id
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GraphEvent::RunFailed { .. }))
                .count(),
            1
        );
        assert!(matches!(
            events.last(),
            Some(GraphEvent::RunFailed {
                failure: RunFailure::RunTimedOut { .. } | RunFailure::NodeTimedOut { .. },
                ..
            })
        ));
    }
}

#[tokio::test]
async fn fork_reexecutes_an_interrupted_nested_source_and_can_interrupt_again() {
    let graph = repeated_nested_interrupt_graph();
    let store = Arc::new(InMemoryCheckpointer::new(Codec));
    let seeded = graph
        .invoke_with_checkpoint(
            State::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "nested-interrupt",
                typed(&store),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("seed interrupt");
    let source = seeded.as_interrupted().expect("interrupted");
    let source_id = source.checkpoint_id();
    let source_interrupt_id = source.interrupt().id();
    assert_eq!(
        source.interrupt().node_path(),
        &NodePath::new(&group_agent_core::GraphPath::new(["child"]), "approval")
    );

    let config =
        ForkConfig::new("nested-interrupt", source_id, typed(&store)).with_resume_value(1_u64);
    let branch_id = config.branch_id();
    let repeated = graph.fork(config).await.expect("repeated interrupt");
    let repeated = repeated
        .outcome()
        .as_interrupted()
        .expect("branch interrupted");
    assert_ne!(repeated.checkpoint_id(), source_id);
    assert_ne!(repeated.interrupt().id(), source_interrupt_id);
    assert_eq!(
        repeated.interrupt().node_path(),
        &NodePath::new(&group_agent_core::GraphPath::new(["child"]), "approval")
    );

    let completed = graph
        .resume(
            ResumeConfig::new("nested-interrupt", typed(&store))
                .with_branch_id(branch_id)
                .with_resume_value(2_u64),
        )
        .await
        .expect("resume repeated interrupt");
    assert_eq!(completed.final_state().value, 2);
    let history = store
        .branch_history(&"nested-interrupt".into(), branch_id)
        .await
        .expect("branch history");
    assert_eq!(history.len(), 3);
    for pair in history.windows(2) {
        assert_eq!(pair[1].parent_id(), Some(pair[0].id()));
    }
}
