# Group

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. Its
name borrows the metaphor of a hierarchy of algebraic structures: Group is
intended to be the execution foundation beneath higher-level agent frameworks.
It does not claim to implement a mathematical group.

## Current stage

Stage 9 adds shared-state subgraphs and structured execution namespaces while
preserving typed suspension and latest-only checkpoint lineage:

```text
START -> prepare -> [local_search, web_search] -> synthesis -> END
                  successful boundary -> Checkpoint
                  node interrupt -> Interrupted Checkpoint -> Resume value
START -> prepare -> research.{search -> verify} -> answer -> END
```

Every node in one parallel frontier inspects the same immutable state snapshot.
The Runtime waits for the whole frontier, orders successful updates by compiled
node order, commits them through `GraphState::apply_batch`, and only then
calculates the next frontier. Sequential one-node frontiers continue to use
`GraphState::apply`.

Checkpoint-enabled invocations create a user-defined state snapshot only after
a super-step has committed and resolved its complete next frontier. Normal
`invoke` calls do not create snapshots, call storage, or take checkpoint locks.

The current core includes:

- asynchronous trait-based nodes;
- fixed edges, static fan-out, fan-in barriers, and conditional target
  whitelists;
- concurrent node futures without per-node task spawning;
- explicit, deterministic parallel state-update merging;
- opt-in snapshots and asynchronous replaceable checkpoint storage;
- process-local, thread-safe `InMemoryCheckpointer`;
- latest and ordered history queries with CAS-protected checkpoint lineage;
- restoration of state, frontier, cumulative step, and super-step position;
- explicit graph-version compatibility and latest-only resume checks;
- typed interrupt payloads and resume values without Serde bounds;
- interrupted checkpoints and completed-or-interrupted execution outcomes;
- shared-state `CompiledGraph<S>` mounting through `add_subgraph`;
- structured `GraphPath` and `NodePath` namespaces for nested execution;
- subgraph-aware events, errors, checkpoints, resume, and interrupts;
- explicit loops protected by a per-run `max_steps`;
- immutable, reusable, concurrently shareable compiled graphs;
- immediate lifecycle delivery through a thread-safe `EventSink`;
- independent full or disabled event retention for successful run reports;
- cooperative cancellation through Tokio Util `CancellationToken`;
- optional run-level and per-node Tokio deadlines;
- typed `RunFailed` events and per-invocation `RunId` values;
- ordered successful run reports and extensible lifecycle events;
- source-preserving structured errors;
- topology, edge-shape, whitelist, and reachability validation.

## Shared-state subgraphs and execution namespaces

A compiled graph using the same `GraphState` can be mounted as a structural
item in a parent:

```rust
let research = research_builder.compile()?;

let mut graph = StateGraph::new();
graph.set_version("agent-v4");
graph.add_node("prepare", Prepare)?;
graph.add_subgraph("research", research)?;
graph.add_node("answer", Answer)?;
graph
    .add_edge(START, "prepare")
    .add_edge("prepare", "research")
    .add_edge("research", "answer")
    .add_edge("answer", END);
```

The mount is not a node, performs no user code, and consumes neither a step nor
a super-step. Child real nodes borrow and update the same Runtime-owned State,
use the same `RunId`, cancellation token, run deadline, event configuration,
and checkpoint lineage, and contribute to the parent's cumulative step and
super-step counters. Reaching child `END` follows the mount's parent
transition; it does not complete the parent run. `START -> END` children return
to the parent immediately. Nested subgraphs are supported.

`GraphPath` is a structured sequence of mount identifiers. `NodePath` is that
namespace plus one leaf `NodeId`; for example, `/research/verify` displays two
segments rather than a string that Runtime later parses. Display uses
slash-prefixed segments and percent-escapes `%` and `/` within identifiers, so
identifiers containing `.`, `/`, `%`, or an empty string remain unambiguous.
The root `GraphPath` displays as `<root>`. Both types are cheap to clone through
shared storage and implement `Display`, `Debug`, equality, and hashing.
Runtime lookup and `Eq`/`Hash` use the structured segments, not displayed text.
`NodeContext::node_path()` exposes the complete path while `node_id()` remains
a leaf compatibility accessor. Node lifecycle events, node-related run errors,
interrupt metadata, visited nodes, state batch sources, and checkpoint
frontiers use `NodePath`.

Entering and leaving a child emits `SubgraphStarted` and
`SubgraphCompleted`. A child does not create a second top-level `RunStarted` or
`RunCompleted`. Failure or interruption before child exit omits
`SubgraphCompleted`. On resume inside a child, event order begins
`RunStarted -> RunResumed -> SubgraphStarted` for each containing namespace,
then continues with node events.

Compilation pre-resolves child entry, exit, paths, and internal transitions.
`add_subgraph` takes ownership of an already immutable compiled child, so
direct and indirect reference cycles are unrepresentable through the safe
builder API; flattening still guards path uniqueness as a compiler invariant.
For Stage 9, a subgraph mount cannot run beside another active parent-frontier
item; such topology is rejected with `SubgraphInParallelFrontier`. `END` exits
only its own branch and is removed before this check, so a fan-out such as
`[END, child]` is valid because only the child remains active. A subgraph plus
an ordinary active node remains invalid, directly or through later
transitions. Parallel super-steps inside a child remain supported.
Parent/child State mapping and conditional fan-out are not implemented. See
[`examples/subgraph.rs`](crates/group-agent-core/examples/subgraph.rs).

One root `GraphVersion` is the compatibility version of the complete composed
graph. Change it whenever a mounted child's topology, State/Snapshot schema,
batch reducer, router behavior, or interrupt meaning becomes incompatible with
saved checkpoints.

## Parallel super-steps

Static fan-out is one transition kind:

```rust
graph.add_fan_out("prepare", ["local_search", "web_search"])?;
graph
    .add_edge("local_search", "synthesis")
    .add_edge("web_search", "synthesis");
```

An executable node has exactly one fixed, fan-out, or conditional transition.
Conditional routers still select one target; conditional fan-out is not
implemented. `START` continues to require exactly one fixed successor.

The Runtime maintains an active frontier. Nodes in a multi-node frontier start
in compiled node order and are polled concurrently using `FuturesUnordered`,
without `tokio::spawn`. They all borrow the same `&State`. Only after every node
succeeds does the Runtime commit updates and calculate successors. Duplicate
successor indices are sorted and deduplicated, so a fan-in target executes once
in the next super-step. An `END` successor removes only that branch; other
branches continue until the frontier is empty.

`max_steps` counts real node executions across all frontiers. A parallel
frontier is atomic with respect to this limit: if the complete frontier does not
fit, none of its nodes starts and `MaxStepsExceeded` identifies the first stable
frontier position that would exceed the limit.

### Deterministic state merge

`GraphState::apply_batch` receives `Vec<NodeUpdate<S::Update>>` in compiled node
order. Each entry exposes its complete source `NodePath`, leaf `NodeId`, and
update:

```rust
fn apply_batch(
    &mut self,
    updates: Vec<NodeUpdate<Self::Update>>,
) -> Result<(), StateError> {
    // Validate the entire batch without mutating self.
    let validated = updates
        .iter()
        .map(|item| validate(item.node_path(), item.update()))
        .collect::<Result<Vec<_>, _>>()?;

    // Commit only after all validation succeeds.
    for update in validated {
        self.commit(update);
    }
    Ok(())
}
```

The default implementation rejects a batch containing multiple updates before
modifying state; it never silently applies last-write-wins. A custom batch
implementation must validate the complete batch before mutation because the
Runtime does not clone the complete state to provide rollback. One-node
frontiers continue to call `apply`, so existing sequential states need no
change. See
[`examples/parallel.rs`](crates/group-agent-core/examples/parallel.rs) for a
complete fan-out, merge, and fan-in example.

## Checkpoint foundation

Checkpoint capability is separate from `GraphState`, so ordinary states still
need neither `Clone` nor Serde:

```rust
impl CheckpointState for AgentState {
    type Snapshot = AgentSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(AgentSnapshot::from(self))
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self::from(snapshot))
    }
}
```

`restore` is called synchronously during resume and remains outside storage
locks. It is not preemptible by cancellation or timeout; control is observed
again immediately after it returns.

Checkpointing is enabled only through `invoke_with_checkpoint`:

```rust
use std::sync::Arc;

use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, Checkpointer, InMemoryCheckpointer,
};

let store = Arc::new(InMemoryCheckpointer::<AgentSnapshot>::new());
let config = CheckpointConfig::new(
    "conversation-42",
    Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
    CheckpointPolicy::EverySuperstep,
);

let report = compiled
    .invoke_with_checkpoint(
        initial_state,
        RunConfig::default(),
        EventConfig::default(),
        RunControl::default(),
        config,
    )
    .await?;
```

Graphs intended for checkpoint resume must have an explicit compatibility
version before compilation:

```rust
graph.set_version("agent-graph-v3");
let compiled = graph.compile()?;
```

The root version is stored in every new checkpoint and covers the complete
composed graph. Update it whenever parent or child topology, state/Snapshot
schema, batch reducer behavior, router semantics, or interrupt meaning changes
in a way that makes an old frontier or state unsafe to continue.
Unversioned checkpoints and unversioned compiled graphs cannot resume.

`CheckpointConfig::new` explicitly starts from state with no parent checkpoint.
When the supplied state is based on an existing checkpoint, identify that
lineage rather than allowing storage insertion order to choose it:

```rust
let base = store
    .latest(&ThreadId::from("conversation-42"))
    .await?
    .expect("base checkpoint")
    .id();
let config = CheckpointConfig::new(
    "conversation-42",
    Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
    CheckpointPolicy::EverySuperstep,
)
.with_expected_parent(Some(base));
```

`CheckpointPolicy::EverySuperstep` saves after every successful super-step.
`FinalOnly` creates only the final completed checkpoint. Each immutable
`Checkpoint` records its `CheckpointId`, `ThreadId`, `RunId`, parent, graph
version, super-step, cumulative step count, shared `Arc<Snapshot>`, stable
`NodePath` next frontier, completed flag, and optional interrupt metadata.
`Checkpointer::latest`, `get`, and `history`
return shared `Arc<Checkpoint<_>>` values, so queries do not deep-copy
snapshots. `get` is scoped by both `ThreadId` and `CheckpointId`; it never
returns a checkpoint owned by another thread. History is ordered oldest to
newest.

A checkpoint parent represents the state lineage on which execution was based,
not the previous insertion by wall-clock order. Each write carries both a
Runtime-assigned `CheckpointId` idempotency key and an `expected_parent`.
`InMemoryCheckpointer` compares the thread's latest checkpoint with that
expected parent and inserts atomically under one short lock. A mismatch returns
`GraphRunError::CheckpointConflict`; it never silently joins unrelated runs.
Consequently, concurrent runs using the same `ThreadId` and base normally race:
the first accepted write advances the lineage and the other conflicts.
Different thread identifiers remain isolated. Within one run, every successful
save becomes the expected parent of that run's next save.

The exact save boundary is:

1. every node in the frontier succeeds;
2. `apply` or `apply_batch` commits successfully;
3. all successor routing succeeds and the next frontier is stable;
4. the user snapshot is created outside storage locks;
5. the checkpointer saves it before Runtime enters the next super-step.

A failing node, batch merge, state apply, or router does not create a checkpoint
for that super-step. Snapshot, conflict, and storage failures return structured
`GraphRunError::SnapshotFailed`, `CheckpointConflict`, or
`CheckpointSaveFailed`, emit one final `RunFailed`, and stop execution. State
already committed at the boundary and external node side effects are not rolled
back.

Run cancellation and run timeout remain active while the asynchronous save
future is pending. Cancellation has priority over run timeout, and both have
priority over a simultaneously ready save result. Such failures use checkpoint
boundary context (`node_id = None`, with the cumulative completed step count),
emit exactly one `RunFailed`, and emit neither `CheckpointSaved` nor
`RunCompleted`. Dropping a save future cannot prove that a backend produced no
side effect: storage may have committed before its future returned. Custom
checkpointers must therefore treat `CheckpointRequest::checkpoint_id()` as an
idempotency key. An exact replay with the same metadata and snapshot `Arc`
returns the original result even if latest has advanced. Reusing the same ID
with different lineage, graph version, boundary, frontier, completion, or
snapshot metadata returns `CheckpointWriteError::IdempotencyConflict`.
`InMemoryCheckpointer` implements this contract.

Snapshot creation is synchronous and cannot be preempted. It occurs before
entering storage and never under the in-memory store lock. A legal
`START -> END` graph saves exactly one completed checkpoint under either
policy, with `superstep = 0`, `step = 0`, an empty frontier, and the configured
expected parent. Its successful terminal order is `CheckpointSaved` followed
by `RunCompleted`. The in-memory implementation provides no database durability.
See [`examples/checkpoint.rs`](crates/group-agent-core/examples/checkpoint.rs).

## Resume from checkpoint

`ResumeConfig` keeps checkpoint selection, checkpoint policy, additional step
budget, events, and execution controls in one configuration:

```rust
let report = compiled
    .resume(
        ResumeConfig::new(
            "conversation-42",
            Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
        )
        // Omit this to load latest.
        .with_checkpoint_id(checkpoint_id)
        .with_run_config(RunConfig::new(100))
        .with_checkpoint_policy(CheckpointPolicy::EverySuperstep)
        .with_event_config(EventConfig::default())
        .with_control(RunControl::default()),
    )
    .await?;
```

Resume loads a specified checkpoint through `get`, or uses `latest` by
default. A specified checkpoint must still equal current latest; otherwise
`ResumeConflict` is returned because Fork is not implemented. The
Runtime validates ThreadId, latest-only status, explicit graph version,
completed/frontier consistency, and every saved frontier `NodePath`, resolving
it to compiled internal indices in O(F). `START`, explicit
`END`, unknown or invalid namespaced nodes, unversioned data, and version
mismatches produce
`CheckpointIncompatible`.

Only after every compatibility check and frontier resolution succeeds does the
Runtime call `CheckpointState::restore` outside the storage lock. The resolved
indices are reused directly for execution rather than parsed again. Events are
ordered `RunStarted`, `RunResumed`, then any containing `SubgraphStarted`
boundaries and the continued node lifecycle. Restore failure instead emits
`RunStarted` followed by one `RunFailed`. `RunResumed` identifies the thread,
checkpoint, cumulative step, and super-step position.

Steps and super-steps continue from the checkpoint. `RunConfig::max_steps`
means the additional number of nodes allowed by this resume call; error and
checkpoint positions still use cumulative lineage steps. A resumed save uses
the restored checkpoint as `expected_parent`, and later saves continue that
chain. Resuming a completed checkpoint restores state but executes no node and
does not create another completed checkpoint; its exact success sequence is
`RunStarted -> RunResumed -> RunCompleted`.

Cancellation and run timeout start at the `resume` call entry and remain active
while loading storage and executing. Restore itself is synchronous and
uninterruptible. Any load, compatibility, latest, restore, cancellation, or
timeout failure saves nothing new and emits exactly one `RunFailed`. See
[`examples/resume.rs`](crates/group-agent-core/examples/resume.rs).

## Suspension and human interrupt

Ordinary update-only nodes continue implementing `Node` with no signature
change. A node that may suspend implements `InterruptibleNode` and is registered
through `add_interruptible_node`:

```rust
#[async_trait]
impl InterruptibleNode<AgentState> for ApprovalNode {
    async fn run(
        &self,
        _state: &AgentState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<AgentUpdate>, NodeError> {
        if context.has_resume_value() {
            let decision = context
                .require_resume_value::<ApprovalDecision>()
                .map_err(|source| {
                    NodeError::with_source("invalid approval value", source)
                })?;
            return Ok(NodeOutcome::update(AgentUpdate::Approved(
                decision.clone(),
            )));
        }

        Ok(NodeOutcome::interrupt(ApprovalPrompt {
            summary: "Publish this draft?",
        }))
    }
}
```

`InterruptRequest` assigns a fresh `InterruptId`. Its typed payload is held
behind `Arc` and accessed with safe `downcast_ref`; neither State, Snapshot,
payload, nor Resume value requires Serde. Ordinary `Node` execution creates no
interrupt payload allocation.

`NodeContext::require_resume_value<T>()` distinguishes a missing value from a
concrete type mismatch through `ResumeValueError`; mismatch context includes
the expected and actual Rust type names and can be preserved as a
`NodeError` source. The older `resume_value<T>() -> Option<&T>` remains
available for optional inspection.

Checkpoint-enabled invocation and Resume now return
`ExecutionOutcome::{Completed, Interrupted}`. A singleton node interrupt:

1. applies no state update and performs no successor routing;
2. emits `NodeInterrupted`;
3. snapshots the unchanged committed state;
4. saves an incomplete interrupted checkpoint whose singleton frontier is the
   current node;
5. emits `CheckpointSaved` followed by `RunInterrupted`;
6. returns `ExecutionOutcome::Interrupted`, never `RunCompleted`.

The interrupted report exposes the shared payload, InterruptId, checkpoint and
thread identifiers, last committed State, cumulative committed step and
super-step counters, visited attempts, and retained events. Interrupt is a
successful suspension, not a `GraphRunError`. If checkpointing is disabled,
Runtime returns `InterruptRequiresCheckpoint`. A save failure, lineage
conflict, cancellation, or run timeout remains a failure: it emits one
`RunFailed` and returns no interrupted outcome.

Resume an interrupted checkpoint by supplying a typed value:

```rust
let outcome = graph
    .resume(
        ResumeConfig::new("conversation-42", store)
            .with_resume_value(ApprovalDecision::Approve),
    )
    .await?;
```

The checkpoint must be latest and graph-compatible as before. An interrupted
checkpoint without a value returns `MissingResumeValue`; a normal or completed
checkpoint rejects an unexpected value. Runtime restores State, re-executes the
interrupted node, and exposes the value only through that node's `NodeContext`.
The value is valid only for this one re-execution attempt. After the node
returns an Update, it is cleared before successor execution. If the node
interrupts again, the old value is not stored in the new checkpoint and is not
automatically reused; a later resume must supply a new value. The next save
uses the interrupted checkpoint as expected parent. Repeated interrupts create
fresh InterruptId and CheckpointId values along one continuous lineage.

Re-execution can repeat code and external side effects that ran before the
interrupt. Runtime does not roll those effects back or deduplicate them.
Pre-interrupt work must therefore be idempotent, and irreversible effects
should normally occur only after the node validates its Resume value.

Interrupts are supported only from singleton frontiers. An interrupt observed
in a parallel frontier drops remaining futures, commits none of that
super-step's updates, and returns `UnsupportedParallelInterrupt`. Payloads are
process-local and have no persistent serialization format. See
[`examples/interrupt.rs`](crates/group-agent-core/examples/interrupt.rs).

## Event observation

`EventSink` is a small synchronous, infallible callback trait and does not
require a channel per invocation. `EventConfig` controls delivery and report
retention independently:

```rust
use std::sync::Arc;

use group_agent_core::{
    EventConfig, EventRetention, EventSink, GraphEvent, RunConfig,
};

let sink: Arc<dyn EventSink> = Arc::new(|event: &GraphEvent| {
    // Keep this callback short and non-blocking.
    eprintln!("run {}: {event:?}", event.run_id());
});

let report = compiled
    .invoke_with_events(
        initial_state,
        RunConfig::default(),
        EventConfig::new(EventRetention::None).with_sink(sink),
    )
    .await?;
assert!(report.events().is_empty());
```

All four event configurations are valid:

- `All` / no Sink: retain every event in a successful `RunReport`; this is the
  default and preserves earlier invocation behavior.
- `All` / Sink: deliver and retain the same ordered event sequence.
- `None` / Sink: deliver events immediately without retaining them.
- `None` / no Sink: neither deliver nor retain events. The Runtime skips event
  construction on this disabled path.

Every event carries a lightweight `RunId`, so concurrent invocations sharing one
sink remain distinguishable. Events never contain the complete state or an
update. A sink runs inline on the execution path, must be thread-safe, and
should not perform blocking or otherwise expensive work. `EventSink::on_event`
cannot return an error. If a sink panics, that panic propagates directly; it is
not converted into `GraphRunError`, and no later event delivery is guaranteed.
Group intentionally does not provide a Tokio channel or stream adapter.

For a multi-node frontier, `SuperstepStarted` is emitted before its stable
`NodeStarted` sequence. `NodeCompleted` follows the real future-completion order
and is intentionally not deterministic. After the batch commits,
`StateUpdated` is emitted in stable node order. `SuperstepCompleted` is emitted
only after update commit and all successor routing succeed. To preserve Stage
1–4 sequential event compatibility, these two super-step boundary events are
only emitted for multi-node frontiers. In checkpoint-enabled runs, a required
save must also succeed before `SuperstepCompleted` is emitted.

Checkpoint-enabled runs emit `CheckpointSaved` only after storage confirms the
save. The event includes checkpoint/thread/run identifiers, boundary position,
and completed status without including the Snapshot. Snapshot or storage
failures emit the corresponding typed `RunFailed`; they do not emit
`CheckpointSaved`.

Resume emits `RunResumed` only after loading, latest/version/frontier
validation, and successful state restoration. It precedes every continued node
event. A resume frontier inside a child then emits `SubgraphStarted` for its
containing namespaces before restarting nodes.

Subgraph entry and successful exit emit `SubgraphStarted` and
`SubgraphCompleted` with a structured `GraphPath`. They share the parent
`RunId`; nested children do not emit additional top-level run events.

Successful suspension emits `NodeInterrupted -> CheckpointSaved ->
RunInterrupted` after `NodeStarted`. It emits neither `NodeCompleted`,
`StateUpdated`, nor `RunCompleted` for the interrupted attempt. Checkpoint save
or control failure replaces the final suspension event with one `RunFailed`.

`RunReport` remains a success-only result. On a node, state-apply, batch-apply,
router, undeclared-target, step-limit, snapshot, or checkpoint-storage failure,
the Runtime first delivers all events already reached and then a final
`GraphEvent::RunFailed` to the sink before returning `GraphRunError`. The
`RunFailed` payload contains a stable typed `RunFailure` classification and
execution context; the original source chain stays on `GraphRunError` and is
not stringified into the event. A failed run does not return a partial
`RunReport`.

### Event API migration from Stage 2 to Stage 3

Stage 3 added `RunId` to every `GraphEvent` variant and added `RunFailed`.
Constructing event variants and matching every named field are therefore
breaking changes from the Stage 2 API. Consumers should include `run_id` when
constructing an event and use `..` when a match does not need every field. The
enum remains `#[non_exhaustive]`.

### Execution namespace API migration in Stage 9

Stage 9 changed node-location fields from leaf-only `NodeId` values to
structured `NodePath` values. This affects node-related fields in
`GraphEvent`, execution context in `GraphRunError` and `RunFailure`,
`RunReport::visited_nodes`, `NodeUpdate` sources, checkpoint next frontiers,
and checkpoint/returned interrupt metadata. Code that constructs these values
or exhaustively matches their field types must migrate accordingly.

Use `NodePath::leaf()` or `NodePath::as_str()` when only the leaf is needed.
Compatibility accessors named `node_id()` remain available on
`NodeContext`, `NodeUpdate`, and checkpoint interrupt metadata; use their
`node_path()` accessors when the complete namespace is required. Display output
is diagnostic only and must not be parsed for Runtime navigation.

## Execution control

`RunControl` composes with the existing `RunConfig` and `EventConfig`. It uses
Tokio timers and Tokio Util's `CancellationToken`; Group does not implement its
own executor, timer, polling thread, or cancellation primitive.

```rust
use std::time::Duration;

use group_agent_core::{EventConfig, RunConfig, RunControl};
use tokio_util::sync::CancellationToken;

let cancellation = CancellationToken::new();
let control = RunControl::new()
    .with_cancellation_token(cancellation.clone())
    .with_run_timeout(Duration::from_secs(30))
    .with_node_timeout(Duration::from_secs(10));

let report = compiled
    .invoke_with_control(
        initial_state,
        RunConfig::default(),
        EventConfig::default(),
        control,
    )
    .await?;
```

`RunControl::default()` supplies no external cancellation token and enables no
timeout. In that case node execution follows a direct-await fast path. With
control enabled:

- run timeout starts when `invoke_with_control` begins, before `RunStarted` is
  delivered;
- node timeout starts immediately before `NodeStarted` is delivered, so time in
  that synchronous sink callback counts toward the node deadline;
- the Runtime checks cancellation and the run deadline after `RunStarted`,
  before every node in a frontier, while each node future is pending, after
  observed node completion, and before advancing or completing the run;
- synchronous checks and asynchronous waiting both select the earlier absolute
  run or node deadline; equal deadlines select the run timeout. If Runtime
  polling resumes after both have expired, classification still follows that
  absolute ordering;
- cancellation precedes the selected timeout, and the selected timeout precedes
  a simultaneously ready node result. This preserves the equal-deadline
  priority cancellation, run timeout, node timeout, then node result. At node
  boundaries, cancellation and run timeout also take priority over `max_steps`.

The Runtime uses biased `tokio::select!` and does not spawn a task per node.
Cancellation or timeout drops all still-pending node futures in that
super-step. A failed super-step applies none of its collected updates. Dropping
a future does not roll back external side effects already performed by that
future. Synchronous `GraphState::apply`, `GraphState::apply_batch`, conditional
routers, and `EventSink` callbacks cannot be preempted; control is observed at
the next Runtime check after they return. Applied updates are never replayed.

Each parallel node has its own node deadline. Run timeout and cancellation cover
the complete invocation. If a parallel node error, cancellation, or timeout is
observed, the remaining futures are dropped and the first failure observed by
the Runtime wins. Absolute deadline ordering and the existing priority remain:
cancellation, run timeout, node timeout, then node result.

`NodeContext` exposes `cancellation_token()`, `is_cancelled()`,
`run_deadline()`, and `remaining_run_time()`. Cancellation state remains outside
`GraphState`.

Control failures return structured `GraphRunError::Cancelled`,
`GraphRunError::RunTimedOut`, or `GraphRunError::NodeTimedOut`. The sink first
receives all reached partial events and exactly one typed `RunFailed`; it does
not receive `RunCompleted`.

## Conditional routing

A conditional router is synchronous, read-only, and fallible:

```rust
graph.add_conditional_edges(
    "router",
    ["answer", "revise"],
    |state: &DraftState| {
        if state.ready {
            Ok(NodeId::from("answer"))
        } else {
            Ok(NodeId::from("revise"))
        }
    },
)?;
```

Each executable node has exactly one fixed edge, static fan-out, or conditional
router. A router may only return a declared single target. After a parallel
batch commits, routers for frontier nodes inspect the merged state in stable
node order. Async model, database, or tool work belongs in a node; the node
should write its result into state, and the router should only inspect that
updated state.

See
[`examples/conditional.rs`](crates/group-agent-core/examples/conditional.rs) for
an executable loop that revises state before routing back to the router.

## Runtime structure and performance policy

Public graph construction uses readable `NodeId` values backed by `Arc<str>`.
Compilation aggregates fixed successors, static fan-out targets, source counts,
conditional routers, and successor presence once, then reuses that data for
shape validation, outgoing-edge completeness, and transition compilation.
Together with topology construction and reachability traversal, ordinary
compilation remains approximately O(V + E). Parents that combine subgraph
mounts with fan-out additionally run a composition-only reachable-frontier-pair
check so indirect mixed subgraph frontiers fail at compile time. It operates on
each produced frontier, removes `END` branches before co-activity checks, uses
structured identifiers rather than path strings, and does not affect graphs
without both features. Compilation resolves all transitions and conditional
target whitelists to internal graph indices. Runtime execution uses those
indices directly. Fixed transitions remain O(1); fan-out targets are already
resolved.
Frontier sorting and deduplication operate only on produced successor indices,
not by scanning the complete graph. Internal `petgraph` types remain private.
Subgraph mounting flattens structural entry/exit items and precomputes
structured paths at compile time, so Runtime neither concatenates nor parses
path strings. Subgraph resume resolves only its saved frontier.

Each invocation owns its state, frontier, events, visited-node list, and step
counter. The Runtime does not clone complete states, take a global execution
lock, spawn each node, create mandatory channels, or repeat full graph
validation. `GraphState` does not require `Clone`; `RunReport<S>` is cloneable
only when `S: Clone`.

Compiled items distinguish normal nodes, interruptible nodes, and structural
subgraphs. Runtime matches the item kind and directly awaits the selected
public trait future. A normal `async-trait` node therefore keeps its one
required boxed trait future instead of passing through a second boxed adapter
future.

Checkpointing adds no storage call, snapshot creation, or lock acquisition to a
normal invocation. Enabled runs construct only next-frontier metadata and never
scan the complete graph. Snapshot cost is defined entirely by the user's
`CheckpointState` implementation.

Criterion benchmarks provide regression baselines only; no comparative
performance claim is made.

```bash
cargo bench --workspace
```

The baseline covers compilation of 100-node and 1,000-node fixed graphs,
execution of fixed and conditional graphs, repeated invocation, and the Stage 4
control/observation cases. Stage 5 adds 2-, 8-, and 32-branch immediate
frontiers and an 8-branch short-wait frontier. The scheduler baselines are named
explicitly as a 32-total-node linear chain and a 32-branch/33-total-node
fan-out, so they are not presented as equivalent topologies. Stage 6 adds
checkpoint-disabled and in-memory checkpoint-enabled invocation baselines.
Stage 7 adds load-plus-restore-plus-one-immediate-node and completed-checkpoint
no-op resume baselines. Stage 8 adds singleton interrupt-save and
interrupt-resume-plus-final-save baselines. Stage 9 adds the normal-node
single-box path, a ten-node shared-state child, two-level nesting, child
checkpoint/resume, and child interrupt/resume. These are regression baselines
without performance thresholds or cross-framework claims.

## Workspace

```text
.
├── AGENTS.md
├── Cargo.toml
├── README.md
├── rust-toolchain.toml
├── rustfmt.toml
└── crates
    └── group-agent-core
        ├── Cargo.toml
        ├── benches
        │   └── runtime.rs
        ├── examples
        │   ├── checkpoint.rs
        │   ├── conditional.rs
        │   ├── interrupt.rs
        │   ├── linear.rs
        │   ├── parallel.rs
        │   ├── resume.rs
        │   └── subgraph.rs
        ├── src
        │   ├── checkpoint.rs
        │   ├── context.rs
        │   ├── edge.rs
        │   ├── error.rs
        │   ├── event.rs
        │   ├── graph.rs
        │   ├── lib.rs
        │   ├── node.rs
        │   ├── path.rs
        │   ├── runtime.rs
        │   └── state.rs
        └── tests
            ├── compile_validation.rs
            ├── checkpointing.rs
            ├── conditional_routing.rs
            ├── execution_control.rs
            ├── interrupt.rs
            ├── linear_execution.rs
            ├── observability.rs
            ├── parallel_execution.rs
            ├── resume.rs
            ├── subgraph.rs
            └── review_regressions.rs
```

## Run

```bash
cargo test --workspace
cargo run -p group-agent-core --example linear
cargo run -p group-agent-core --example conditional
cargo run -p group-agent-core --example parallel
cargo run -p group-agent-core --example checkpoint
cargo run -p group-agent-core --example resume
cargo run -p group-agent-core --example interrupt
cargo run -p group-agent-core --example subgraph
cargo bench --workspace --no-run
```

## Current exclusions

This stage does not support parent/child State mapping, parent-frontier parallel
subgraphs, parallel interrupts, Replay, Fork, Time Travel, SQLite, PostgreSQL,
SQLx, snapshot or payload serialization, conditional or dynamic fan-out,
built-in Tokio channels or streams, standalone reducer registration, LLM or
tool APIs, MCP, RAG, token streaming, Tower middleware, Axum, HTTP services,
distributed workers, macro DSLs, or visualization. The event sink and
Checkpointer are adapter boundaries for later integrations; those excluded
capabilities are not implemented here.

## Architecture review cadence

After Stage 10, 20, 30, and every later multiple of ten, perform a full
repository architecture review before continuing feature stages. Corrective
stages such as Stage 5.1 do not count toward this ten-stage cadence.
After Stage 9 Review passes, Stage 10 is the next full-repository architecture
review.
