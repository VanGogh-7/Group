# Group

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. Its
name borrows the metaphor of a hierarchy of algebraic structures: Group is
intended to be the execution foundation beneath higher-level agent frameworks.
It does not claim to implement a mathematical group.

## Current stage

Stage 6 adds an opt-in checkpoint foundation to the parallel super-step Runtime:

```text
START -> prepare -> [local_search, web_search] -> synthesis -> END
                  successful boundary -> Checkpoint
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
- latest and ordered history queries with checkpoint parent chains;
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
order. Each entry exposes its source `NodeId` and update:

```rust
fn apply_batch(
    &mut self,
    updates: Vec<NodeUpdate<Self::Update>>,
) -> Result<(), StateError> {
    // Validate the entire batch without mutating self.
    let validated = updates
        .iter()
        .map(|item| validate(item.node_id(), item.update()))
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

`restore` reserves the state boundary required by a later Resume stage. Stage 6
does not call it and cannot resume, replay, fork, or time-travel.

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

`CheckpointPolicy::EverySuperstep` saves after every successful super-step.
`FinalOnly` creates only the final completed checkpoint. Each immutable
`Checkpoint` records its `CheckpointId`, `ThreadId`, `RunId`, parent,
super-step, cumulative step count, shared `Arc<Snapshot>`, stable next frontier,
and completed flag. `Checkpointer::latest` and `history` return shared
`Arc<Checkpoint<_>>` values, so queries do not deep-copy snapshots. History is
ordered oldest to newest.

The exact save boundary is:

1. every node in the frontier succeeds;
2. `apply` or `apply_batch` commits successfully;
3. all successor routing succeeds and the next frontier is stable;
4. the user snapshot is created outside storage locks;
5. the checkpointer saves it before Runtime enters the next super-step.

A failing node, batch merge, state apply, or router does not create a checkpoint
for that super-step. Snapshot and save failures return structured
`GraphRunError::SnapshotFailed` or `CheckpointSaveFailed`, emit one final
`RunFailed`, and stop execution. State already committed at the boundary and
external node side effects are not rolled back.

`InMemoryCheckpointer` joins the new checkpoint to the latest checkpoint for
the same `ThreadId` while holding one short process-local mutex. Snapshot
creation never occurs under that lock. It provides no database durability or
serialization. See
[`examples/checkpoint.rs`](crates/group-agent-core/examples/checkpoint.rs).

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
Together with topology construction and reachability traversal, compilation
targets approximately O(V + E). It resolves all transitions and conditional
target whitelists to internal graph indices. Runtime execution uses those
indices directly. Fixed transitions remain O(1); fan-out targets are already
resolved. Frontier sorting and deduplication operate only on produced successor
indices, not by scanning the complete graph. Internal `petgraph` types remain
private.

Each invocation owns its state, frontier, events, visited-node list, and step
counter. The Runtime does not clone complete states, take a global execution
lock, spawn each node, create mandatory channels, or repeat full graph
validation. `GraphState` does not require `Clone`; `RunReport<S>` is cloneable
only when `S: Clone`.

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
These are regression baselines without performance thresholds or
cross-framework claims.

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
        │   ├── linear.rs
        │   └── parallel.rs
        ├── src
        │   ├── checkpoint.rs
        │   ├── context.rs
        │   ├── edge.rs
        │   ├── error.rs
        │   ├── event.rs
        │   ├── graph.rs
        │   ├── lib.rs
        │   ├── node.rs
        │   ├── runtime.rs
        │   └── state.rs
        └── tests
            ├── compile_validation.rs
            ├── checkpointing.rs
            ├── conditional_routing.rs
            ├── execution_control.rs
            ├── linear_execution.rs
            ├── observability.rs
            ├── parallel_execution.rs
            └── review_regressions.rs
```

## Run

```bash
cargo test --workspace
cargo run -p group-agent-core --example linear
cargo run -p group-agent-core --example conditional
cargo run -p group-agent-core --example parallel
cargo run -p group-agent-core --example checkpoint
cargo bench --workspace --no-run
```

## Current exclusions

This stage does not support Resume, Replay, Fork, Time Travel, human interrupts,
SQLite, PostgreSQL, SQLx, snapshot serialization, conditional or dynamic
fan-out, built-in Tokio channels or streams, standalone reducer registration,
LLM or tool APIs, MCP, RAG, token streaming, subgraphs, Tower middleware, Axum,
HTTP services, distributed workers, macro DSLs, or visualization. The event
sink and Checkpointer are adapter boundaries for later integrations; those
excluded capabilities are not implemented here.

## Architecture review cadence

After Stage 10, 20, 30, and every later multiple of ten, perform a full
repository architecture review before continuing feature stages. Corrective
stages such as Stage 5.1 do not count toward this ten-stage cadence.
