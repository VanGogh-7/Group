# Group

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. Its
name borrows the metaphor of a hierarchy of algebraic structures: Group is
intended to be the execution foundation beneath higher-level agent frameworks.
It does not claim to implement a mathematical group.

## Current stage

Stage 15.2 hardens SQLite reads for explicit writable Forks and independent
branch heads. Resume
continues the latest head of either the default lineage or a selected branch;
Replay remains strictly read-only; Fork creates a new `BranchId` from one exact
historical checkpoint. The Stage 10.1 Record/Codec/content-idempotency contract
remains unchanged:

```text
START -> prepare -> [local_search, web_search] -> synthesis -> END
                  conditional router -> [selected targets, END]
                  successful boundary -> Checkpoint
                  node interrupt -> Interrupted Checkpoint -> Resume value
historical Checkpoint -> read-only Replay -> no checkpoint writes
historical Checkpoint -> explicit Fork(BranchId) -> independent branch head
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

The current workspace includes:

- asynchronous trait-based nodes;
- fixed edges, static and conditional fan-out, fan-in barriers, and conditional
  target whitelists;
- concurrent node futures without per-node task spawning;
- explicit, deterministic parallel state-update merging;
- opt-in snapshots and asynchronous replaceable checkpoint storage;
- storage-neutral `CheckpointRecord` values and explicit Snapshot/payload codecs;
- record-backed, thread-safe `InMemoryCheckpointer`;
- a production SQLite `CheckpointStore` adapter built on SQLx with embedded
  migrations, WAL defaults, transactional idempotency, and lineage CAS;
- latest and ordered history queries with CAS-protected checkpoint lineage;
- restoration of state, frontier, cumulative step, and super-step position;
- explicit graph-version compatibility and latest-only resume checks;
- read-only replay from an explicit historical checkpoint without lineage
  writes or implicit Fork;
- explicit forks from historical checkpoints with independent branch
  head/history queries and CAS-protected branch continuation;
- typed interrupt payloads and resume values without Serde bounds;
- interrupted checkpoints and completed-or-interrupted execution outcomes;
- shared-state `CompiledGraph<S>` mounting through `add_subgraph`;
- structured `GraphPath` and `NodePath` namespaces for nested execution;
- subgraph-aware events, errors, checkpoints, resume, and interrupts;
- explicit loops protected by a per-run `max_steps`;
- immutable, reusable, concurrently shareable compiled graphs;
- immediate lifecycle delivery through a thread-safe `EventSink`;
- an optional bounded Tokio broadcast adapter with explicit per-subscriber lag;
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
Parent/child State mapping and conditional fan-out directly into a subgraph
mount are not implemented. Conditional fan-out inside a child remains valid
when it selects ordinary child nodes or `END`. See
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

An executable node has exactly one fixed, static fan-out, single-target
conditional, or conditional fan-out transition. `START` continues to require
exactly one fixed successor.

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

// AgentCheckpointCodec implements CheckpointCodec<AgentSnapshot>.
let store = Arc::new(InMemoryCheckpointer::new(AgentCheckpointCodec));
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

### Durable record and codec boundary

`CheckpointRecord` is the storage-neutral persistence model. It contains a
separate `CheckpointFormatVersion`, checkpoint/thread/run/parent identifiers,
optional `GraphVersion`, fixed-width `u64` cumulative step and super-step,
structured
`NodePath` frontier, completion/interrupt metadata, encoded Snapshot bytes, and
optional encoded interrupt payload bytes. `CheckpointRecordParts` and
`CheckpointRecord::try_from_parts` let an external backend reconstruct and
validate records without private Runtime constructors.

`CheckpointCodec<T>` supplies a stable Snapshot `CodecDescriptor`
(`payload schema + schema version + codec/encoding identity`), byte
encode/decode methods, and optional durable interrupt payload methods. Thus
JSON, bincode, and other encodings cannot collide merely because they reuse a
schema name and version. Descriptor mismatch is rejected before decoding.
`EncodedValue` equality includes the complete descriptor and bytes.

The codec must emit deterministic, canonical bytes for the same logical value;
otherwise stable-content idempotency cannot be guaranteed. It does not require
`GraphState`, Snapshot, or payload types to implement Serde or Clone. Codec
calls are synchronous and always occur outside store locks. Format, descriptor,
counter-conversion, and decode failures are structured and retain complete
codec source chains.

Typed Runtime counters remain `usize`. Record reconstruction converts both
`u64` counters with `usize::try_from` and returns a structured incompatibility
when the current target cannot represent a value; it never silently truncates.

`CheckpointStore` is the asynchronous record port for third-party durable
backends. `RecordCheckpointer<T>` combines a store and codec into the typed
Runtime `Checkpointer<T>` boundary. `InMemoryCheckpointStore` implements the
same record CAS/idempotency contract; `InMemoryCheckpointer<T>` is its
convenient typed adapter. The in-memory store remains process-local, but its
records can be exported and reconstructed by a fresh store/adapter instance.

### SQLite durable checkpoint backend

`group-agent-checkpoint-sqlite` is an independent workspace crate. It depends
on `group-agent-core`; Core does not depend on SQLx or the SQLite adapter.
Applications continue to provide their own `CheckpointCodec`:

```rust
use std::sync::Arc;

use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{CheckpointCodec, CheckpointStore, RecordCheckpointer};

let sqlite = Arc::new(
    SqliteCheckpointStore::connect("sqlite://group-checkpoints.sqlite3").await?,
);
sqlite.migrate().await?;

let records: Arc<dyn CheckpointStore> = sqlite;
let checkpointer = Arc::new(RecordCheckpointer::new(
    records,
    Arc::new(AppCheckpointCodec) as Arc<dyn CheckpointCodec<AppSnapshot>>,
));
```

`connect` creates a missing file database and configures foreign keys, WAL
journaling, and a five-second busy timeout. `from_pool` accepts an
application-managed `sqlx::SqlitePool`; in that case the application owns those
connection settings. `migrate` uses migrations embedded in the crate, is safe
to repeat, and does not require `DATABASE_URL` while compiling.

The schema has an append-only checkpoint-record table and one current-head row
per `ThreadId`. UUID-backed identifiers use their exact 16-byte form. `step`
and `superstep` use eight-byte big-endian blobs, which represent every `u64`
without conversion through SQLite's signed integer type and retain
lexicographic sort order. Structured `NodePath` segments are stored as an
adapter-private JSON DTO; Snapshot and interrupt descriptors and bytes are
stored in separate lossless columns. Serde is therefore private to this
adapter and imposes no bound on `GraphState`, Snapshot, or Update.

Each save starts `BEGIN IMMEDIATE` and performs idempotency lookup before
lineage CAS, then inserts the record and advances the thread head in the same
SQLx-tracked transaction. Exact old-operation replay succeeds even after the
head advances. Reusing an ID with different stable Record content returns
`IdempotencyConflict`; two writers from one parent cannot create an implicit
Fork. SQLite busy/lock/database errors remain source-preserving storage errors,
not lineage conflicts. User Codec, snapshot, restore, and other application
code never executes inside the database transaction.

SQLx is pinned to `0.8.6` with only the Tokio, SQLite, migration, and macro
features. The 0.8 release line supports an MSRV below Group's Rust 1.85 floor;
the workspace validates the adapter with Rust 1.85. SQLx 0.9 is not selected
because it requires a newer compiler. File-database restart tests destroy the
original pool, typed checkpointer, checkpoint, and Snapshot handles, then
reconnect and reconstruct ordinary, conditional fan-out, nested-subgraph, and
durable-interrupt executions from records alone.

`CheckpointId`, `InterruptId`, and `RunId` are UUID v4-backed values. They
support display, parsing, hashing, and stable 16-byte reconstruction and do not
depend on process-local counters. `CheckpointFormatVersion` is independent of
`GraphVersion`: the former versions the record layout, while the latter
versions the complete graph and state semantics.

Graphs intended for checkpoint resume or replay must have an explicit
compatibility version before compilation:

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
not the previous insertion by wall-clock order. Each record carries both a
Runtime-assigned `CheckpointId` idempotency key and an `expected_parent`.
`CheckpointStore::save` compares the thread's latest record with that expected
parent and inserts atomically. A mismatch returns
`GraphRunError::CheckpointConflict`; it never silently joins unrelated runs.
Consequently, concurrent runs using the same `ThreadId` and base normally race:
the first accepted write advances the lineage and the other conflicts.
Different thread identifiers remain isolated. Within one run, every successful
save becomes the expected parent of that run's next save.

The exact save boundary is:

1. every node in the frontier succeeds;
2. `apply` or `apply_batch` commits successfully;
3. all successor routing succeeds and the next frontier is stable;
4. the user snapshot is created and encoded outside storage locks;
5. the checkpointer atomically stores the record before Runtime enters the next
   super-step.

A failing node, batch merge, state apply, or router does not create a checkpoint
for that super-step. Snapshot, encoding, conflict, and storage failures return
structured `GraphRunError::SnapshotFailed`, `CheckpointEncodeFailed`,
`CheckpointConflict`, or `CheckpointSaveFailed`, emit one final `RunFailed`,
and stop execution. State already committed at the boundary and external node
side effects are not rolled back. Record reconstruction failures remain
structured sources of `CheckpointLoadFailed`.

Run cancellation and run timeout remain active while the asynchronous save
future is pending. Cancellation has priority over run timeout, and both have
priority over a simultaneously ready save result. Such failures use checkpoint
boundary context (`node_id = None`, with the cumulative completed step count),
emit exactly one `RunFailed`, and emit neither `CheckpointSaved` nor
`RunCompleted`. Dropping a save future cannot prove that a backend produced no
side effect: storage may have committed before its future returned. Custom
stores must therefore treat `CheckpointRecord::id()` as an idempotency key.
An exact replay with identical stable record content returns the original
record even if latest has advanced; Snapshot or payload `Arc` identity is
irrelevant. Reusing the same ID with different bytes, lineage, format/schema
version, graph version, frontier, completion, or interrupt metadata returns
`CheckpointWriteError::IdempotencyConflict`. Idempotency lookup precedes parent
CAS, and both checks plus insertion are atomic. `InMemoryCheckpointStore`
implements this contract.

Snapshot creation and codec work are synchronous and cannot be preempted. They
occur before entering storage and never under the in-memory store lock. A legal
`START -> END` graph saves exactly one completed checkpoint under either
policy, with `superstep = 0`, `step = 0`, an empty frontier, and the configured
expected parent. Its successful terminal order is `CheckpointSaved` followed
by `RunCompleted`. The in-memory implementation provides no database
durability.
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
`ResumeConflict` is returned; selecting an older checkpoint never implicitly
creates a Fork. With `with_branch_id`, “latest” and the same explicit-target
check are scoped to that branch head instead of the default thread head. The
Runtime validates ThreadId, latest-only status, explicit graph version,
completed/frontier consistency, and every saved frontier `NodePath`, resolving
it to compiled internal indices in O(F). `START`, explicit
`END`, unknown or invalid namespaced nodes, unversioned data, and version
mismatches produce
`CheckpointIncompatible`. The frontier must also contain no duplicate path, be
ordered by compiled internal index, and remain within one `GraphPath`
namespace. These checks traverse only the actual frontier, do not scan the
compiled graph, and occur before `CheckpointState::restore`.

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

## Read-only replay from history

`ReplayConfig` requires an exact `ThreadId` and `CheckpointId`; Replay never
falls back to latest selection:

```rust
let replay = compiled
    .replay(
        ReplayConfig::new(
            "conversation-42",
            historical_checkpoint_id,
            Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
        )
        .with_run_config(RunConfig::new(100))
        .with_event_config(EventConfig::default())
        .with_control(RunControl::default()),
    )
    .await?;
```

Replay loads that checkpoint through `Checkpointer::get`, validates the same
GraphVersion, completion, interrupt metadata, and canonical O(F) frontier
rules as Resume, and restores State outside storage locks. It then assigns a
new `RunId` and continues from the checkpoint's cumulative step, super-step,
and resolved internal frontier using the normal execution kernel.
`RunConfig::max_steps` is an additional node budget for this replay call.
A completed checkpoint is restored and returns a no-op `ReplayReport`.

Unlike Resume, Replay does not require the checkpoint to be latest and never
constructs a writable checkpoint configuration. It performs no checkpoint
save, parent CAS, head update, history insertion, or implicit branch creation.
The original thread may advance concurrently without affecting Replay. Its
successful event order begins `RunStarted -> ReplayStarted`, followed by any
continued subgraph/node events and `RunCompleted`. Preparation and execution
failures emit exactly one `RunFailed`.

An interrupted source checkpoint requires a correctly typed Resume value, and
a normal checkpoint rejects an unexpected value. If a replayed node interrupts
again, execution fails with `ReplayInterruptUnsupported`; read-only Replay
cannot save a new interrupted checkpoint and emits no `RunInterrupted`.

Replay is not Fork: it returns an in-memory `ReplayReport` and creates no
branch head or durable descendant. It also re-executes node code. Database
writes, network requests, tool calls, and other external side effects may
therefore occur again. Runtime provides no rollback, sandbox, or automatic
deduplication. See
[`examples/replay.rs`](crates/group-agent-core/examples/replay.rs).

## Explicit fork and branch heads

`ForkConfig` requires an exact source `ThreadId` and `CheckpointId`. It assigns
a new `BranchId` by default (or accepts an application-selected one), validates
and restores the source checkpoint using the same O(F) frontier rules as
Resume/Replay, creates the branch head at that source, and then reuses the
normal execution kernel:

```rust
let config = ForkConfig::new(
    thread_id.clone(),
    historical_checkpoint_id,
    checkpointer.clone(),
);
let branch_id = config.branch_id();
let fork = compiled.fork(config).await?;

let branch_history = checkpointer
    .branch_history(&thread_id, branch_id)
    .await?;
```

The source checkpoint need not be latest. Creating or advancing a branch never
changes the default thread head/history or another branch. Branch history
starts with the shared source checkpoint, followed by records written only to
that branch. Each descendant retains the ordinary `CheckpointRecord::parent_id`
chain; branch ownership and the branch head are additive Store metadata rather
than new Record fields.

Branch Resume is latest-only and explicit:

```rust
let outcome = compiled
    .resume(
        ResumeConfig::new(thread_id, checkpointer)
            .with_branch_id(branch_id),
    )
    .await?;
```

The Store applies idempotency before an independent branch-head CAS. Concurrent
writers based on one branch head therefore allow only one successor; they
cannot create an implicit fork. `CheckpointConfig::with_branch_id` routes
checkpoint-enabled execution to the same branch CAS and therefore requires an
`expected_parent` that is the current head of that exact branch.
`ForkStarted` identifies the new run, source checkpoint, historical counters,
and branch; branch Resume also emits `BranchResumed`. Interrupts, nested
subgraphs, conditional fan-out, and completed no-op checkpoints retain their
existing Runtime semantics.

`CheckpointStore` and `Checkpointer<T>` expose additive `create_branch`,
`save_branch`, `branch_head`, and `branch_history` capabilities. The in-memory
and SQLite adapters implement them. A `BranchId` has one owning `ThreadId`.
Duplicate `create_branch` calls return `BranchAlreadyExists`; they are not
idempotent success, including when the caller repeats the same source. An
absent branch, or a branch queried through the wrong thread, makes
`branch_head` return `None` and `branch_history` return an empty collection.

Branch creation is atomic: a load, validation, restore, cancellation, timeout,
or `create_branch` failure before successful creation leaves no Branch. Once
creation succeeds, a later node, control, routing, snapshot, encoding, CAS, or
storage failure keeps the Branch at its last confirmed head. In particular, a
Fork that fails before its first successful descendant save retains the source
checkpoint as its head and can be continued by explicit branch Resume.

SQLite migrations `0002_branch_heads.sql`, `0003_branch_ownership.sql`, and
`0004_branch_read_consistency.sql` persist branch metadata separately. The
ownership migration adds composite ThreadId constraints for source, head, and
membership. The consistency migration adds a branch-first membership index and
triggers requiring an initial source head, membership for every non-source
head, and a parent-continuous membership insertion. A branch save updates its
Record, membership row, and head in one `BEGIN IMMEDIATE` transaction, so any
failure rolls back all three.

SQLite `branch_head` and `branch_history` each use one read transaction and one
JOIN-based record query scoped by both `thread_id` and `branch_id`. The shared
decoder verifies source and head ownership, requires a non-source head to be a
member, and validates the complete stable `source -> descendants -> head`
parent chain before returning any Record. Missing, cross-thread, non-member,
duplicate, or discontinuous data returns a structured corruption error.
Concurrent saves therefore cannot expose a mixed metadata/Record snapshot.
File-database restart tests reconstruct branches without process caches.

Fork starts from the exact historical State and does not accept a State patch.
There is no branch merge, branch deletion, or implicit branch selection. See
[`examples/fork.rs`](crates/group-agent-core/examples/fork.rs).

Resume, Replay, and Fork remain separate operations: Resume continues only the
latest selected lineage, Replay executes one exact historical checkpoint
without any write, and Fork is the only operation that creates a new writable
branch.

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
super-step's updates, and returns `UnsupportedParallelInterrupt`. Payloads
created by `InterruptRequest` are typed and process-local until the configured
`CheckpointCodec` provides a durable encoding. A record-backed write with an
unsupported payload fails explicitly with `CheckpointEncodeFailed`; it never
silently drops the payload. See
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
Core intentionally contains no channel or stream implementation. Applications
that want a Tokio stream can depend on the separate
`group-agent-observability-tokio` crate:

```rust
use group_agent_core::{EventConfig, EventRetention};
use group_agent_observability_tokio::EventBroadcast;
use tokio_stream::StreamExt;

let events = EventBroadcast::new(256)?;
let mut stream = events.subscribe();
let config = EventConfig::new(EventRetention::None).with_sink(events.sink());

let report = compiled
    .invoke_with_events(initial_state, RunConfig::default(), config)
    .await?;
drop(events);

while let Some(item) = stream.next().await {
    match item {
        Ok(event) => observe(event.run_id(), event),
        Err(error) => record_gap(error),
    }
}
```

`EventBroadcast::new` rejects capacity zero and capacities that cannot be
safely represented instead of allowing Tokio's constructor to panic. It uses
`checked_next_power_of_two`; `capacity()` returns the effective power-of-two
capacity of Tokio broadcast's shared bounded ring buffer. For example, a
requested capacity of three has an effective capacity of four. Its sink
performs one synchronous Tokio broadcast send and never awaits or blocks for
capacity. When a subscriber falls behind, overwritten events are reported as
`EventStreamError::Lagged { skipped }`; the stream can then continue with newer
events. Lag is never hidden as a complete event history.

Each call to `subscribe` starts at that instant and has an independent cursor,
so it receives no earlier events. Multiple subscribers can observe the same
subsequent events independently. A stream ends only after every sender
(`EventBroadcast` and all sink handles) is dropped and its buffered events are
drained. Having no subscribers, or dropping subscribers, never fails graph
execution.

`EventRetention` controls only successful `RunReport` storage; stream delivery
is controlled by the Sink and remains active with `EventRetention::None`.
Events from concurrently executing runs may interleave globally, while the
existing synchronous sink callback preserves emission order within each run.
Use `GraphEvent::run_id()` to filter or group them. The broadcast adapter is
not a durable or reliable-delivery system: it provides no event-history replay,
asynchronous backpressure, disk queue, or network transport. This is
independent of graph checkpoint Replay. SQLite durability and event streaming
are independent optional capabilities.

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

Replay emits `ReplayStarted` only after exact historical loading, compatibility
validation, and successful restoration. It includes the new `RunId`, source
thread and checkpoint, and historical step/super-step. Replay never emits
`CheckpointSaved`; an interrupt during replay ends with one typed `RunFailed`
instead of `RunInterrupted`.

Subgraph entry and successful exit emit `SubgraphStarted` and
`SubgraphCompleted` with a structured `GraphPath`. They share the parent
`RunId`; nested children do not emit additional top-level run events.

Single-target conditional routing emits `RouteSelected`. A successful
conditional fan-out decision emits exactly one `RoutesSelected` after the
complete result has been validated; its `targets` are in stable compiled order.
Router failure, an empty result, a duplicate result, or an undeclared target
emits no route-selection event and ends with structured `RunFailed` metadata.

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

### Durable checkpoint API migration in Stage 10.1

- `InMemoryCheckpointer::new` now requires a `CheckpointCodec<Snapshot>`.
- Durable backends implement the non-generic `CheckpointStore` record port and
  use `RecordCheckpointer<T>` for Runtime integration.
- `CheckpointRecord`, `CheckpointRecordParts`, `EncodedValue`, and
  `Checkpoint::from_record` are the public persistence/reconstruction boundary.
- `CodecDescriptor::new` now requires independent schema, schema-version, and
  encoding identities; use `schema_version()` instead of the old `version()`
  accessor. `EncodedValue` and record idempotency compare all three.
- Durable Record step and super-step fields are now `u64`; typed
  `Checkpoint<T>` and Runtime counters remain `usize` with checked
  reconstruction.
- `CheckpointId`, `InterruptId`, and `RunId` changed from numeric process-local
  counters to UUID-backed values. Use `Display`/`FromStr`, `from_bytes`, or
  `from_uuid`; numeric `get()` construction/access no longer applies.
- `CheckpointWriteError` can now report `Encoding`, and Runtime exposes
  `CheckpointEncodeFailed` plus the corresponding `RunFailure`.

### Replay API additions in Stage 14

- `ReplayConfig::new(thread_id, checkpoint_id, checkpointer)` always requires an
  exact source checkpoint.
- `CompiledGraph::replay` returns `ReplayReport`, never `ExecutionOutcome`,
  because a replay interrupt is a structured failure rather than a saved
  suspension.
- `GraphEvent::ReplayStarted`,
  `GraphRunError::ReplayInterruptUnsupported`, and the matching `RunFailure`
  classification are new public variants. `GraphEvent` and `RunFailure` remain
  non-exhaustive.

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

Conditional fan-out uses the same read-only, fallible boundary but returns one
or more targets:

```rust
graph.add_conditional_fan_out(
    "router",
    ["local", "web", "cache", END],
    |state: &AgentState| {
        let mut targets = vec![NodeId::from("local")];
        if state.needs_web {
            targets.push(NodeId::from("web"));
        }
        Ok(targets)
    },
)?;
```

Each executable node has exactly one fixed edge, static fan-out, single-target
conditional router, or conditional fan-out router. Both router forms run only
after the source update commits. A conditional fan-out result must be
non-empty, contain no duplicate `NodeId`, and remain within its whitelist.
Invalid results return structured `EmptyRouteTargets`,
`DuplicateRouteTarget`, or `InvalidRouteTarget` errors; duplicates are not
silently removed. `END` may appear beside ordinary targets and exits only the
source branch. One executable target forms a singleton frontier; multiple
targets form a parallel super-step. Targets are resolved to internal indices
and sorted into stable compiled order. Fan-in deduplication still ensures that
one downstream target executes once.

Conditional fan-out may currently select only ordinary nodes and `END`.
Declaring a subgraph mount in its whitelist is rejected at compile time. After
a parallel batch commits, routers for frontier nodes inspect the merged state
in stable node order. Async model, database, or tool work belongs in a node;
the node should write its result into state, and the router should only inspect
that updated state.

See
[`examples/conditional.rs`](crates/group-agent-core/examples/conditional.rs) for
an executable loop and
[`examples/conditional_fan_out.rs`](crates/group-agent-core/examples/conditional_fan_out.rs)
for dynamic multi-target selection.

## Runtime structure and performance policy

Public graph construction uses readable `NodeId` values backed by `Arc<str>`.
Compilation aggregates fixed successors, static fan-out targets, both
conditional router forms, source counts, and successor presence once, then
reuses that data for shape validation, outgoing-edge completeness, and
transition compilation.
Together with topology construction and reachability traversal, ordinary
compilation remains approximately O(V + E). Parents that combine subgraph
mounts with fan-out additionally run a composition-only reachable-frontier-pair
check so indirect mixed subgraph frontiers fail at compile time. It operates on
each produced frontier, removes `END` branches before co-activity checks, uses
structured identifiers rather than path strings, and does not affect graphs
without both features. Compilation resolves every target whitelist to internal
indices. One internal transition kernel handles fixed, single-target
conditional, static fan-out, conditional fan-out, and structural subgraph
enter/exit transitions after state commit. Fixed transitions remain O(1);
static targets are pre-sorted. Conditional fan-out processes only the router's
actual `T` targets and performs `O(T log T)` stable ordering without scanning
the graph.
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

Checkpointing adds no storage call, snapshot creation, codec work, or lock
acquisition to a normal invocation. Enabled runs construct only next-frontier
metadata and never scan the complete graph. Snapshot and codec cost are defined
entirely by user implementations.

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
without performance thresholds or cross-framework claims. Stage 10.1 adds UUID
v4 generation, controlled default/retention/checkpoint invocation cases, Record
encode/decode, and fresh-adapter record reconstruction plus Resume. Stage 11
adds a single-target conditional baseline, conditional fan-out at 2, 8, and 32
targets, isomorphic static fan-out cases in the same harness, and
checkpoint-plus-resume of a multi-node frontier.
Stage 13 adds one shared harness for no Sink, broadcast with no subscriber, one
subscriber, four subscribers, and `EventRetention::None` with one subscriber.
Stage 14 adds read-only replay from a middle checkpoint through one immediate
node, completed-checkpoint no-op replay, and replay of a two-node frontier.
Stage 15 adds a historical fork plus one immediate node in the same harness.
Stage 15.1 adds a branch Resume baseline and an independent SQLite
restart-plus-branch-Resume benchmark. Stage 15.2 runs the branch Resume
baseline against a real `InMemoryCheckpointStore` and `RecordCheckpointer`,
rather than a benchmark-only branch implementation. Criterion uses explicit
warm-up, measurement, sample-size, and noise-threshold settings. Results are
local regression baselines only; short-run variation is not a reason to
redesign the runtime.

## Workspace

```text
.
├── AGENTS.md
├── Cargo.toml
├── README.md
├── rust-toolchain.toml
├── rustfmt.toml
└── crates
    ├── group-agent-checkpoint-sqlite
    │   ├── Cargo.toml
    │   ├── migrations
    │   │   ├── 0001_checkpoint_store.sql
    │   │   ├── 0002_branch_heads.sql
    │   │   ├── 0003_branch_ownership.sql
    │   │   └── 0004_branch_read_consistency.sql
    │   ├── benches
    │   │   └── branch_restart.rs
    │   ├── src
    │   │   └── lib.rs
    │   └── tests
    │       ├── restart.rs
    │       └── store.rs
    ├── group-agent-observability-tokio
    │   ├── Cargo.toml
    │   ├── benches
    │   │   └── event_broadcast.rs
    │   ├── src
    │   │   └── lib.rs
    │   └── tests
    │       └── event_stream.rs
    └── group-agent-core
        ├── Cargo.toml
        ├── benches
        │   └── runtime.rs
        ├── examples
        │   ├── checkpoint.rs
        │   ├── conditional.rs
        │   ├── conditional_fan_out.rs
        │   ├── interrupt.rs
        │   ├── linear.rs
        │   ├── parallel.rs
        │   ├── fork.rs
        │   ├── replay.rs
        │   ├── resume.rs
        │   └── subgraph.rs
        ├── src
        │   ├── checkpoint.rs
        │   ├── checkpoint_codec.rs
        │   ├── checkpoint_record.rs
        │   ├── checkpoint_store.rs
        │   ├── context.rs
        │   ├── edge.rs
        │   ├── error.rs
        │   ├── event.rs
        │   ├── graph.rs
        │   ├── id.rs
        │   ├── lib.rs
        │   ├── node.rs
        │   ├── path.rs
        │   ├── runtime.rs
        │   ├── state.rs
        │   └── transition.rs
        └── tests
            ├── branch_store.rs
            ├── compile_validation.rs
            ├── checkpointing.rs
            ├── conditional_fan_out.rs
            ├── conditional_routing.rs
            ├── durable_checkpoint.rs
            ├── execution_control.rs
            ├── fork.rs
            ├── interrupt.rs
            ├── linear_execution.rs
            ├── observability.rs
            ├── parallel_execution.rs
            ├── replay.rs
            ├── resume.rs
            ├── subgraph.rs
            └── review_regressions.rs
```

## Run

```bash
cargo test --workspace
cargo run -p group-agent-core --example fork
cargo run -p group-agent-core --example replay
cargo run -p group-agent-core --example resume
cargo run -p group-agent-core --example interrupt
cargo bench --workspace --no-run
```

## Current exclusions

This stage does not support State patches during Fork, branch merge, branch
deletion, parent/child State mapping, parent-frontier parallel subgraphs,
parallel interrupts, Replay writes or historical State modification, Time
Travel, PostgreSQL, built-in Serde codecs, arbitrary Node Command or Send APIs,
conditional fan-out into subgraph mounts, custom asynchronous backpressure,
disk event queues, OpenTelemetry exporters, metrics exporters, WebSocket or SSE
servers, network event proxies, standalone reducer registration, LLM or tool
APIs, MCP, RAG, token streaming, Tower middleware, Axum, HTTP services,
distributed workers, macro DSLs, or visualization. SQLite is the only
reference database backend; the bounded Tokio stream adapter is process-local
and intentionally lossy.

## Architecture review cadence

After Stage 10, 20, 30, and every later multiple of ten, perform a full
repository architecture review before continuing feature stages. Corrective
stages such as Stage 5.1 do not count toward this ten-stage cadence.
Stage 9.1 Review has passed. Stage 10.1 supplied the durable-checkpoint contract
correction required by the Stage 10 architecture review. Stages 11 through 15
preserve that reviewed Record/Codec/content-idempotency contract; Stage 15 adds
branch metadata as a Store capability without changing `CheckpointRecord`.
