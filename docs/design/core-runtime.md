# Core Runtime Design

This document expands the current Core contract summarized in
[ARCHITECTURE.md](../../ARCHITECTURE.md). It is not a stage history.

## State, Node, and Update

`GraphState` is `Send + Sync + 'static` and defines one typed Update. It does
not require Clone or Serde. Runtime owns the only mutable State value for an
invocation.

A `Node` receives `&S` and `&NodeContext`, performs asynchronous work, and
returns `S::Update`. An `InterruptibleNode` explicitly returns either an
Update or an interrupt request. Nodes never receive shared writable State.

Only Runtime calls:

- `GraphState::apply` for a singleton super-step;
- `GraphState::apply_batch` for a parallel super-step.

The default batch implementation rejects more than one Update before
mutation. A custom reducer must validate the complete source-tagged batch
before committing.

## Declaration and compilation

`StateGraph` is the mutable builder. It owns Node registrations, subgraph
mounts, fixed edges, static fan-out, conditional routers, and declared target
whitelists.

Compilation performs work that should not recur on each invocation:

- identifier and reserved-node validation;
- one transition kind per executable Node;
- duplicate and unknown target checks;
- possible reachability and possible END reachability;
- subgraph flattening and structured path construction;
- transition target resolution to private internal indices;
- stable successor ordering.

`CompiledGraph` is immutable, reusable, and concurrently shareable. Public APIs
do not expose petgraph cursors or internal indices.

## Runtime and deterministic super-steps

Runtime owns State, RunId, events, visited attempts, counters, active frontier,
and pending Updates. Separate invocations share no mutable execution state.

For a parallel frontier:

1. Nodes borrow the same immutable State snapshot.
2. Runtime polls their Futures concurrently without per-Node `tokio::spawn`.
3. All results must succeed before any Update is committed.
4. Updates are restored to stable compiled-Node order.
5. `apply_batch` commits the complete batch.
6. Routers run in stable source order against the committed State.
7. Successors are sorted and deduplicated by compiled index.

Completion order may affect `NodeCompleted` event order, but never merge or
frontier order. A failure drops pending siblings and discards every
uncommitted Update in that super-step.

## Routing and loops

Each Node has exactly one transition shape:

- one fixed successor;
- one static fan-out;
- one single-target conditional router;
- one conditional fan-out router.

Routers are synchronous and read-only. Async routing work belongs in a Node,
which writes the decision into State through an Update. Routing always occurs
after State commit.

Conditional results must stay within a compile-time whitelist. Fan-out results
must be non-empty and duplicate-free. END exits only its source branch.

Conditional routes may revisit Nodes. `max_steps` bounds real Node executions,
and Runtime never executes part of a parallel frontier merely to consume a
remaining step budget.

## Shared-state subgraphs

Subgraphs use the same `GraphState`. A mount is structural: it executes no
Node, consumes no step, and follows the parent transition only after the child
reaches END.

`GraphPath` and `NodePath` are structured Arc-backed segments. Runtime does not
parse display strings to navigate. Child Nodes share the parent RunId,
control, events, checkpoint lineage, and counters.

The current contract forbids a subgraph mount beside another active executable
item in the same parent frontier. Parallel super-steps inside a child remain
valid.

## Cancellation and deadlines

`RunControl` carries an optional external `CancellationToken`, run timeout, and
per-Node timeout. Cancellation is invocation control, never State.

Run and Node timeout selection uses absolute deadlines. On ties the ordering is
cancellation, run timeout, Node timeout, then Node result. Runtime checks
control at invocation entry, Node boundaries, while Futures are pending, after
synchronous callbacks return, and before completion.

Timeout or cancellation drops the in-flight Future. It cannot preempt
`apply`, routers, codecs, or event callbacks, and it does not roll back
external side effects.

## Interrupts

Interrupt is supported only for a singleton frontier and requires
checkpointing. Runtime applies no Update and performs no normal routing. The
interrupted checkpoint retains unchanged committed State and the same Node as
its singleton frontier.

A typed resume value is visible through `NodeContext` only while re-executing
that Node. It is valid for one attempt and is neither State nor automatically
reused after a repeated interrupt.

## Prohibited shortcuts

The current design forbids:

- requiring full State Clone for execution or checkpointing;
- `Arc<RwLock<State>>` or a global run lock;
- one spawned task per Node;
- asynchronous routers;
- routing before Update commit;
- cancellation flags or resume values in State;
- exposing private graph indices.

## Direct evidence

Primary implementation and tests:

- `crates/group-agent-core/src/state.rs`
- `crates/group-agent-core/src/node.rs`
- `crates/group-agent-core/src/graph.rs`
- `crates/group-agent-core/src/runtime.rs`
- `crates/group-agent-core/tests/compile_validation.rs`
- `crates/group-agent-core/tests/parallel_execution.rs`
- `crates/group-agent-core/tests/execution_control.rs`
- `crates/group-agent-core/tests/subgraph.rs`

Related decisions:

- [ADR-002](../adr/002-immutable-state-updates.md)
- [ADR-003](../adr/003-deterministic-superstep-merge.md)

