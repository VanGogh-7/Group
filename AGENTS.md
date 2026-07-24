# Group Repository Instructions

## Project identity

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. The
name borrows a metaphor from the hierarchy of algebraic structures: Group is the
execution foundation for higher-level agent frameworks. The project does not
implement or claim to implement a mathematical group.

## Current capability

The current core supports immutable compiled state graphs with:

- asynchronous trait-based nodes;
- sequential fixed edges and static fan-out transitions;
- parallel super-steps over one immutable state snapshot;
- fan-in barriers with deterministic frontier ordering and deduplication;
- explicit deterministic parallel update merging through `apply_batch`;
- optional checkpoint snapshots after successful super-step boundaries;
- asynchronous replaceable checkpointers and an in-memory implementation;
- checkpoint latest/history queries and CAS-protected state lineage by logical
  thread;
- synchronous conditional edges with declared target whitelists;
- explicit conditional loops guarded by `max_steps`;
- local success-only run reports with optional event retention;
- synchronous, thread-safe event sinks;
- cooperative cancellation with Tokio Util `CancellationToken`;
- optional run-level and per-node Tokio timeouts;
- per-run identifiers and typed failure lifecycle events;
- structured build, compile, node, state, route, and run errors;
- reusable and concurrently shareable compiled graphs.

## Mandatory state and execution boundaries

- `GraphState` is `Send + Sync + 'static`; it must not require `Clone`.
- A state defines one strongly typed `Update`.
- A `Node` receives only `&S` and `&NodeContext`.
- A `Node` returns `S::Update`; it never receives shared writable state.
- Only the Runtime calls `GraphState::apply`.
- Single-node super-steps call `GraphState::apply`. Multi-node super-steps call
  `GraphState::apply_batch` with source-tagged updates in stable node order.
- The default `apply_batch` rejects multiple updates before state mutation.
  Custom implementations must validate the complete batch before committing.
- State updates must be applied before successor routing.
- Routers are synchronous, read-only functions. Async work belongs in a node,
  which writes the routing decision into state through an update.
- Do not introduce `Arc<RwLock<State>>`, global run locks, or per-node
  `tokio::spawn`.
- Cancellation state belongs to `RunControl` and `NodeContext`, never
  `GraphState`.
- `GraphState` must not gain `Clone` or Serde bounds for checkpointing.
  Checkpoint-enabled state implements the separate `CheckpointState` capability.

## Graph responsibilities

- `StateGraph` is the mutable declaration builder. It owns node registrations,
  fixed-edge declarations, static fan-out declarations, conditional routers,
  and target whitelists.
- The compiler validates all identifiers, edge-shape constraints, possible
  reachability, and possible END reachability. It performs expensive resolution
  work once.
- `CompiledGraph` is immutable and reusable. It stores pre-resolved internal
  indices and must not expose `petgraph` or other internal cursor types.
- Runtime invocation owns its input state, events, visited nodes, and step count.
  It also owns the active frontier and per-super-step update collection.
  Separate invocations must remain isolated, including concurrent invocations.
- Every lifecycle event carries a `RunId`. A shared sink must be able to
  distinguish concurrent invocations without receiving state or update values.

## Observability boundaries

- `EventSink` is a synchronous, infallible `Send + Sync` callback.
- The Runtime delivers each event to the configured sink when the event occurs.
- Successful-report retention and sink delivery are independent. Default
  invocation behavior retains all events for compatibility.
- Sink callbacks run inline and must remain lightweight and non-blocking.
- A sink panic propagates directly. It is not converted into `GraphRunError`,
  and later event delivery is not guaranteed after the panic.
- Failed invocations return `GraphRunError`, not `RunReport`. Before returning,
  the Runtime emits all reached lifecycle events followed by `RunFailed`.
- `RunFailed` contains a typed stable failure classification and execution
  context. It does not copy state, updates, or stringify the error source chain.
- There is no built-in channel or stream adapter in this stage.
- The four valid configurations are All/no Sink, All/Sink, None/Sink, and
  None/no Sink. The last configuration skips event construction.
- Stage 3 added `RunId` to every `GraphEvent`; variant construction and exact
  named-field matching are breaking changes from Stage 2.

## Execution control boundaries

- Group is Tokio-native from Stage 4 onward. Use Tokio time and Tokio Util
  `CancellationToken`; do not implement custom executors, timers, tokens, or
  polling threads.
- `RunControl` accepts an optional external cancellation token, run timeout, and
  per-node timeout. Its default has no external token and no timeout.
- Run timeout starts at invocation entry. Node timeout starts immediately before
  `NodeStarted` delivery.
- Runtime checks control after `RunStarted`, before each frontier node, while
  each future is pending, after observed node completion, and before successor
  execution or `RunCompleted`.
- Synchronous checks and asynchronous waiting must share one deadline selector:
  choose the earlier absolute run or node deadline, and choose run on equality.
  Classification must remain correct even when both are expired before Runtime
  polling resumes.
- Cancellation precedes the selected timeout, which precedes a simultaneously
  ready node result. Equal deadlines therefore preserve cancellation, run
  timeout, node timeout, then node result. Cancellation and run timeout precede
  `max_steps` at node boundaries.
- Cancellation and timeout drop the in-flight node future without spawning it.
  Dropping a future does not roll back external side effects.
- In a parallel super-step, node errors, cancellation, and timeouts drop all
  remaining futures and discard every uncommitted update from that super-step.
  The first failure observed by Runtime wins.
- `GraphState::apply`, `GraphState::apply_batch`, routers, and sink callbacks are
  synchronous and cannot be preempted. Runtime observes control after they
  return.
- Control failures emit exactly one typed `RunFailed`, do not emit
  `RunCompleted`, and return structured `GraphRunError`.

## Transition and super-step semantics

- `START` has exactly one fixed successor and cannot use conditional routing.
- `END` has no outgoing edge.
- Each executable node has exactly one transition kind: one fixed successor,
  one static fan-out, or one conditional router.
- Static fan-out declares a non-empty, duplicate-free target set. Conditional
  fan-out is not supported.
- Runtime maintains an active frontier. Every node in one multi-node frontier
  borrows the same immutable state and is polled concurrently without
  `tokio::spawn`.
- Runtime waits for all frontier nodes before applying updates or routing.
- Successors are sorted and deduplicated by compiled internal index, so fan-in
  targets execute once and ordering never depends on completion order.
- An `END` successor ends only its branch. Execution completes when the next
  frontier is empty.
- A conditional router declares a non-empty, duplicate-free target whitelist.
- Every allowed target must resolve during compilation.
- Runtime calls a conditional router only after the source update is applied.
- For a parallel frontier, Runtime calls routers after the complete batch is
  applied, in stable source order.
- A router result outside the whitelist is a structured run error.
- Conditional routes may revisit nodes. START and END do not count as steps.
- `max_steps = N` permits at most N real node executions.
- Runtime never executes a partial parallel frontier to consume the remaining
  step budget.

## Parallel event semantics

- Multi-node frontiers emit `SuperstepStarted` and `SuperstepCompleted`;
  singleton frontiers retain the earlier sequential event sequence.
- `NodeStarted` is emitted in stable compiled node order.
- `NodeCompleted` follows observed future-completion order and may vary.
- `StateUpdated` is emitted only after a successful batch commit and in stable
  node order.
- `SuperstepCompleted` is emitted only after batch commit and all routing
  succeed. When checkpointing requires a save at that boundary, the save must
  also succeed first.

## Checkpoint boundaries

- Checkpointing is opt-in. Normal invocation must not create a snapshot, enter a
  checkpointer method, or acquire a checkpoint lock.
- `CheckpointState` defines a separate `Snapshot`, snapshot creation, and future
  restoration boundary without changing `GraphState`.
- Snapshot values are retained through `Arc`; latest/history queries must not
  deep-copy them.
- Save only after all frontier nodes, state commit, and successor routing
  succeed. Save before entering the next frontier.
- `EverySuperstep` saves once per successful super-step. `FinalOnly` saves only
  the completed empty-frontier checkpoint.
- A legal `START -> END` graph saves exactly one completed checkpoint under
  either policy, with super-step and step zero and an empty frontier.
- Checkpoints retain checkpoint/thread/run identifiers, parent, super-step,
  cumulative step count, next frontier, Snapshot, and completed state.
- Parent means the state lineage used by execution, not storage insertion
  order. A new-state configuration explicitly expects no parent; a run based on
  a checkpoint must supply that checkpoint as `expected_parent`.
- Runtime carries the last successful checkpoint within a run as the next
  expected parent. Checkpointers atomically compare thread latest with
  `expected_parent` before insertion. A mismatch is `CheckpointConflict`, so
  concurrent runs on one `ThreadId` do not silently cross-link parent chains.
- `CheckpointRequest` carries a Runtime-assigned `CheckpointId` operation key.
  Checkpointers should save idempotently because a dropped save future may have
  committed an external side effect before returning.
- Snapshot logic is synchronous and cannot be preempted. It runs before
  entering storage and never while an in-memory store lock is held.
- Cancellation and run timeout remain active while save is pending, with
  cancellation before run timeout before save result. Save-boundary control
  failures use no node identifier and the cumulative completed step count.
- Snapshot, conflict, save, cancellation, or timeout failure emits one
  `RunFailed`, no `CheckpointSaved` for an unconfirmed save, and no
  `RunCompleted`. Runtime does not claim to roll back committed state, storage
  effects, or external node side effects.
- Stage 6 provides no resume, replay, fork, time travel, database persistence,
  or serialization. Future resume work must validate graph version and topology
  compatibility in addition to stored frontier metadata.

## Performance principles

- Aggregate fixed edges, fan-out targets, conditional routers, source counts,
  and successor presence once during compilation and reuse the result across
  validation and transition compilation. Compilation should remain
  approximately O(V + E).
- Use internal indices on the Runtime hot path.
- Fixed and fan-out transitions must remain pre-resolved to internal indices.
- Frontier deduplication must operate on produced successors and must not scan
  every compiled graph node.
- Do not clone complete state values per step.
- Do not use a global Mutex or RwLock for execution.
- Do not spawn nodes; use in-task future concurrency for a super-step.
- One `Arc<dyn Node<S>>` dispatch and the `async-trait` boxed future are accepted
  costs at this stage.
- When no external token or timeout is configured, node execution should retain
  a direct-await fast path with only small control-state branches.
- `EventRetention::None` without a sink should avoid constructing events.
- Disabled checkpointing must avoid snapshot creation, storage calls, and lock
  acquisition. Enabled frontier metadata must inspect only produced successors.
- `InMemoryCheckpointer` may lock only briefly while saving or cloning query
  results; never execute user Snapshot code while holding its mutex.
- Performance conclusions require repeatable benchmarks. Do not make comparative
  performance claims from architecture alone.

## Out of scope

Do not add resume, replay, fork, time travel, human interrupts, SQLite,
PostgreSQL, SQLx, forced Serde bounds, conditional or dynamic fan-out, built-in
Tokio channels or streams, LLM providers, tool calling, MCP, RAG, token
streaming, standalone reducer registration, subgraphs, Tower middleware, Axum,
distributed workers, macro DSLs, or a visual interface unless a later stage
explicitly authorizes them. Do not create placeholder crates for future
capabilities.

## Rust and code standards

- Use Rust 2024 edition and preserve the declared MSRV unless a stage explicitly
  changes it.
- Unsafe code is forbidden.
- Keep code comments and public API documentation in English.
- Use typed public errors, preserve source chains, and do not introduce
  `anyhow::Error` into framework APIs.
- Avoid lints that make ordinary development needlessly difficult.
- Keep public `NodeId` values readable and cheap to clone; never expose internal
  graph indices.

## Required validation

Run these commands after implementation changes:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p group-agent-core --example linear
cargo run -p group-agent-core --example conditional
cargo run -p group-agent-core --example parallel
cargo run -p group-agent-core --example checkpoint
cargo bench --workspace --no-run
cargo check --workspace --all-targets --all-features
```

Also validate the declared MSRV:

```bash
cargo +1.85.0 check --workspace --all-targets --all-features
cargo +1.85.0 test --workspace
```

Run benchmarks for measurements with:

```bash
cargo bench --workspace
```

## Codex implementation and review workflow

Development Codex must read this file, the current README, source, and tests
before changing behavior. It should preserve stage scope, implement the smallest
coherent change, add semantic tests, run all required validation, and report
public API changes and unverified checks.

Review Codex should default to read-only inspection unless explicitly asked to
fix issues. It must separate confirmed passes from unexecuted checks, verify
state ownership and event ordering, inspect error source chains and compiled hot
paths, and ground findings in current repository files rather than planned
features.

After every stage, update both `AGENTS.md` and `README.md` so repository guidance,
examples, supported features, and exclusions match the implementation.

After Stage 10, 20, 30, and every later multiple of ten, perform a full
repository architecture review before starting the next feature stage.
Corrective stages such as Stage 5.1 do not count toward this cadence.
