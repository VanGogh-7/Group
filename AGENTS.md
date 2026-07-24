# Group Repository Instructions

## Project identity

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. The
name borrows a metaphor from the hierarchy of algebraic structures: Group is the
execution foundation for higher-level agent frameworks. The project does not
implement or claim to implement a mathematical group.

## Current capability

The current workspace provides an immutable compiled state-graph core with:

- asynchronous trait-based nodes;
- sequential fixed edges plus static and conditional fan-out transitions;
- parallel super-steps over one immutable state snapshot;
- fan-in barriers with deterministic frontier ordering and deduplication;
- explicit deterministic parallel update merging through `apply_batch`;
- optional checkpoint snapshots after successful super-step boundaries;
- asynchronous replaceable checkpointers and an in-memory implementation;
- an independent SQLx SQLite durable `CheckpointStore` with embedded
  migrations, file-restart recovery, transactional idempotency, and lineage CAS;
- checkpoint latest/history queries and CAS-protected state lineage by logical
  thread;
- latest-only resume from specified or latest versioned checkpoints;
- read-only replay from an explicitly selected historical checkpoint without
  changing thread head, history, or lineage;
- restoration of state, frontier, cumulative steps, and super-step position;
- typed node interrupts, interrupted checkpoints, and typed resume values;
- completed-or-interrupted execution outcomes for checkpoint-enabled runs;
- shared-state compiled subgraphs with nested execution namespaces;
- structured `GraphPath` and `NodePath` metadata across Runtime boundaries;
- subgraph-aware checkpoint, resume, interrupt, error, and event behavior;
- synchronous single-target and fan-out conditional edges with declared target
  whitelists;
- explicit conditional loops guarded by `max_steps`;
- local success-only run reports with optional event retention;
- synchronous, thread-safe event sinks;
- an independent bounded Tokio broadcast adapter with explicit subscriber lag;
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
- An `InterruptibleNode` explicitly returns `NodeOutcome::Update` or
  `NodeOutcome::Interrupt`; ordinary update-only nodes keep the simpler `Node`
  API.
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
- Resume values belong to `NodeContext`, never `GraphState`, and are visible
  only while re-executing the node retained by an interrupted checkpoint.
- A Resume value is valid for one re-execution attempt. A repeated interrupt
  neither persists nor automatically reuses the old value.
- `GraphState` must not gain `Clone` or Serde bounds for checkpointing.
  Checkpoint-enabled state implements the separate `CheckpointState` capability.

## Graph responsibilities

- `StateGraph` is the mutable declaration builder. It owns node registrations,
  shared-state subgraph mounts, fixed-edge declarations, static fan-out
  declarations, both conditional router forms, and target whitelists.
- The compiler validates all identifiers, edge-shape constraints, possible
  reachability, and possible END reachability. It performs expensive resolution
  work once.
- `CompiledGraph` is immutable and reusable. It stores pre-resolved internal
  indices and must not expose `petgraph` or other internal cursor types.
- Graphs intended for resume or replay use an explicit `GraphVersion`.
  Checkpoints from unversioned graphs are saveable but cannot be resumed or
  replayed.
- The root `GraphVersion` covers the complete composed graph. Any incompatible
  child topology or semantic change requires a new root version.
- Runtime invocation owns its input state, events, visited nodes, and step count.
  It also owns the active frontier and per-super-step update collection.
  Separate invocations must remain isolated, including concurrent invocations.
- Every lifecycle event carries a `RunId`. A shared sink must be able to
  distinguish concurrent invocations without receiving state or update values.

## Shared-state subgraph boundaries

- `StateGraph::add_subgraph` mounts an owned `CompiledGraph<S>` using the same
  `GraphState`; Stage 9 does not map between different State types.
- A mount is structural. It executes no `Node`, consumes no step or super-step,
  and follows its parent transition only after the child reaches `END`.
- Child real nodes share the parent invocation's State ownership, RunId,
  cancellation, run deadline, EventSink, event retention, checkpoint lineage,
  and cumulative counters.
- Empty and finitely nested children are valid. Child `END` returns to its
  mount; it does not complete the parent run.
- `GraphPath` and `NodePath` are structured `Arc`-backed segment sequences.
  Runtime must not concatenate or parse strings to navigate nested execution.
  Display uses slash-prefixed, percent-escaped segments so dots and empty
  identifiers are unambiguous; Runtime lookup and Eq/Hash remain structural.
- Node events, node errors, interrupts, visited attempts, batch-update sources,
  and checkpoint frontiers carry complete `NodePath` values. `node_id()` is a
  leaf compatibility accessor.
- `SubgraphStarted` and `SubgraphCompleted` are lightweight boundary events.
  Children never create independent top-level run events, and failure or
  interruption before exit emits no `SubgraphCompleted`.
- Stage 9 forbids subgraphs beside other active items in a parent parallel
  frontier. `END` exits only its branch and must be removed before this
  co-activity check, so `[END, child]` is valid while `[node, child]` is not.
  Parallel super-steps inside a child remain valid.
- Compilation flattens and pre-resolves child entries, exits, paths, and
  transitions. Internal graph indices remain private.
- Mounting takes ownership of an immutable compiled child, making direct and
  indirect mount-reference cycles unrepresentable through the safe builder
  API. Flattening must still reject duplicate structured paths.

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
- Core has no channel or stream dependency. The independent
  `group-agent-observability-tokio` crate adapts `EventSink` to Tokio broadcast
  without changing Runtime scheduling.
- The Tokio adapter uses a bounded channel and synchronous non-blocking send.
  Slow subscribers receive `Lagged { skipped }`; gaps must never be hidden.
- Subscriptions start at the call to `subscribe` and have independent cursors.
  No subscribers or closed subscribers must not fail graph execution.
- `EventRetention` and stream delivery are independent. Events still reach a
  configured stream Sink when retention is `None`.
- Multiple runs may interleave in one stream. Per-run emission order remains
  stable and consumers distinguish runs through `GraphEvent::run_id()`.
- The stream is process-local and lossy, not reliable durable delivery. It
  provides no event-history replay, custom backpressure, disk queue, or network
  transport.
- `EventBroadcast::new` uses checked power-of-two rounding for Tokio's shared
  bounded ring buffer. Zero returns `ZeroCapacity`, an unrepresentable request
  returns `CapacityTooLarge`, and `capacity()` returns the effective capacity.
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
- Interrupt is supported only for singleton frontiers. An observed
  parallel interrupt drops pending siblings, commits no update from that
  super-step, and returns `UnsupportedParallelInterrupt`.
- `GraphState::apply`, `GraphState::apply_batch`, routers, and sink callbacks are
  synchronous and cannot be preempted. Runtime observes control after they
  return.
- Control failures emit exactly one typed `RunFailed`, do not emit
  `RunCompleted`, and return structured `GraphRunError`.

## Transition and super-step semantics

- `START` has exactly one fixed successor and cannot use conditional routing.
- `END` has no outgoing edge.
- Each executable node has exactly one transition kind: one fixed successor,
  one static fan-out, one single-target conditional router, or one conditional
  fan-out router.
- Static fan-out declares a non-empty, duplicate-free target set.
- Conditional fan-out declares a non-empty, duplicate-free whitelist of
  ordinary nodes and `END`. It cannot directly target a subgraph mount.
- One internal transition kernel resolves fixed, conditional, fan-out, and
  structural subgraph enter/exit transitions only after the source update or
  complete parallel batch has committed.
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
- Conditional fan-out results must be non-empty and duplicate-free. Empty,
  duplicate, and out-of-whitelist results are structured run errors and emit
  no `RoutesSelected`.
- Conditional fan-out targets are sorted by compiled internal index. `END` may
  accompany executable targets and exits only the source branch.
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
- Single-target conditional routing emits `RouteSelected`; conditional fan-out
  emits one `RoutesSelected` with targets in stable `NodePath` order.
- Historical execution Replay emits one `ReplayStarted` after successful
  validation and restore, before containing subgraph boundaries or node events.
- `SuperstepCompleted` is emitted only after batch commit and all routing
  succeed. When checkpointing requires a save at that boundary, the save must
  also succeed first.

## Checkpoint boundaries

- The Stage 10.1 durable checkpoint contract has passed architecture review.
  Transition work must preserve its Record, Codec, lineage, CAS, restore
  validation, and lock boundaries.
- Checkpointing is opt-in. Normal invocation must not create a snapshot, enter a
  checkpointer method, or acquire a checkpoint lock.
- `CheckpointState` defines a separate `Snapshot`, snapshot creation, and future
  restoration boundary without changing `GraphState`.
- `CheckpointRecord` is the storage-neutral durable domain model.
  `CheckpointFormatVersion` versions that layout independently of
  `GraphVersion`.
- `CheckpointCodec<T>` owns Snapshot and durable interrupt payload byte
  conversion. Every `CodecDescriptor` separately identifies payload schema,
  schema version, and codec/encoding. Codec work is synchronous and must run
  outside store locks; it must not impose Serde or Clone on State.
- `CheckpointStore` exchanges only records. `RecordCheckpointer<T>` adapts it
  to the typed Runtime port. Third-party persistence implementations must not
  depend on private Runtime constructors.
- `group-agent-checkpoint-sqlite` is the first reference durable backend. It
  depends on Core, while Core has no SQLx or adapter dependency. Applications
  still provide the Codec.
- SQLite stores UUID identifiers as exact 16-byte values and durable `u64`
  counters as sortable eight-byte big-endian blobs, never through a lossy
  signed-integer conversion. Adapter-private Serde DTOs may encode structured
  paths without adding Serde bounds to Core types.
- SQLite save uses one SQLx-tracked `BEGIN IMMEDIATE` transaction. It queries
  the operation ID before head CAS, inserts the complete Record, and advances
  the per-thread head atomically. Busy or lock errors remain storage errors,
  never lineage conflicts.
- `CheckpointId`, `InterruptId`, and `RunId` use UUID v4 rather than
  process-local counters and support display, parsing, hashing, and stable-byte
  reconstruction.
- Snapshot values are retained through `Arc`; latest/history queries must not
  deep-copy them.
- Save only after all frontier nodes, state commit, and successor routing
  succeed. Save before entering the next frontier.
- `EverySuperstep` saves once per successful super-step. `FinalOnly` saves only
  the completed empty-frontier checkpoint.
- A legal `START -> END` graph saves exactly one completed checkpoint under
  either policy, with super-step and step zero and an empty frontier.
- Checkpoints retain checkpoint/thread/run identifiers, parent, super-step,
  cumulative step count, complete `NodePath` next frontier, Snapshot, and
  completed state.
- Durable `CheckpointRecord` step and super-step fields are fixed-width `u64`.
  Typed Runtime counters remain `usize`; reconstruction uses checked conversion
  and rejects out-of-range records without truncation.
- Parent means the state lineage used by execution, not storage insertion
  order. A new-state configuration explicitly expects no parent; a run based on
  a checkpoint must supply that checkpoint as `expected_parent`.
- Runtime carries the last successful checkpoint within a run as the next
  expected parent. Checkpointers atomically compare thread latest with
  `expected_parent` before insertion. A mismatch is `CheckpointConflict`, so
  concurrent runs on one `ThreadId` do not silently cross-link parent chains.
- A record carries a Runtime-assigned `CheckpointId` operation key. Same-ID,
  same-content replay must return the original record even after latest
  advances, regardless of Snapshot/payload `Arc` identity. Same ID with
  different bytes, lineage, format/schema/encoding descriptor, graph version,
  frontier, or interrupt metadata must return `IdempotencyConflict`. Codecs
  must produce deterministic canonical bytes for equivalent logical values.
- Idempotency lookup precedes expected-parent CAS. Idempotency, CAS, and
  insertion must be atomic so concurrent writers cannot form an implicit Fork.
- Snapshot and Codec logic are synchronous and cannot be preempted. They run
  before entering storage and never while a store lock is held.
- Cancellation and run timeout remain active while save is pending, with
  cancellation before run timeout before save result. Save-boundary control
  failures use no node identifier and the cumulative completed step count.
- Snapshot, encoding, conflict, save, cancellation, or timeout failure emits
  one `RunFailed`, no `CheckpointSaved` for an unconfirmed save, and no
  `RunCompleted`. Runtime does not claim to roll back committed state, storage
  effects, or external node side effects.
- Record queries and typed `latest`, `get`, and `history` adapters return shared
  `Arc` values. `get` is scoped by both ThreadId and CheckpointId.
- An interrupted checkpoint is neither completed nor a successful super-step
  commit. It retains an InterruptId, complete node path, shared typed payload,
  unchanged state snapshot, committed step/super-step counters, and a singleton
  frontier containing the interrupted node.
- Interrupt checkpoints are mandatory regardless of `CheckpointPolicy`. A
  node interrupt without checkpointing is `InterruptRequiresCheckpoint`.
- A typed interrupt payload is process-local unless the configured codec can
  encode it. Record-backed storage must fail explicitly for an unsupported
  local-only payload and must never discard it.

## Resume boundaries

- `ResumeConfig` centralizes checkpoint selection, checkpoint policy,
  additional step budget, events, and execution controls.
- Resume loads a specified checkpoint or latest. A specified checkpoint must
  still be latest; otherwise return `ResumeConflict`. Fork is not implicit.
- Validate ThreadId, latest-only status, explicit graph version,
  completion/frontier consistency, interrupt metadata, and each frontier
  `NodePath` before calling `CheckpointState::restore`. A resumable frontier
  must contain resolved executable paths exactly once, in compiled-index order,
  and in one `GraphPath` namespace. START, explicit END, unknown paths,
  duplicate nodes, non-canonical order, mixed namespaces, unversioned
  graphs/checkpoints, and version mismatch are `CheckpointIncompatible`.
- Graph version must change when topology, State/Snapshot schema, reducer, or
  router semantics become incompatible with existing checkpoints.
- Resolve only the actual saved frontier through the compiled NodePath index
  before restore, then reuse those internal indices for execution. Never scan
  all graph nodes or resolve the frontier twice during resume.
- After all compatibility checks and frontier resolution succeed, call
  `CheckpointState::restore` synchronously outside every storage lock. Restore
  is not preemptible; observe cancellation and run timeout again after it
  returns.
- Resume assigns a new RunId and emits `RunStarted`, then `RunResumed`, then
  containing `SubgraphStarted` boundaries when resuming inside a child, then
  continued execution events. Preparation failure emits one `RunFailed` and no
  `RunResumed`.
- Restore cumulative step and super-step counters. `RunConfig::max_steps` is
  the additional node budget for this resume call, while emitted positions
  remain cumulative.
- The restored checkpoint is the expected parent of the next save. Later saves
  continue from each prior successful save.
- Resuming a completed checkpoint restores State, executes no node, saves no
  duplicate checkpoint, and emits `RunStarted -> RunResumed -> RunCompleted`.
- An interrupted checkpoint requires a Resume value. A normal or completed
  checkpoint rejects an unexpected Resume value.
- Resume re-executes the interrupted node with the value exposed through
  `NodeContext`. The value is cleared after that node successfully returns an
  update and is never passed to successor nodes.
- Cancellation and run timeout start at resume entry and remain active during
  checkpoint loading and subsequent execution. Resume failure saves no new
  checkpoint.
- There is no fork, branch head, time travel, parallel interrupt, or PostgreSQL
  persistence. SQLite is provided by an independent Store adapter. Encoding is
  user-defined through codecs; no built-in Serde format is imposed.

## Replay boundaries

- `ReplayConfig` requires an explicit `ThreadId`, `CheckpointId`, and typed
  Checkpointer. Replay never selects latest implicitly and does not reuse
  `ResumeTarget`.
- Replay loads exactly through `Checkpointer::get`. The source checkpoint need
  not be latest, and Runtime performs no later latest query or parent CAS.
- GraphVersion, thread ownership, completion/frontier consistency, interrupt
  metadata, and canonical O(F) frontier resolution use the same pre-restore
  validation as Resume. Resolved internal indices are reused for execution.
- Restore remains synchronous, outside storage locks, and non-preemptible.
  Cancellation and run timeout begin at replay entry, cover exact checkpoint
  loading, are observed before and after restore, and continue through normal
  execution. Node timeout is unchanged.
- Replay assigns a new `RunId`, restores historical cumulative step and
  super-step counters, and treats `RunConfig::max_steps` as an additional node
  budget for this call.
- Successful order is `RunStarted -> ReplayStarted -> continued events ->
  RunCompleted`. `ReplayStarted` carries source thread/checkpoint and historical
  step/super-step. Preparation failure emits no `ReplayStarted`; every failure
  emits exactly one final `RunFailed`.
- Replay uses the existing execution and transition kernel with checkpoint
  writes unconditionally disabled. It creates no snapshot, record, parent link,
  head update, history entry, branch, or implicit Fork. Concurrent advancement
  of the source thread does not affect an already loaded replay.
- A completed source checkpoint is restored and returns a no-op `ReplayReport`.
  `ReplayReport` identifies the source checkpoint separately from its new
  replay RunId.
- An interrupted source checkpoint requires a Resume value; a normal or
  completed checkpoint rejects one. The value remains scoped to the retained
  node's single re-execution attempt.
- Any new node interrupt during Replay returns
  `ReplayInterruptUnsupported`, emits no `RunInterrupted`, and performs no
  checkpoint write. This applies even if the interrupt occurs in a parallel
  replay frontier.
- Replay re-executes user nodes and can duplicate external database, network,
  tool, or other side effects. Runtime provides no rollback, sandbox, or
  automatic deduplication.
- Replay is not Fork. `BranchId`, branch heads, and writable descendants from a
  historical checkpoint are deferred to Stage 15.

## Suspension boundaries

- `InterruptRequest` and `ResumeValue` use safe type erasure and shared `Arc`
  storage. They require neither GraphState nor payload types to implement
  Clone or Serde.
- `NodeContext::require_resume_value<T>()` is the fallible typed accessor. It
  distinguishes missing values from type mismatches and reports expected and
  actual type names through `ResumeValueError`; preserve it in a `NodeError`
  source chain.
- A singleton node interrupt applies no update and performs no normal routing.
  Runtime emits `NodeInterrupted`, snapshots unchanged committed state, and
  saves a checkpoint whose frontier is that same node.
- Only a confirmed save produces `CheckpointSaved`, `RunInterrupted`, and
  `ExecutionOutcome::Interrupted`. It never produces `RunCompleted`.
- Save failure, CAS conflict, cancellation, or run timeout remains a
  `GraphRunError`, emits one `RunFailed`, and does not return an interrupted
  outcome. Run timeout and cancellation remain active during the save future.
- Repeated interrupts allocate new InterruptId and CheckpointId values and
  continue lineage from the prior interrupted checkpoint. The old Resume value
  is not checkpointed or reused. Latest-only and CAS rules remain unchanged.
- The node is re-executed on resume. Runtime neither rolls back nor deduplicates
  external effects performed before suspension. Pre-interrupt work must be
  idempotent, and irreversible effects should be deferred until after the node
  has validated its Resume value.
- Resume values remain process-local. Interrupt payloads become durable only
  when the configured codec explicitly supports their type/schema.

## Performance principles

- Aggregate fixed edges, fan-out targets, both conditional router forms, source
  counts, and successor presence once during compilation and reuse the result
  across validation and transition compilation. Ordinary compilation should
  remain approximately O(V + E). The composition-only frontier-pair validation
  may run only when a parent contains both fan-out and a subgraph mount. It must
  discard `END` and process only members of each produced active frontier.
- Use internal indices on the Runtime hot path.
- Fixed and fan-out transitions must remain pre-resolved to internal indices.
- Conditional fan-out resolution must inspect only selected targets and remain
  approximately Router plus O(T log T) stable sorting.
- Subgraph entry, exit, namespace paths, and transitions must be pre-resolved
  during compilation. Runtime must not repeatedly build or parse path strings.
- Resume from a child checkpoint traverses only the actual saved frontier.
- Replay traverses only its exact saved frontier, performs one exact
  `Checkpointer::get`, and reuses the same execution kernel with storage writes
  disabled. It must not add ordinary invoke or Resume hot-path work.
- Frontier deduplication must operate on produced successors and must not scan
  every compiled graph node.
- Do not clone complete state values per step.
- Do not use a global Mutex or RwLock for execution.
- Do not spawn nodes; use in-task future concurrency for a super-step.
- A normal node uses one `Arc<dyn Node<S>>` dispatch and one `async-trait`
  boxed future. Do not reintroduce a second boxed internal adapter future.
- When no external token or timeout is configured, node execution should retain
  a direct-await fast path with only small control-state branches.
- `EventRetention::None` without a sink should avoid constructing events.
- Disabled checkpointing must avoid snapshot creation, storage calls, and lock
  acquisition. Enabled frontier metadata must inspect only produced successors.
- Ordinary update nodes allocate no interrupt payload and enter no snapshot
  path unless checkpointing is explicitly enabled.
- `InMemoryCheckpointStore` may lock only briefly while atomically applying
  idempotency/CAS/insertion or cloning record handles. Never execute Snapshot
  or Codec code while holding its mutex.
- SQLite uses pooled connections and short transactions. Never run user Codec,
  snapshot, restore, node, router, or sink code inside a database transaction.
- Ordinary invocation and `group-agent-core` must not depend on the SQLite
  crate or enter SQLx paths.
- Without the observability adapter, Core's event hot path must remain
  unchanged. Core must not depend on `tokio-stream`.
- The Tokio broadcast Sink may clone only the lightweight `GraphEvent` needed
  for channel ownership. It must not block, await, spawn per node, or allocate
  an unbounded queue.
- Performance conclusions require repeatable benchmarks. Do not make comparative
  performance claims from architecture alone.

## Out of scope

Do not add parent/child State mapping, parent-frontier parallel subgraphs, Fork,
`BranchId`, branch heads, Replay writes, historical State modification, time
travel, parallel interrupts, PostgreSQL, forced Serde bounds or built-in Serde
codecs, arbitrary Node Command or Send APIs, conditional fan-out into subgraph
mounts, custom asynchronous event backpressure, disk event queues,
OpenTelemetry or metrics exporters, WebSocket or SSE servers, network event
proxies, LLM providers, tool calling, MCP, RAG, token streaming, standalone
reducer registration, Tower middleware, Axum, distributed workers, macro DSLs,
or a visual interface unless a later stage explicitly authorizes them. SQLite
is the sole reference database backend and Tokio broadcast is the sole stream
adapter in this stage. Fork and branch heads are deferred to Stage 15. Do not
create placeholder crates for future capabilities.

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
cargo run -p group-agent-core --example replay
cargo run -p group-agent-core --example resume
cargo run -p group-agent-core --example interrupt
cargo bench --workspace --no-run
cargo check --workspace --all-targets --all-features
```

Also validate the declared MSRV:

```bash
cargo +1.85.0 check --workspace --all-targets --all-features
cargo +1.85.0 test --workspace
```

Inspect the resolved dependency direction and finally check the working-tree
diff:

```bash
cargo tree --workspace
git diff --check
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
Stage 9.1 Review has passed. Stage 10.1 supplied the durable-checkpoint contract
correction identified by the Stage 10 architecture review. Stages 11 through
14 preserve that reviewed Record/Codec/Store boundary.
