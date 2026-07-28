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
- explicit writable forks from historical checkpoints with `BranchId`,
  isolated branch heads/history, and independent CAS;
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
- an independent provider-neutral `group-agent-model` crate with strongly typed
  messages, tool data, requests, responses, capabilities, errors, and
  validated non-streaming/streaming model calls;
- a reusable validating chat-stream collector with stable text/tool ordering,
  bounded sparse tool indices, JSON argument completion, partial cumulative
  usage merging, continuation Extensions, atomic per-event commit, a permanent
  failed state after the first error, and mandatory logical finish.
- an independent `group-agent-genai` crate fixed to genai 0.6.5 with injected
  Client/auth/endpoint configuration, request/response/tool mapping, online
  stream normalization, explicit continuation Extensions, partial Usage,
  source-preserving error classification, and offline loopback HTTP coverage.
- an independent Rust 1.85 `group-agent-tool` crate with object-safe local
  tools, deterministic immutable registration, precompiled JSON Schema
  validation with concrete source retention, explicit side-effect behavior,
  caller-runtime timeouts, fallible panic-safe payload-free observers,
  stop-scheduling-and-drain fail-fast, call-ID-safe Tool messages, and
  spawn-free deterministic batches.
- an independent client-only `group-agent-mcp` crate fixed to rmcp 2.2.0 with
  reusable stdio sessions, cycle- and limit-safe adapter-owned Tool discovery,
  immutable Registry snapshots, reversible namespace mapping, conservative
  remote behavior, fail-closed content mapping, explicit close/kill/wait,
  runtime-independent best-effort direct-child Drop fallback, and offline
  lifecycle tests.

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
- Graphs intended for resume, replay, or fork use an explicit `GraphVersion`.
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
- A default Resume remains latest-only on the default thread head. A Resume
  configured with `BranchId` is latest-only on that branch head and emits
  `BranchResumed` after `RunResumed`.
- There is no implicit fork, State patch, branch merge/deletion, time travel,
  parallel interrupt, or PostgreSQL persistence. SQLite is provided by an
  independent Store adapter. Encoding is user-defined through codecs; no
  built-in Serde format is imposed.

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
- Replay is not Fork and cannot select or advance a branch head.

## Fork and branch boundaries

- `ForkConfig` requires an exact `ThreadId` and `CheckpointId`, allocates a new
  `BranchId` by default, and may accept an explicit collision-checked ID.
- Fork loads through exact `get`, defensively verifies the returned checkpoint
  ID, validates and resolves the saved frontier before restore, restores
  outside Store locks, then creates a branch whose initial head is the exact
  source checkpoint.
- The source need not be latest. Fork never changes the default thread head,
  default history, another branch, or any historical Record.
- `CheckpointRecord`, Codec identity, and content-idempotency semantics remain
  frozen. Branch ownership, source, head, and Record membership are additive
  Store metadata rather than Record fields.
- `Checkpointer` and `CheckpointStore` expose additive `create_branch`,
  `save_branch`, `branch_head`, and `branch_history` methods. Implementations
  without branch capability fail explicitly; the in-memory and SQLite adapters
  implement the capability.
- Every `BranchId` has exactly one owning `ThreadId`. Its source, head, and
  membership Records must all belong to that same thread.
- Duplicate `create_branch` returns `BranchAlreadyExists`, including an exact
  repeat with the same thread and source; creation is not idempotent success.
- Failed branch creation creates no Branch. Once creation succeeds, later Fork
  execution failure preserves the Branch at its last confirmed head. A failure
  before the first descendant save therefore leaves the source as the head and
  allows a later explicit branch Resume.
- An absent branch or a branch queried with the wrong thread makes
  `branch_head` return `None` and `branch_history` return an empty collection.
- Branch history begins with the shared source Record and continues through
  branch-only descendants. Each descendant's ordinary `parent_id` must match
  the prior branch head.
- Branch save performs ID/content idempotency before an independent branch-head
  CAS. Concurrent writers from one branch parent allow only one successful
  successor and cannot form another implicit fork.
- Fork assigns a new `RunId` and emits `RunStarted -> ForkStarted -> continued
  events`. `ForkStarted` carries source thread/checkpoint, branch ID, and
  historical step/super-step. A completed source is a no-op execution with a
  newly created branch head.
- Fork reuses the existing execution/checkpoint kernel. Conditional fan-out,
  nested subgraphs, Interrupt, cumulative counters, failure reuse, and
  additional `max_steps` retain their existing semantics. An Interrupt writes
  to the branch and can later be resumed with an explicit branch Resume value.
- `CheckpointConfig::with_branch_id` selects branch CAS but does not infer
  lineage; `expected_parent` must be the current head of that exact branch.
- Resume remains latest-only continuation, Replay remains exact historical
  read-only execution, and Fork remains the only branch-creation operation.
- Fork starts from the exact historical State. State patches, branch merge,
  branch deletion, and implicit branch selection are out of scope.
- SQLite migrations `0002_branch_heads.sql`,
  `0003_branch_ownership.sql`, and `0004_branch_read_consistency.sql` persist
  branch metadata, enforce composite ThreadId ownership, require non-source
  heads to be members, and require inserted members to continue the current
  head. Branch Record insertion, membership insertion, and branch-head update
  occur in one short `BEGIN IMMEDIATE` transaction and roll back together.
- SQLite `branch_head` and `branch_history` run in one read transaction and
  share one JOIN-based query scoped by both ThreadId and BranchId. Before
  returning data they validate source/head ownership, non-source head
  membership, stable ordering, and the complete source-to-head parent lineage.
  Corrupt, cross-thread, missing, duplicate, non-member, or discontinuous data
  returns a structured storage corruption error.

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

## Provider-neutral model boundaries

- `group-agent-model` and `group-agent-core` must remain mutually independent.
  Applications may depend on both; only model examples/tests use Core through a
  dev-dependency.
- Message roles are strongly typed as System, User, Assistant, and Tool.
  Assistant messages may contain text and tool calls together. Tool results
  retain a stable ToolCallId and cannot be represented as User messages.
- Empty text is valid and ordered ContentPart values must remain ordered.
  `ContentPart::as_text` returns `Option<&str>` so future non-text variants do
  not need a fake text value. Text helpers ignore future non-text content.
  Display and Debug implementations must not expose potentially sensitive
  message content.
- ToolDefinition, ToolCall, ToolResult, and ToolChoice are data only. The model
  crate must not execute tools or add a registry, Tool trait, retry, MCP, or
  timeout policy.
- Provider-specific request, response, assistant, tool-call, and usage metadata
  belongs in the ordered provider-neutral `Extensions` container. Keys are
  normalized and validated; duplicate insertion is rejected. Fragment merging
  accepts the same key/value idempotently and rejects conflicting values.
  Common types must not expose provider SDK or raw provider request types.
- Provider adapters implement the object-safe, `Send + Sync`
  `ChatModelAdapter` raw port. Raw methods accept only
  `ValidatedChatRequest`, a public type with private fields and crate-private
  construction. Independent adapter crates may read its accessors or consume
  it, but applications cannot construct it from `ChatRequest`. Applications
  hold the cheaply cloneable `ChatModel` facade, which shares one
  `Arc<dyn ChatModelAdapter>` and one immutable validated metadata snapshot.
- `ChatModel::complete` and `ChatModel::stream` are non-overridable public
  facade methods. They always run `ChatRequest::validate`, then common
  capability validation, then the streaming capability check when applicable,
  then privately construct `ValidatedChatRequest`, then perform one raw adapter
  dispatch. Invalid requests and unsupported capabilities must never enter raw
  adapter code.
- Facade construction rejects internally contradictory metadata. Parallel tool
  calls require tool calling. Structured-output capability remains absent until
  the request model has a provider-neutral field that validation can consume.
- Stream initialization and later items may fail independently. No model method
  creates a Tokio Runtime, detached task, channel, or retry loop.
- Stream aggregation preserves text order, stores tool calls by bounded stable
  index without sparse Vec growth, appends argument fragments, parses JSON only
  at completion, merges tool-call Extensions by index, and sorts final calls by
  index. ResponseStarted is optional and occurs at most once; absent identity
  fields remain unknown.
- TokenUsage input, output, and total are independently optional. Usage events
  are cumulative snapshots: Some updates one field, None never clears known
  data, known counters cannot decrease, and extension conflicts fail.
  Snapshot merge validates all counters and Extensions before in-place commit,
  moves only new extension values, and never clones accumulated Extensions.
  Checked input-plus-output overflow and inconsistent explicit totals are
  errors.
- Finished occurs at most once; every event variant after it is invalid, and
  transport EOF without it is a protocol error. The first stream item error
  stops collection without returning a partial success. Every Collector event
  validates before commit. The first Active-state error permanently poisons a
  manual Collector; later push/finalization cannot recover a successful
  response. Manual Collector use and `collect_chat_stream` are alternatives.
- Content-bearing Debug output exposes only safe structure such as variants,
  identifiers, counts, byte lengths, numeric usage, and extension keys. It must
  not print prompts, tool arguments/results, schemas, fragments, extension
  values, or adapter error messages.
- ModelError preserves concrete sources and separates validation, capability,
  authentication, permission, rate limit, availability, timeout, protocol,
  decode, cancellation, and other classifications. Retryability metadata is not
  a retry policy.
- Cancellation and timeout remain caller-owned. Dropping complete/stream or a
  containing Group Node Future cancels in-flight work. ChatRequest must not
  contain Group RunControl, NodeContext, or Tokio CancellationToken values.
- `group-agent-model` performs no network access and supports no concrete
  provider. `group-agent-genai` is the separate Stage 17 provider boundary and
  must not add model concepts to Core or genai types to Model.

## Tool Runtime boundaries

- `group-agent-tool` depends normally on `group-agent-model`. Core and Model
  never depend on Tool. Tool may use Core only as a dev-dependency for ordinary
  Node examples and integration tests.
- Tool's normal dependency graph must contain no `group-agent-core`, `genai`,
  HTTP client, SQLx, MCP, or provider SDK. The crate inherits Rust 1.85.
- `Tool` is object-safe, `Send + Sync`, and storable as `Arc<dyn Tool>`.
  Implementations expose one immutable cached `ToolDefinition` and stable
  `ToolBehavior`. They receive validated borrowed JSON arguments, call
  identity, an optional opaque idempotency key, and provider-neutral execution
  metadata.
- Tool implementations receive no `NodeContext`, `RunControl`, Group
  cancellation token, runtime, channel, or detached task. Cancellation is
  Future drop. Dropping a single or batch Runtime Future must drop all
  still-pending Tool Futures. External side effects are not rolled back.
- `ToolOutput` contains an existing `ToolResult` and optional Extensions.
  Business rejection is an explicit `ToolResult { is_error: true }`.
  Infrastructure failure remains a source-preserving `ToolError` and
  `ToolRuntimeError`; Runtime must never silently convert it to success.
- `ToolBehavior` classifies `ReadOnly`, `IdempotentWrite`, and
  `NonIdempotentWrite`, records explicit parallel eligibility, and may require
  an idempotency key. Read-only tools cannot require a write-idempotency key.
  Non-idempotent writes cannot require a key that would make their declared
  behavior idempotent; they are sequential by default and overlap only after
  an explicit Tool declaration.
- Behavior metadata is advisory policy input, not an exactly-once claim.
  Stage 18 adds no automatic retry, durable idempotency store, distributed
  lock, permission system, or rollback mechanism.
- Registry construction rejects invalid or mismatched definitions, empty
  descriptions, non-canonical names, duplicate names, inconsistent behavior,
  and invalid JSON Schema. Each schema is compiled once at registration and
  cached with its Tool; execution must not rebuild definitions or validators.
- The immutable registry keeps definitions in lexical ToolName order and uses
  an index for lookup rather than a full scan. It is cheaply cloned through
  shared immutable ownership; there is no global registry or execution-time
  lock.
- JSON Schema validation uses the mature `jsonschema` crate with default
  resolver features disabled. Validation errors expose JSON instance/schema
  pointers and the rejecting keyword in default formatting, never complete
  arguments, schema values, or source messages. Registration and execution
  errors retain the concrete `jsonschema::ValidationError` source. Explicit
  source-chain traversal can expose upstream details, so applications must
  filter such diagnostics before logging them.
- Single execution order is call-identity validation, indexed lookup, cached
  schema validation, behavior/idempotency policy, Tool execution, and explicit
  ToolOutput-to-ToolResult conversion. Missing or invalid calls never enter a
  Tool.
- Per-call timeout uses Tokio time from the caller's runtime and drops the Tool
  Future when elapsed. Runtime creates no Tokio runtime. Timeout errors retain
  call identity, Tool name, duration, and the concrete timeout source.
- Batch validates duplicate ToolCallId values before any execution,
  prevalidates every call, uses bounded `FuturesUnordered` scheduling without
  per-call `tokio::spawn`, and stores results directly by input index in O(n).
  The default collect-all policy isolates per-call failures. Fail-fast is
  explicit: after the first observed primary failure it stops scheduling new
  calls, drains every already-started call to its real observable outcome, and
  marks only never-started calls `NotStartedDueToFailFast`.
- Batch results always match input order, regardless of completion order.
  Invalid-schema and missing-tool items do not enter Tools. A dropped batch
  Future cancels every still-pending Tool Future without fabricating a report;
  this caller-owned drop boundary is distinct from fail-fast.
- `ToolEventSink` is synchronous, fallible, optional, panic-caught, and called
  inline outside Registry locks.
  Events contain only safe call identity, batch index, result/error class, and
  timeout duration. They never contain arguments, Tool output, metadata values,
  or source text. `ExecutionStarted` observer failure or panic prevents Tool
  execution and returns `ObserverFailed`. Completed, failed, and timed-out
  observer failures are retained only as secondary diagnostics and never
  replace the already determined primary Tool outcome.
- Public Tool Runtime errors are typed and source-preserving. Default Debug and
  Display redact arguments, output, Tool messages, metadata values, schema
  values, and source secrets. Applications that explicitly traverse a source
  chain remain responsible for filtering upstream data.
- `ToolResult` remains payload-only Model data. `execute_message` and ordered
  batch message conversion pair it with the original `ToolCallId` through
  `Message::tool`; callers should not manually pair unrelated IDs and results.
- Tool execution provides no retry, exactly-once guarantee, side-effect
  rollback, or recovery of execution facts after the caller drops the complete
  Runtime Future.
- Group integration is application-level: an ordinary Node may hold
  `ToolRuntime`, execute a call read from State, return a State Update, and wrap
  Runtime failure with `NodeError::with_source`. Group continues to own node
  cancellation and deadlines; Tool Runtime does not modify Core.

## MCP Tool Adapter boundaries

- `group-agent-mcp` depends normally on Model, Tool, and official crates.io
  `rmcp = "=2.2.0"`. Core is a dev-dependency only. Core, Model, and Tool never
  depend on MCP.
- rmcp default features are disabled. Enable only `client` and
  `transport-async-rw`; stdio uses adapter-owned direct-child construction so
  its guard remains valid during Tokio runtime teardown. Do not enable rmcp
  child-process helpers, server macros, HTTP transports, OAuth, or unrelated
  MCP capabilities.
- The MCP crate declares Rust 1.88 independently because rmcp 2.2.0's published
  source uses let-chain syntax not accepted by Rust 1.87. Do not raise the Rust
  1.85 MSRV of Core, Model, Tool, SQLite, or observability, and do not patch,
  vendor, or downgrade upstream to evade that requirement.
- `McpClientSession` performs one initialization and reuses one rmcp service.
  It is cheaply shared, owns rmcp's service task, supports explicit shutdown,
  and must not add per-call detached tasks, forwarding channels, a Runtime, or
  a global session. Stdio commands use an executable plus argument vector,
  never a shell command string.
- Explicit stdio shutdown atomically rejects new calls, closes and joins rmcp,
  waits for the direct child, kills it after the configured bounded grace
  period when needed, and waits again to reap it before publishing success or
  failure. Service close and child cleanup use independent tasks that the
  supervisor always awaits, so service or child worker panic cannot skip child
  cleanup. If both paths fail, the service failure is primary. One
  Session-owned cleanup supervisor and completion serve every concurrent or
  repeated shutdown caller; cancellation of a caller Future must not cancel
  cleanup. Store the final result, publish `CLOSED`, and only then wake
  completion waiters. Outer rmcp JoinError, `QuitReason::JoinError`, and worker
  panic are source-preserving `ShutdownFailed`. rmcp 2.2.0 logs but does not
  return `transport.close()` errors, so Group cannot expose them. Zero grace
  performs one non-blocking exit check then immediately kills and waits for a
  still-live direct child.
- Drop is not explicit shutdown: it must not block, await the shared completion,
  report an async result, or depend on Tokio cleanup. It synchronously requests
  direct-child termination and tries to give wait/reap to a standard thread.
  When standard-thread creation fails because of OS or resource exhaustion,
  kill has still been attempted but wait/reap is not guaranteed and a zombie
  may remain until parent exit or another OS mechanism reaps it. Never add an
  unbounded Drop wait or a global reaper; explicit shutdown is the reliable,
  recommended lifecycle path. If the explicit cleanup task already owns the
  child, Drop must not double-kill or double-wait it. No process-tree cleanup is
  claimed.
- Discovery requires the server tools capability and owns the `tools/list`
  pagination state machine. Track all returned cursors, reject same-cursor and
  multi-cursor cycles, enforce non-zero page/tool limits with checked
  arithmetic, and stage every page privately. Mapping, Schema compilation,
  Registry construction, and immutable `McpToolSet` publication occur only
  after all pages succeed. Refresh creates a new snapshot; Stage 19.1 does not
  listen to `tools/list_changed` or mutate a live Registry.
- Preserve legal names for a single server. Multi-server use requires a stable
  namespace or prefix whose frozen mapping retains the exact local, server, and
  remote names. Invalid names and collisions fail before publication. Calls
  always send the original remote name and never modify arguments with routing
  data.
- Registry registration owns JSON Schema compilation and validation. Invalid
  arguments must fail locally without issuing an MCP request. The adapter must
  not recompile schemas or repeat Tool Runtime domain validation.
- Remote Tools default to `NonIdempotentWrite` and sequential execution.
  Annotations are untrusted hints. Only an explicit exact server/tool behavior
  override may expand concurrency, and every override is validated and frozen
  during discovery. Repeated entries for the same remote Tool are invalid even
  when the values match; never use last-write-wins. The adapter never retries
  and claims neither exactly-once execution nor rollback.
- Text blocks retain their order; structured content is serialized once and
  appended as one JSON text part. MCP `isError` remains a business
  `ToolResult`. Image, audio, binary, embedded-resource, resource-link, and
  unknown content fail closed as `UnsupportedContent`; never discard, download,
  or synthesize placeholder content.
- Transport, protocol, discovery, process, Schema, and serialization failures
  retain concrete sources. Default Debug/Display must redact command
  environment, arguments, output, raw protocol payloads, and source messages.
  rmcp MCP/JSON-RPC error responses classify as `Protocol`; I/O, send, and
  connection-closure failures classify as `Transport`; explicitly closed
  sessions classify as `SessionClosed`. `McpToolSet` Debug exposes only server
  IDs, counts, Registry presence, and naming-policy categories, never mappings
  or Tool names. Applications that explicitly traverse sources own upstream
  log filtering.
- Tool Runtime continues to own identity, Schema, timeout, batch, side-effect,
  fail-fast-drain, and ToolMessage pairing semantics. Dropping a call Future
  releases local ownership but provides no remote side-effect rollback or
  immediate termination guarantee.
- Stage 19 production transport support is child-process stdio. HTTP, OAuth,
  credential storage, server hosting, Resources, Prompts, Sampling, Roots,
  automatic refresh, sandboxing, retry, and Agent loops remain out of scope.

## Genai adapter boundaries

- `group-agent-genai` is fixed to `genai = "=0.6.5"` for Stage 17. Do not use
  0.7 beta, Git dependencies, unpublished commits, or a locally patched genai.
- The workspace package MSRV default remains Rust 1.85. Core, Model, Tool,
  SQLite, and observability inherit that default. `group-agent-genai` alone
  declares Rust 1.88 because the published genai 0.6.5 source uses let-chain
  syntax that became stable in Rust 1.88. This is an effective source-derived
  requirement; do not claim that genai 0.6.5 declares
  `rust-version = "1.88"` in its manifest.
- Applications inject one preconfigured `genai::Client`. The adapter never
  reads `.env`, resolves organization credential policy, prints secrets, or
  rebuilds a Client per request.
- Raw adapter methods continue to accept only `ValidatedChatRequest`.
  Standalone mapping helpers must validate ordinary `ChatRequest` values before
  conversion.
- Capabilities are explicit. genai 0.6.5 has no provider-neutral
  `parallel_tool_calls` request control, so the adapter rejects that capability
  and every explicit request value instead of silently ignoring it.
- Only documented `group.genai.*` request Extensions are accepted. Unknown
  owned keys fail; foreign namespaces are ignored and never forwarded.
  Extensions cannot inject headers, Authorization, API keys, or arbitrary
  `extra_body`.
- Thought signatures are carried as genai thought content exactly once and
  return on ToolCall Extensions. Non-streaming requests that may produce
  ToolCalls require `GenaiChatModelAdapter::new_with_stable_target`, a
  `ClientConfig` without a dynamic `ServiceTargetResolver`, and one exact
  `ServiceTarget` shared by validation and dispatch. Dynamic or unknown target
  resolution fails closed before HTTP for such requests; ordinary text
  completion remains available.
- For stable OpenAI Responses non-streaming ToolCalls, genai first reads,
  parses, and may clone the complete raw response value. Group's configurable
  8 MiB default is only a post-capture parser admission limit. It is not a
  network-read, HTTP-body, or peak-memory bound. An early-terminating counting
  serializer retains no serialized bytes; the raw value is parsed only long
  enough to correlate encrypted reasoning with the matching normalized
  function call, then taken and released. Successful Group `ChatResponse`,
  Extensions, adapter mapping errors, and their default formatting do not
  expose it.
- Identical signatures within one function call are deduplicated in first
  occurrence order. Distinct signatures preserve provider order, and
  deduplication never crosses function-call boundaries. Empty signatures,
  checked length overflow, and configured count or byte-limit violations fail.
  A real two-turn HTTP fixture, not manual signature injection, verifies
  continuation. Response IDs are stateless: applications must explicitly place
  one into the next request's previous-response-ID Extension.
- Binary, Custom, and assistant ToolResponse content are rejected. Reasoning is
  optional redacted Extension data and never normal assistant text.
- Stream wrappers directly own genai streams. They create no channel, detached
  task, or collector, emit an explicit Protocol error for EOF without End, and
  become terminal after the first item error.
- Stage 17.2 streaming is fail-closed. `GenaiStreamingPolicy` defaults to
  Disabled; enabled text streaming requires a Client bound to
  `AdapterKind::OpenAI`. There is no caller-supplied protocol profile. Group
  checks the exact resolved `ChatStreamResponse.model_iden` before polling its
  lazy stream, so custom or changing resolvers cannot redirect an audited call
  into Responses. Under genai 0.6.5, OpenAI Chat text-only streaming is
  supported, requests that may produce ToolCalls fail before HTTP dispatch,
  and all OpenAI Responses streaming is rejected with zero server hits.
- A `ThoughtSignatureChunk` is not valid on the audited OpenAI Chat text-only
  stream. Empty and non-empty chunks both cause an immediate terminal Protocol
  error without retaining content, guessing ownership, or emitting a partial
  Group event.
- Streaming ToolCall cumulative arguments are append-only: equal input is
  idempotent, a prefix extension emits only the suffix, and a non-prefix change
  is Protocol. Terminal arguments are parsed and compared as complete JSON
  values; malformed accumulated JSON is Decode.
- genai errors remain concrete sources. Group's default Debug and Display do
  not expose prompts, tool arguments/results, reasoning, raw bodies, provider
  error bodies, headers, or auth data. Explicitly traversing or recording the
  complete `Error::source()` chain can reach upstream genai errors and may
  expose upstream data; applications must filter sensitive information before
  logging a full source chain.
- `ResponseId` Debug and Display are redacted; `as_str()` is the explicit
  continuation-value accessor. Group does not trace raw SSE data. The genai
  0.6.5 Responses streaming path that can trace raw events is never polled or
  dispatched by the Group adapter. Non-streaming captured raw values are not
  placed in successful Group responses or adapter mapping errors and are
  released after signature correlation; their parser admission limit applies
  only after genai has captured the value.
- The adapter implements no retry, fallback, rate limiter, circuit breaker,
  tool execution, MCP, embedding, RAG, memory, ReAct, or prebuilt Agent.
- Provider tests are loopback-only and must never call the public internet.

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
- Fork traverses only its exact source frontier and adds no work to ordinary
  invoke, default Resume, or Replay. Branch metadata is touched only when
  explicitly requested.
- Branch Resume performance baselines must use the real
  `InMemoryCheckpointStore` plus `RecordCheckpointer`; benchmark-only branch
  implementations do not validate the Store read path.
- Frontier deduplication must operate on produced successors and must not scan
  every compiled graph node.
- Do not clone complete state values per step.
- Model facade validation must move one `ChatRequest` into
  `ValidatedChatRequest` without cloning it. Do not add another async-trait
  dispatch layer.
- Model stream event atomicity must use borrowed prevalidation and local
  deltas, never clone the complete Collector or ToolCall accumulator.
- TokenUsage merge must inspect accumulated Extensions in place and move only
  new values after complete validation; never clone the complete accumulated
  extension map.
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
- Tool registry lookup uses its immutable name index and never scans every
  definition. JSON Schema validators compile only during registration.
- Tool Runtime must not enter Core's hot path, clone complete ToolCall
  arguments without ownership need, spawn per batch call, or perform
  superlinear result reordering. Benchmark registries, schemas, calls, and
  batch setup remain outside measured iterations.
- MCP execution reuses its initialized session, immutable discovery snapshot,
  existing Registry index, and precompiled schemas. Benchmarks cover only
  offline mapping and injected-session dispatch; never benchmark child-process
  startup, network transport, or sleeps.
- Performance conclusions require repeatable benchmarks. Do not make comparative
  performance claims from architecture alone.

## Out of scope

Do not add State patches, branch merge, branch deletion, parent/child State
mapping, parent-frontier parallel subgraphs, Replay writes, historical State
modification, time travel, parallel interrupts, PostgreSQL, forced Serde bounds
or built-in Serde codecs, arbitrary Node Command or Send APIs, conditional
fan-out into subgraph mounts, custom asynchronous event backpressure, disk
event queues, OpenTelemetry or metrics exporters, WebSocket or SSE servers,
network event proxies, provider fallback, credential storage, provider retries
or rate limiters, automatic Tool retry, exactly-once Tool execution, durable
Tool idempotency storage, Tool sandboxing, distributed Tool locks, MCP HTTP,
MCP OAuth, MCP server hosting, MCP Resources, Prompts, Sampling, Roots,
automatic MCP discovery refresh, RAG,
embeddings, agent memory, ReAct, prebuilt agents, standalone reducer
registration, Tower middleware, Axum, distributed workers, macro DSLs, or a
visual interface unless a later stage explicitly authorizes them.
Provider-neutral chat streaming exists only in `group-agent-model`; it does not
change Core observability. SQLite is the sole reference database backend and
Tokio broadcast is the sole Core event-stream adapter in this stage. Do not
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
cargo run -p group-agent-core --example fork
cargo run -p group-agent-core --example replay
cargo run -p group-agent-core --example resume
cargo run -p group-agent-core --example interrupt
cargo run -p group-agent-model --example model_node
cargo run -p group-agent-genai --example genai_model
cargo check -p group-agent-genai --example genai_node
cargo run -p group-agent-tool --example tool_runtime
cargo run -p group-agent-tool --example tool_node
cargo test -p group-agent-tool --doc
cargo run -p group-agent-mcp --example mcp_stdio
cargo run -p group-agent-mcp --example mcp_tool_node
cargo test -p group-agent-mcp --doc
cargo bench --workspace --no-run
cargo check --workspace --all-targets --all-features
```

Validate the layered MSRV. Rust 1.85 covers every workspace crate except the
MCP and genai adapters:

```bash
cargo +1.85.0 check \
  --workspace \
  --exclude group-agent-genai \
  --exclude group-agent-mcp \
  --all-targets \
  --all-features
cargo +1.85.0 test \
  --workspace \
  --exclude group-agent-genai \
  --exclude group-agent-mcp
```

Rust 1.88 covers the MCP adapter's targets, features, examples, benchmarks,
tests, and documentation:

```bash
cargo +1.88.0 check \
  -p group-agent-mcp \
  --all-targets \
  --all-features
cargo +1.88.0 test -p group-agent-mcp
cargo +1.88.0 test -p group-agent-mcp --doc
```

Rust 1.88 covers all genai adapter targets, features, examples, benchmarks,
tests, and documentation:

```bash
cargo +1.88.0 check \
  -p group-agent-genai \
  --all-targets \
  --all-features
cargo +1.88.0 test -p group-agent-genai
cargo +1.88.0 test -p group-agent-genai --doc
```

Finally inspect dependencies and check the working-tree diff:

```bash
cargo tree --workspace
cargo tree -p group-agent-core -e normal
cargo tree -p group-agent-model -e normal
cargo tree -p group-agent-tool -e normal
cargo tree -p group-agent-mcp -e normal
cargo tree -p group-agent-genai -e normal
cargo metadata --no-deps --format-version 1
git diff --check
git diff -- crates/group-agent-core
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
15 preserve that reviewed Record/Codec/content-idempotency boundary; branch
metadata is additive Store capability. Stage 16 adds an independent
provider-neutral model boundary without changing Core or the durable checkpoint
API. Stage 16.1 hardens the public model facade, partial usage, redacted Debug,
and continuation Extensions without changing Core. Stage 16.2 makes the raw
adapter boundary non-bypassable and stream/Usage merging atomic without
changing Core. Stage 17 adds the separate genai 0.6.5 adapter with injected
Client ownership, offline HTTP coverage, and no Core changes. The Stage 17 MSRV
resolution retains Rust 1.85 for Runtime, Model, SQLite, and observability while
declaring Rust 1.88 only for the optional genai adapter; the stable full
workspace gate remains mandatory.
Stage 18 adds the separate Rust 1.85 local Tool Runtime with Model-only normal
dependency, registration-time schema compilation, Future-drop cancellation,
deterministic batches, offline Group Node integration, and no Core changes.
Stage 18.1 retains concrete Schema sources, changes fail-fast to
stop-scheduling plus drain-started outcomes, makes observer failures
non-overwriting and panic-safe, and adds ToolCallId-safe message helpers without
changing Core or Model semantics.
Stage 19 adds the separate client-only MCP Tool adapter with exact rmcp 2.2.0,
reusable stdio sessions, paginated immutable discovery, conservative behavior,
fail-closed result mapping, explicit shutdown, offline process lifecycle
coverage, and no Core, Model, or Tool reverse dependency.
Stage 19.1 makes pagination adapter-owned and bounded with cursor-cycle
detection and no partial snapshot, makes MCP error classification exact,
rejects duplicate behavior overrides, redacts Tool-set Debug, and hardens stdio
with explicit close/kill/wait plus a runtime-independent direct-child Drop
fallback.
Stage 19.2 makes shutdown cleanup and completion Session-owned, maps rmcp outer
and `QuitReason` JoinErrors to source-preserving `ShutdownFailed`, keeps cleanup
running after waiter cancellation, defines zero-grace immediate kill/wait, and
limits observable guarantees to errors rmcp 2.2.0 actually returns.
Stage 19.3 separates service and child cleanup outcomes so worker panic cannot
skip direct-child reap, publishes `CLOSED` before waking completion waiters,
and documents standard-thread reaper creation failure as a best-effort Drop
boundary.
