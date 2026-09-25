# Durable Execution Design

This document describes the current checkpoint, replay, and branch contracts.
The stable public boundary is storage-neutral; SQLite is one adapter.

## Two-Agent sequence durability

AgentSequence uses one parent Checkpointer<SequenceSnapshot>; there are no child
stores to coordinate. Durable start requires EverySuperstep. Resume and Fork
explicitly force EverySuperstep regardless of supplied Core policy. Approval
Resume requires an explicit latest checkpoint pin and a SequenceApprovalDecision
whose stage matches the active node. Approve/reject reuse existing ToolRuntime
semantics. Missing, stale, wrong-type or wrong-stage decisions do no Tool work.

SequenceSnapshotCodec uses `group-agent-prebuilt-agent-sequence-state/1/json`
and `group-agent-prebuilt-agent-sequence-approval/1/json`; the slash-separated text
here represents descriptor fields. Existing Agent descriptors and SQLite
migrations are unchanged. Snapshots contain phase plus canonical first/optional
second Agent snapshots. They exclude sinks, models, Tools, mapper code and derived
validated JSON. Decode/restore checks transcript pairing and phase/round structure;
node guards check active frontier, configured budgets and completed output before
model/mapper/Tool effects. Completed zero-node recovery also validates results.

The graph identity hashes the application revision and ordered stage IDs, limits,
approval flags and output contract IDs using the encoding in
[Stage 23](../specs/023-durable-agent-sequence.md). Bump revision when mapper,
prompt or Tool semantics change. The digest is configuration identity, not a
signature of application code or Store content. Mismatch fails before execution
or branch creation. Fork guards and final conversion can fail after branch
creation; no rollback is implied.

From a saved first FinalAnswer, Resume may rerun the pure mapper but does not
rerun the first Agent. Once handoff messages are saved, the mapper is not rerun.
Saved second approval, Tool results and completed sequence similarly resume their
own boundary. An external effect before its checkpoint save can remain unknown
and may repeat; CAS does not grant exclusive execution. Callers coordinate
concurrent approval attempts. Replay writes no lineage but may re-execute model,
mapper and Tool work from unfinished checkpoints; it is not effect-free inspection.


## Prebuilt structured result recovery

Prebuilt's optional `structured-output` feature adds
`ToolCallingAgent::new_with_output(model, tools, config, contract)` and
`AgentOutcome::structured_output()`. AgentConfig remains Copy. Plain Agents and
MaxRounds outcomes return no structured result. Valid final responses are
checked by the Model facade before the model update is committed.

Structured Agents use new graph version identities incorporating SHA-256 of
compact canonical JSON `["group-json-output/1", name, schema]`, recursively
sorting object keys while preserving arrays. Schema annotations and contract
name affect identity. Plain/approval identities and snapshot/codec bytes are
unchanged. Changed contract or plain-mode recovery fails Core compatibility
checks before execution or Fork branch creation.

Validated values are transient and absent from snapshots. Completed Resume,
Replay and Fork revalidate saved final Assistant text; missing final messages
also fail. Conversion errors directly expose `StructuredOutputError`, while
runtime failures still expose `GraphRunError`. Conversion occurs after Core
returns: a Fork may already have created its branch and checkpoint when output
validation fails. Replay is read-only; a failed conversion cannot roll back a
prior Core write. The digest prevents configuration drift, not malicious Store
tampering. No exactly-once execution claim is made.


## Capability split

Durability does not add Clone or Serde bounds to `GraphState`.

- `CheckpointState` creates and restores an application-defined Snapshot.
- `CheckpointRecord` is the durable storage-neutral domain record.
- `CheckpointCodec<T>` converts Snapshot and supported interrupt payloads to
  deterministic bytes.
- `CheckpointStore` exchanges records and lineage metadata.
- `Checkpointer<T>` adapts a Store and Codec to typed Runtime operations.

Snapshot and codec work is synchronous, outside storage locks. Record queries
return shared Arc values rather than deep copies.

`RecordCheckpointer` holds weak decoded-checkpoint references. Repeated reads
share a checkpoint while callers retain its handle; releasing all handles
allows its snapshot to be reclaimed, and later reads decode from the Store.
Expired cache entries, including their encoded records, are removed on later
cache insertions. Cleanup runs after an insertion/replacement budget
based on surviving handles, with a minimum interval of 64 operations, avoiding
a full-cache scan on every save. Bucket capacity is reduced during cleanup. This is amortized cleanup, not immediate
reclamation of all encoded bytes during idle periods or a byte-size limit.
The Store remains responsible for durable content idempotency and lineage CAS;
the cache compares complete record content for entries it still retains.

## Save boundary

Checkpointing is opt-in. A normal invocation does not snapshot, call a Store,
or acquire checkpoint locks.

A checkpoint is saved only after:

1. every Node in the frontier succeeds;
2. State commit succeeds;
3. successor routing succeeds;
4. any required Store operation succeeds.

`EverySuperstep` saves each successful boundary. `FinalOnly` saves only the
completed empty-frontier boundary. Interrupt checkpoints are mandatory
regardless of that policy.

## Record and lineage

A Record includes identifiers, parent, graph version, format version,
cumulative step and super-step, canonical next frontier, snapshot bytes,
completed state, and optional interrupt metadata.

Content idempotency and lineage CAS solve different problems:

- the Runtime-assigned CheckpointId identifies one complete logical write;
- identical replay returns the original Record even after the head advances;
- different content under the same ID is an idempotency conflict;
- expected-parent CAS prevents concurrent writers from silently cross-linking
  lineage.

Idempotency lookup precedes CAS. Both checks and insertion are atomic.

## Resume

Resume selects a specified or latest checkpoint on one thread or branch. A
specified checkpoint must still be the selected head; historical continuation
requires Fork.

Before restore, Runtime validates:

- thread and branch ownership;
- graph and format compatibility;
- completion/frontier consistency;
- interrupt metadata;
- each structured NodePath;
- uniqueness, canonical order, and one graph namespace.

Only the saved frontier is resolved, and the resolved indices are reused for
execution. Restore occurs outside Store locks. Saved counters remain
cumulative while `max_steps` is an additional budget for this call.

`CompiledGraph::resume_with_state_initializer` additionally accepts a synchronous,
fallible callback for attaching invocation-local resources after State restore.
It runs exactly once after validation, successful restore, and control checks,
including for a completed checkpoint. It must preserve restored durable fields
and the saved frontier's meaning; it is not a historical editing API. Resources
attached this way must be excluded from snapshots. Callback failures retain
their concrete `SnapshotError` under `GraphRunError::RestoreFailed`, before
resume-success events, Node execution, or checkpoint writes. Cancellation and
deadlines are checked before and after initialization. Like synchronous restore,
the callback cannot be preempted mid-call and must remain lightweight. Ordinary
`resume` delegates with a no-op callback.

## Replay

Replay loads one exact CheckpointId through `get`. It does not select latest,
query a head after loading, perform CAS, create a branch, or write a
checkpoint.

Replay restores historical State and reuses the execution kernel with writes
disabled. A completed source is a no-op. A new interrupt is unsupported
because Replay cannot persist it.

Replay re-executes user code and can duplicate external side effects. It is
read-only with respect to Group durability, not a sandbox.

## Fork and branches

Fork is the only branch creation operation. It loads an exact historical
checkpoint, validates and restores it, then creates a new `BranchId` whose
initial head is that source.

Branch metadata is additive Store state, not a change to
`CheckpointRecord`. Each branch has:

- exactly one owning ThreadId;
- one immutable source;
- one current head;
- membership for branch-only descendants;
- independent expected-parent CAS.

Branch history begins with the shared source and continues through descendants
whose ordinary parent IDs form one complete chain. Duplicate branch creation
is an error, not idempotent success.

## Interrupt durability

An interrupted Record retains an InterruptId, NodePath, typed or encoded
payload, unchanged committed snapshot, counters, and singleton frontier.
Resume requires a value for an interrupted checkpoint and rejects one for a
normal checkpoint.

Process-local payloads are allowed only with a process-local Checkpointer.
Record-backed storage must fail if its Codec cannot encode the payload.

## SQLite adapter

`group-agent-checkpoint-sqlite` uses:

- embedded migrations;
- UUIDs as exact 16-byte values;
- `u64` counters as sortable eight-byte big-endian blobs;
- short SQLx-tracked `BEGIN IMMEDIATE` write transactions;
- one-transaction branch reads with JOIN-based lineage validation;
- triggers and constraints for ownership, membership, and head continuity.

Busy or lock errors remain storage failures, never false lineage conflicts.
Codec work does not run inside SQLite transactions.

## Stability

The Record, Codec, Store, idempotency, CAS, Resume, Replay, Fork, ownership,
membership, and lineage contracts are in the compatibility-first base API
set. New backends should implement these ports without changing Core to depend
on a database library.

## Direct evidence

- `crates/group-agent-core/src/checkpoint_record.rs`
- `crates/group-agent-core/src/checkpoint_codec.rs`
- `crates/group-agent-core/src/checkpoint_store.rs`
- `crates/group-agent-core/src/checkpoint.rs`
- `crates/group-agent-core/src/runtime.rs`
- `crates/group-agent-checkpoint-sqlite/src/lib.rs`
- `crates/group-agent-checkpoint-sqlite/migrations/`
- Core `resume.rs`, `replay.rs`, `fork.rs`, and `durable_checkpoint.rs` tests
- SQLite `store.rs` and `restart.rs` tests

Related decision:
[ADR-004](../adr/004-storage-neutral-checkpoints.md).

## Process termination recovery example

The offline Prebuilt integration test is a runnable process-recovery example:

```bash
cargo test --locked -p group-agent-prebuilt --test process_recovery
```

It launches a real worker process with a file-backed SQLite store, waits for an
explicit checkpoint-boundary marker, forcibly terminates and reaps that worker,
and launches a new process with a new Agent and checkpointer. On Unix it also
asserts SIGKILL termination. Three scenarios cover:

- a persisted approval interrupt followed by approval and one Tool execution;
- the same interrupt followed by rejection and zero Tool executions;
- a persisted Tool result followed by final model work without another Tool
  execution.

The fixture uses an offline streaming model and a file-backed Tool execution
journal shared by both processes. The recovery worker checks the restored
transcript, cumulative rounds, final answer, and completed durable head. Process
waits are bounded and child guards kill/reap workers on assertion failure.

This demonstrates process termination after known committed boundaries, not
power-loss recovery or termination during a SQLite transaction. The external
effect window before the Tool result checkpoint remains: a resumed invocation
may repeat an effect that happened without a saved result. Applications must
handle that window; this example does not provide exactly-once execution.
