# Durable Execution Design

This document describes the current checkpoint, replay, and branch contracts.
The stable public boundary is storage-neutral; SQLite is one adapter.

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

