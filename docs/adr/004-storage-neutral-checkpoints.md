# ADR-004: Durable checkpoints are storage-neutral

## Status

Accepted

## Context

Checkpointing must not impose Clone, Serde, SQLx, or a specific database on
State or Core. Retry identity and concurrent lineage also require different
checks.

## Decision

Separate Snapshot capability, durable `CheckpointRecord`, `CheckpointCodec`,
`CheckpointStore`, and typed `Checkpointer`. Check content idempotency before
expected-parent CAS. Run snapshot and codec work outside Store locks.

## Alternatives

- Serialize State directly inside Core.
- Put SQLx types in Core.
- Treat operation retry and head CAS as one condition.

## Consequences

Applications own schema and encoding; adapters can be implemented without
changing Core. The port is more explicit, and deterministic canonical encoding
is required for logical idempotency.

## Related documents

- [Durable Execution](../design/durable-execution.md)
- [Architecture](../../ARCHITECTURE.md)

