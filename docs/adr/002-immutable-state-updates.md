# ADR-002: Nodes read immutable State and return Updates

## Status

Accepted

## Context

Shared writable State would make concurrent results depend on lock and
scheduler order, and it would allow partial mutation before a Node failure.

## Decision

An invocation owns State. Nodes receive `&State` and return typed Updates.
Runtime alone calls `apply` or `apply_batch`. State is not required to
implement Clone or Serde.

## Alternatives

- Pass `&mut State` to Nodes.
- Store State in `Arc<RwLock<_>>`.
- Let Nodes commit directly.

## Consequences

Parallel Nodes share one logical snapshot and failed work can be discarded
before commit. Applications must define explicit Update and reducer semantics.
Long synchronous reducers remain non-preemptible.

## Related documents

- [Core Runtime](../design/core-runtime.md)
- [ADR-003](003-deterministic-superstep-merge.md)

