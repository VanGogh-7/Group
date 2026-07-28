# ADR-003: Super-step merge is deterministic

## Status

Accepted

## Context

Async Node completion order is nondeterministic. Applying each result as it
arrives would make State and successor order scheduler-dependent.

## Decision

Runtime polls a frontier concurrently without per-Node spawn, waits at a
barrier, restores Updates to stable compiled-Node order, commits one complete
batch, and only then routes. Successors are sorted and deduplicated by compiled
index.

## Alternatives

- Apply Updates on completion.
- Use task arrival order as reducer order.
- Spawn detached Node tasks and collect later.

## Consequences

Merge and frontier order are reproducible while completion events may remain
nondeterministic. A slow Node delays the barrier, and custom reducers must
validate the entire batch before mutation.

## Related documents

- [Core Runtime](../design/core-runtime.md)
- [ADR-002](002-immutable-state-updates.md)

