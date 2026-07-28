# ADR-006: ToolRuntime centralizes execution policy

## Status

Accepted

## Context

Local and remote-backed Tools need one definition of schema validation,
timeouts, side effects, batching, fail-fast, observers, and ToolCall identity.
Duplicating those rules in adapters would create incompatible execution facts.

## Decision

`group-agent-tool` owns immutable registration, precompiled JSON Schema,
per-call timeout, bounded spawn-free batches, side-effect behavior,
stop-scheduling-and-drain fail-fast, observer semantics, and ToolMessage
helpers.

## Alternatives

- Execute Tools inside Model.
- Put ToolRuntime in Core.
- Let every adapter implement its own batch and retry policy.

## Consequences

Local and MCP Tools share one execution contract. Runtime adds no hidden retry,
rollback, exactly-once guarantee, sandbox, or durable idempotency store.

## Related documents

- [Model and Tools](../design/model-and-tools.md)
- [ADR-007](007-mcp-tool-backend.md)

