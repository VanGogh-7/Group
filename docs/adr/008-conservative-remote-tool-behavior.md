# ADR-008: Remote MCP Tools use conservative side-effect defaults

## Status

Accepted

## Context

A transport response, timeout, annotation, or local Future drop cannot prove
that a remote Tool did not perform a side effect. Automatic parallelism or
retry could duplicate non-idempotent work.

## Decision

Discovered MCP Tools default to `NonIdempotentWrite` and serial execution.
Only an exact application-provided server/Tool override validated during
discovery can relax behavior. The adapter never retries automatically.

## Alternatives

- Trust server annotations as enforcement.
- Default remote Tools to read-only.
- Retry transport failures automatically.

## Consequences

The default sacrifices concurrency for correctness. Applications with an
external durable idempotency protocol may opt into broader behavior explicitly.

## Related documents

- [MCP Adapter](../adapters/mcp.md)
- [Model and Tools](../design/model-and-tools.md)

