# ADR-007: MCP is a Tool backend

## Status

Accepted

## Context

MCP adds remote discovery, naming, transport, and result mapping. Schema,
timeout, batch, side-effect, fail-fast, and ToolMessage behavior already belong
to ToolRuntime.

## Decision

`group-agent-mcp` converts discovered remote Tools into the existing Tool
trait and immutable Registry. It does not create a second Tool execution
system, and rmcp types do not enter Model or Core.

## Alternatives

- Give MCP its own Registry and batch runtime.
- Expose rmcp request and response types through Model.
- Add MCP directly to Core.

## Consequences

Local and remote Tools behave consistently. MCP remains responsible for
session, discovery, transport, naming, mapping, and shutdown lifecycle.

## Related documents

- [MCP Adapter](../adapters/mcp.md)
- [ADR-006](006-tool-runtime-policy.md)

