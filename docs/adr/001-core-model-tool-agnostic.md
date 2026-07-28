# ADR-001: Core remains Model and Tool agnostic

## Status

Accepted

## Context

Graph execution changes slowly, while provider SDKs, message protocols, Tool
execution policy, and MCP transports change quickly. Coupling them would force
Core users to accept unrelated dependencies, MSRV changes, and semantics.

## Decision

`group-agent-core` does not normally depend on Model, Tool, Genai, MCP, SQLx,
or observability adapters. Applications and future prebuilt layers compose
those crates through ordinary Nodes and Core ports.

## Alternatives

- Add provider and Tool Nodes directly to Core.
- Use Core features to embed every optional integration.
- Let provider or rmcp types enter Core public APIs.

## Consequences

Core remains independently usable and stable. Applications write composition
code, and convenience layers must not become reverse dependencies.

## Related documents

- [Architecture](../../ARCHITECTURE.md)
- [Core Runtime](../design/core-runtime.md)
- [Model and Tools](../design/model-and-tools.md)

