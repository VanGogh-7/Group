# ADR-010: Base APIs are stable while edge adapters remain experimental

## Status

Accepted

## Context

Core, Durable, Model, and Tool contracts have been exercised across
concurrency, persistence, provider, local Tool, and MCP integrations. Genai and
MCP configuration surfaces remain coupled to fixed, evolving upstream
releases.

## Decision

Evolve the base APIs compatibility-first through additive changes and explicit
migrations. Keep provider-specific configuration, extension keys,
stable-target policy, MCP constructors, discovery settings, and future
HTTP/OAuth surfaces experimental.

## Alternatives

- Treat every public item as equally frozen.
- Treat all v0.x APIs as disposable.
- Move upstream types into the base layers.

## Consequences

Applications can build on stable ports while accepting adapter-level migration
risk. `Stable` does not mean immutable, and `experimental` does not mean
untested.

## Related documents

- [Architecture stability boundary](../../ARCHITECTURE.md#stability-boundary)
- [Quality and Release Status](../quality.md)
