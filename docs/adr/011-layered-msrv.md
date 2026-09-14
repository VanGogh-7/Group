# ADR-011: The workspace uses layered MSRV

## Status

Superseded on 2026-09-15 by [ADR-012](012-unified-msrv.md).
The decision below records the former policy.

## Context

The fixed Genai and rmcp releases require Rust syntax newer than the foundation
crates. Raising every crate would impose an adapter dependency constraint on
users who do not select those adapters.

## Decision

Keep Core, Model, Tool, Prebuilt, SQLite, and Observability at Rust 1.85.
Declare Genai and MCP at Rust 1.88 and validate both layers independently. The
full workspace therefore requires Rust 1.88 or newer.

## Alternatives

- Raise the entire workspace to Rust 1.88.
- Patch or vendor upstream releases.
- Use nightly or `RUSTC_BOOTSTRAP`.

## Consequences

Foundation users retain the lower compiler floor. CI and release validation
must run more than one toolchain, and adapter packages document their higher
MSRV explicitly.

## Related documents

- [Current architecture MSRV policy](../../ARCHITECTURE.md#msrv-policy)
- [Development Runbook](../runbooks/development.md)
