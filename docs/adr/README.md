# Architecture Decision Records

ADRs capture durable, high-load repository decisions. They explain why a
boundary exists; current behavior remains documented in
[ARCHITECTURE.md](../../ARCHITECTURE.md) and the design documents.

## Status

Use one of:

- **Proposed**: under active design; not an accepted repository contract.
- **Accepted**: current decision.
- **Superseded**: replaced by a newer ADR, which must be linked.
- **Deprecated**: retained for compatibility but no longer recommended.

## Numbering and filenames

Numbers are monotonic three-digit identifiers. Filenames use a stable,
lowercase descriptive slug:

```text
NNN-short-decision-name.md
```

Do not renumber accepted ADRs. Corrections update status and link a new ADR
when the decision changes materially.

## Required sections

Every ADR contains:

- Status
- Context
- Decision
- Alternatives
- Consequences
- Related documents

ADRs should not duplicate implementation walkthroughs, test lists, or stage
chronology.

## Current records

1. [Core remains model and Tool agnostic](001-core-model-tool-agnostic.md)
2. [Nodes read immutable State and return Updates](002-immutable-state-updates.md)
3. [Super-step merge is deterministic](003-deterministic-superstep-merge.md)
4. [Durable checkpoints are storage-neutral](004-storage-neutral-checkpoints.md)
5. [The Model facade validates before adapter execution](005-validated-model-facade.md)
6. [ToolRuntime centralizes execution policy](006-tool-runtime-policy.md)
7. [MCP is a Tool backend](007-mcp-tool-backend.md)
8. [Remote MCP Tools use conservative side-effect defaults](008-conservative-remote-tool-behavior.md)
9. [MCP shutdown completion belongs to the Session](009-mcp-session-shutdown.md)
10. [Base APIs are stable while edge adapters remain experimental](010-stable-base-experimental-adapters.md)
11. [The workspace uses layered MSRV](011-layered-msrv.md)

