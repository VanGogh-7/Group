# H-001 Repository Harness Migration

## Status

Completed

- [x] Baseline worktree, HEAD, note hash, document inventory, and document sizes recorded.
- [x] Existing README and AGENTS headings and Markdown links inventoried.
- [x] Stable documentation hierarchy created.
- [x] AGENTS reduced to the mandatory agent entrypoint.
- [x] README reduced to the five-minute user entrypoint.
- [x] Current architecture and stable design extracted from stage-oriented documents.
- [x] High-load ADRs migrated.
- [x] Execution Plan and review runbooks established.
- [x] Unified verification script implemented.
- [x] Internal links and protected paths verified.
- [x] Full acceptance commands passed.
- [x] Independent review completed after the bootstrap archive.

## Goal

Migrate Group from stage-prompt-centered repository guidance to a sustainable
Harness Engineering workflow:

- concise agent and user entrypoints;
- one current architecture source of truth;
- stable design, ADR, plan, runbook, quality, adapter, and history documents;
- tracked plans for complex work;
- one executable verification entrypoint;
- prompts that describe task deltas instead of restating repository knowledge.

## Non-goals

- Implement Stage 21 or a prebuilt Agent.
- Change product code, tests, public APIs, dependencies, MSRV, Cargo metadata,
  database migrations, or runtime behavior.
- Resolve the release debt identified by Stage 20.
- Modify, track, publish, or link any excluded local-only artifact.
- Create a Git commit.

## Current repository problems

Baseline at task start:

- HEAD: `a9ac934fe6b4b9bf677ab89c94764123b2c016ea`.
- Tracked worktree: clean.
- Protected local-only artifacts were recorded at baseline and kept outside
  the public Harness.
- README: 1,982 lines.
- AGENTS: 1,102 lines.
- Public docs: README, AGENTS, and `docs/adapters/genai.md`.

Observed problems:

- README mixes user onboarding, current architecture, internal review history,
  benchmark details, API migrations, and maintainer workflow.
- AGENTS duplicates most runtime, durability, provider, Tool, and MCP design
  details instead of routing agents to stable sources.
- Stage history and current contracts are interleaved, which makes it difficult
  to distinguish historical corrections from current behavior.
- There is no tracked Execution Plan harness, review runbook, quality ledger,
  ADR index, or unified validation command.
- Repeating repository constraints in prompts invites drift between copies.

## Architecture/documentation invariants

- Current code and executable tests outrank documentation.
- Core remains independent of Model, Tool, Provider, MCP, SQLx, and adapters.
- Nodes read immutable state and return updates; Runtime alone applies updates.
- State is not required to implement Clone or Serde.
- Parallel super-steps use deterministic stable merge and no per-node spawn.
- Durable Record, Codec, Store, idempotency, and lineage CAS remain separate.
- Resume is latest-only, Replay is exact and read-only, and Fork creates the
  only writable historical branch.
- Provider details stay in adapters; ToolRuntime owns schema, timeout, batch,
  fail-fast, and side-effect policy.
- MCP remains a Tool backend and supports production stdio only.
- No layer performs hidden retry.
- Group-owned default formatting does not expose payload or secret content.
- Base crates remain Rust 1.85; Genai and MCP remain Rust 1.88.
- Core, Durable, Model, and Tool base APIs retain the Stage 20 stability
  recommendation; Genai and MCP adapter surfaces remain experimental.
- The repository has a lower-level technical loop but no Stage 21 prebuilt
  Agent.
- RAG, PDF/OCR, memory product logic, UI, and product policy remain outside
  Group Core.

## Implementation slices

### Slice 1: Harness skeleton and navigation

- [x] Add `ARCHITECTURE.md` and `docs/index.md`.
- [x] Add design, adapter, ADR, plan, runbook, history, and quality navigation.
- [x] Keep every tracked document substantive; no empty placeholders.

### Slice 2: Current architecture and stable design

- [x] Move current cross-crate facts into `ARCHITECTURE.md`.
- [x] Split runtime, durability, Model/Tool, and control/error/observability
  detail into four design documents.
- [x] Preserve and normalize the Genai adapter document.
- [x] Add the MCP adapter document.

### Slice 3: Entry surfaces and history

- [x] Rewrite AGENTS to 100-180 lines.
- [x] Rewrite README to a 250-500-line five-minute entrypoint.
- [x] Move Stage 1-20 history out of README and AGENTS.

### Slice 4: ADR and workflow harness

- [x] Add the ADR format/index and eleven high-load decisions.
- [x] Add Execution Plan instructions, template, and active/completed indexes.
- [x] Add development and independent-review runbooks.
- [x] Add the current quality and release-debt ledger.

### Slice 5: Unified verification

- [x] Add executable `scripts/verify` with `fast`, `full`, `msrv`, and `all`.
- [x] Make AGENTS, README, and runbooks use the script as the primary entrypoint.
- [x] Keep verification free of live external-service tests; Cargo dependency
  fetch behavior is corrected by H-001.1.

### Slice 6: Acceptance and closure

- [x] Check Markdown links and eliminate obsolete Stage 17 document paths.
- [x] Verify only authorized documentation and harness files changed.
- [x] Run fast, full, and MSRV verification plus direct acceptance commands.
- [x] Record review findings and completion evidence.
- [x] Mark this plan Completed and move it to `completed/`.

## Acceptance criteria

- [x] AGENTS is a concise mandatory entrypoint of about 100-180 lines.
- [x] README is a five-minute entrypoint of about 250-500 lines.
- [x] ARCHITECTURE describes current facts without stage chronology.
- [x] Stable design detail has one clear home and is linked rather than copied.
- [x] The eleven requested ADRs exist as substantive English documents.
- [x] Execution Plan template and active/completed workflow are usable.
- [x] Development and review runbooks define implementation and independent review.
- [x] `scripts/verify fast`, `full`, and `msrv` pass.
- [x] README, docs index, AGENTS, and ADR links resolve.
- [x] No public document names or links the excluded local learning-note path.
- [x] No product source, test, Cargo, lockfile, toolchain, or migration diff exists.
- [x] Protected local-only artifacts and HEAD remain unchanged.
- [x] No Git commit is created.

## Verification

Required:

```text
chmod +x scripts/verify
./scripts/verify fast
./scripts/verify full
./scripts/verify msrv
cargo fmt --all --check
cargo test --workspace
search for the obsolete Genai document path using the task-supplied pattern
search public Harness files for private local-only paths using the task-supplied pattern
find docs -maxdepth 4 -type f | sort
wc -l AGENTS.md README.md ARCHITECTURE.md
git diff --check
git status --short
git diff --stat
git diff -- crates
git rev-parse HEAD
verify protected local-only artifacts using the task-supplied external check
```

Additional harness checks:

- Validate relative Markdown links.
- Compare current file inventory with the authorized scope.
- Verify executable mode on `scripts/verify`.
- Run scoped checks on Cargo files, toolchain, migrations, and protected
  local-only artifacts.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-07-29 | Treat this as Harness task H-001, not a product Stage. | Repository process work must not imply a product capability change. |
| 2026-07-29 | Preserve the Stage 20 stability split: base APIs stable, Genai/MCP experimental. | The migration reorganizes facts; it does not reopen reviewed contracts. |
| 2026-07-29 | Keep detailed current behavior in architecture/design/adapter docs and historical corrections in one history document. | Separating current truth from chronology reduces drift. |
| 2026-07-29 | Use eleven high-load ADRs instead of mechanically splitting every historical ADR. | ADRs should capture durable decisions, not mirror a stage log. |
| 2026-07-29 | Keep `scripts/verify` as orchestration over existing Cargo commands. | The harness must add no dependency and change no product behavior. |
| 2026-07-29 | Keep README Quick Start on existing offline examples and route detailed contracts to stable docs. | The user entrypoint must be runnable without duplicating internal review history. |
| 2026-07-29 | Keep active/completed directory indexes substantive even when no other Plan is present. | Tracked directories should teach workflow rather than exist as empty placeholders. |
| 2026-07-29 | Close and archive the bootstrap H-001 Plan after implementation acceptance, with independent review still pending. | This was a one-time bootstrap exception required while establishing the Harness. Future Plans must remain active through accepted independent review, correction, re-verification, and writeback. |

## Review findings

The H-001 independent review concluded **PASS WITH MINOR FIXES**. It reported
three bounded findings:

1. The Genai adapter document retained a pre-MCP MSRV matrix, including Rust
   1.85 workspace commands that excluded Genai but not MCP.
2. `scripts/verify` assumed the caller was in the repository root, did not use
   `--locked` consistently, lacked explicit opt-in offline semantics, and did
   not include `git diff --check`.
3. The workflow required Codex B both to remain read-only and to write Review
   Findings into the Plan, while H-001 itself had been archived before review.

Corrections are tracked in
[H-001.1 Repository Harness Corrections](001-repository-harness-corrections.md).
H-001's archive-before-review order is a bootstrap exception, not a lifecycle
precedent. No later task may copy it: subsequent Plans remain active until the
read-only report is accepted, a write-authorized Implementer or closure session
records Review Findings, required corrections and verification pass, and the
Plan is ready to move to `completed/`.

This conclusion belongs to H-001 only. It does not claim or predict the
independent-review result for H-001.1.

## Completion evidence

- Repository baseline remains
  `a9ac934fe6b4b9bf677ab89c94764123b2c016ea`; no commit was created.
- Protected local-only artifacts remained excluded and unchanged.
- `AGENTS.md`, `README.md`, and `ARCHITECTURE.md` contain 171, 329, and 273
  lines respectively.
- A read-only link check resolved all 118 relative Markdown links.
- Searches found no obsolete Stage 17 Genai path and no public private-path
  reference.
- `scripts/verify fast`, `full`, `msrv`, and `all` passed.
- Direct `cargo fmt --all --check` and `cargo test --workspace` passed.
- `git diff --check` passed.
- `crates/`, Cargo manifests, Cargo lockfile, toolchain configuration, and
  database migrations have no diff.
- `scripts/verify` is executable. H-001.1 corrects its lockfile, current
  directory, and Cargo network contract without adding dependencies.
