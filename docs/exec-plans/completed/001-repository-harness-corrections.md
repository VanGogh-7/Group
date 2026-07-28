# H-001.1 Repository Harness Corrections

## Status

Completed

- [x] Baseline recorded.
- [x] Design reviewed.
- [x] Slices implemented.
- [x] Verification passed.
- [x] Independent review completed.
- [x] Review accepted and written back by a write-authorized role.
- [x] Implementation completion evidence recorded.

## Goal

Correct the repository Harness after the H-001 independent review by making
the layered MSRV documentation accurate, making `scripts/verify` portable and
lockfile-safe with explicit network semantics, and removing the conflict
between a read-only reviewer and Plan writeback.

## Non-goals

- Implement Stage 21 or create a Stage 21 / Prebuilt Agent Plan.
- Modify crates, tests, examples, benches, Cargo manifests, `Cargo.lock`,
  toolchain configuration, migrations, or product public APIs.
- Add dependencies, a Markdown checker, CI, provider integration, or MCP
  protocol behavior.
- Modify, track, publish, or link any excluded local-only artifact.
- Move this Plan to `completed/` before an accepted independent review.
- Create a Git commit.

## Context

- [Architecture](../../../ARCHITECTURE.md)
- [Layered MSRV ADR](../../adr/011-layered-msrv.md)
- [Development Runbook](../../runbooks/development.md)
- [Independent Review Runbook](../../runbooks/review.md)
- [Execution Plan workflow](../README.md)
- [H-001 bootstrap Plan](000-repository-harness-migration.md)
- [`scripts/verify`](../../../scripts/verify)

Starting evidence recorded on 2026-07-29:

- HEAD: `a9ac934fe6b4b9bf677ab89c94764123b2c016ea`.
- The worktree already contains the uncommitted H-001 documentation and
  Harness migration. Those user changes must be preserved.
- `git diff -- crates` is empty.
- `git diff -- Cargo.toml Cargo.lock rust-toolchain.toml` is empty.
- Protected local-only artifacts recorded at baseline are outside this Plan and
  remain unchanged.

The H-001 independent review concluded **PASS WITH MINOR FIXES** and reported
three bounded Harness findings:

1. The Genai adapter document retained a pre-MCP workspace MSRV matrix and
   implied that only Genai required Rust 1.88.
2. `scripts/verify` depended on the caller's current directory, did not apply
   `--locked` consistently, lacked an explicit offline opt-in contract, and
   omitted `git diff --check`.
3. The workflow simultaneously required Codex B to remain read-only and to
   write findings into the Plan; H-001 was also archived before its independent
   review as an undocumented lifecycle exception.

These are findings from H-001. They are not an H-001.1 review conclusion.

## Invariants

- Core, Model, Tool, SQLite, and Observability remain Rust 1.85.
- Genai and MCP remain Rust 1.88; the full workspace remains Rust 1.88+.
- Cargo validation resolves exactly the checked-in lockfile.
- Verification never contacts a live Model Provider, MCP Server, or external
  test service. Cargo may fetch locked dependencies unless explicit offline
  mode is enabled.
- `GROUP_VERIFY_OFFLINE=1` makes Cargo offline failures fail closed without
  network fallback.
- The User / Product Owner approves product scope and final acceptance; the
  Mentor / Orchestrator guides technical direction and task boundaries; Codex
  A implements and records evidence; Codex B reviews strictly read-only by
  default.
- Only Codex A or a separately authorized write-enabled closure session writes
  accepted independent-review findings back to a Plan.
- H-001.1 cannot claim an independent conclusion that has not occurred.
- Existing product code, Cargo/toolchain files, migrations, excluded
  local-only artifacts, and unrelated worktree changes remain untouched.

## Proposed design

Keep the authoritative MSRV matrix in `ARCHITECTURE.md`, ADR-011, and the
executable `scripts/verify msrv` mode. Reduce the Genai document to its own
Rust 1.88 requirement plus links to those sources.

Have `scripts/verify` derive its directory from `BASH_SOURCE[0]`, enter the
repository root, optionally export Cargo offline mode, and add `--locked` to
every applicable Cargo gate. Put `git diff --check` in the low-cost `fast`
mode; `all` continues to compose `full` and `msrv`.

Document the four roles and a handoff in which the independent reviewer emits
a read-only report, the User and Mentor accept or reject it, and a
write-authorized Implementer or closure session records it before archival.

## Implementation slices

### Slice 1: MSRV and verification contract

- [x] Correct the Genai MSRV section and search all public Harness documents
  for the same stale command or claim.
- [x] Make `scripts/verify` current-directory independent.
- [x] Add `--locked`, explicit offline opt-in, and `git diff --check`.
- [x] Document Cargo network behavior without implying live-service access.
- [x] Slice verification and self-review.

### Slice 2: Roles and lifecycle

- [x] Define User / Product Owner, Mentor / Orchestrator, Codex A /
  Implementer, and Codex B / Independent Reviewer.
- [x] Define read-only review reporting, acceptance, writeback, and archival
  ownership consistently across the four workflow documents.
- [x] Record the H-001 `PASS WITH MINOR FIXES`, findings, H-001.1 link, and
  bootstrap exception without claiming an H-001.1 result.
- [x] Slice verification and self-review.

### Slice 3: Acceptance and review handoff

- [x] Run all required verification modes and direct acceptance commands.
- [x] Recheck protected local-only artifacts, HEAD, and worktree scope.
- [x] Set this Plan to `In Review` and keep it in `active/`.
- [x] Record implementation evidence without marking independent review
  complete.

## Acceptance criteria

- [x] Genai and MCP are documented as Rust 1.88, the foundation layer as Rust
  1.85, and the full workspace as Rust 1.88+.
- [x] The Genai document no longer duplicates a complete workspace command
  matrix or excludes only Genai from Rust 1.85 workspace commands.
- [x] Every applicable Cargo command in `scripts/verify` uses `--locked`.
- [x] The script works from the repository root and an arbitrary current
  directory.
- [x] Default verification avoids live external services while allowing Cargo
  to fetch locked dependencies; explicit offline mode exports
  `CARGO_NET_OFFLINE=true` and never falls back to the network.
- [x] `fast` remains low cost, `full` remains the full quality gate, `msrv`
  remains layered, and `all` remains `full + msrv`.
- [x] The four roles, scope approval, read-only review, Plan writeback, and
  archive transition have one consistent contract.
- [x] H-001 records its independent review and bootstrap exception.
- [x] H-001.1 is completed only after accepted independent review, correction,
  re-verification, and writeback; no Stage 21 Plan exists.
- [x] Product code, Cargo/toolchain files, migrations, and excluded local-only
  artifacts remain unchanged; HEAD is unchanged.
- [x] No Git commit is created.

## Verification

```text
bash -n scripts/verify
./scripts/verify fast
./scripts/verify full
./scripts/verify msrv
./scripts/verify all
REPO_ROOT="$(pwd)"
cd /tmp
"$REPO_ROOT/scripts/verify" fast
cd "$REPO_ROOT"
GROUP_VERIFY_OFFLINE=1 ./scripts/verify fast
cargo metadata --locked --no-deps --format-version 1
rg -n 'only Genai|exclude group-agent-genai' docs README.md ARCHITECTURE.md AGENTS.md
rg -n 'Codex B|Independent Reviewer|write.*Plan|Review Findings' docs AGENTS.md
rg -n 'Stage 21|Prebuilt Agent' docs/exec-plans/active
git diff --check
git diff -- crates
git diff -- Cargo.toml Cargo.lock rust-toolchain.toml
git status --short
git diff --stat
git rev-parse HEAD
```

Independent review must additionally validate Markdown links without adding a
new checker dependency.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-07-29 | Treat this as H-001.1 Harness correction, not a product Stage. | All findings concern repository documentation and verification orchestration. |
| 2026-07-29 | Keep the full MSRV matrix out of the Genai adapter document. | Architecture, ADR-011, and executable verification are less drift-prone sources. |
| 2026-07-29 | Make offline behavior explicit opt-in rather than the default. | Clean environments may need Cargo to fetch dependencies already fixed by `Cargo.lock`; live service tests remain forbidden. |
| 2026-07-29 | Separate read-only review output from Plan writeback. | Independence and repository mutation authority must not conflict. |
| 2026-07-29 | Keep H-001.1 active through independent review. | The H-001 early archival was a one-time bootstrap exception and must not define the normal lifecycle. |
| 2026-07-29 | Accept the independent `PASS WITH MINOR FIXES` disposition and close the new privacy finding in this write-authorized session. | Generic protected-local-artifact evidence preserves the scope audit without making public Harness documents depend on private paths or digest values. |

## Review findings

Independent Review conclusion: **PASS WITH MINOR FIXES**.

The review confirmed that all three findings inherited from H-001 are closed:

1. The Genai MSRV text now covers both Rust 1.88 adapters and routes the full
   workspace matrix to Architecture, ADR-011, and `scripts/verify msrv`.
2. `scripts/verify` is repository-root independent, consistently lockfile-safe,
   exposes explicit offline behavior, and includes `git diff --check`.
3. The four roles and the read-only report, acceptance, writeback, and archive
   lifecycle no longer require the Independent Reviewer to edit the Plan.

One new **Minor** finding applied to the public active Plan:

- **Observed evidence:** the Plan included a concrete path and command for an
  excluded local-only artifact.
- **Risk:** the command could not run in an ordinary clone and made the public
  Harness appear to depend on private material.
- **Required correction:** remove the specific filename, path, command, and
  digest; retain only generic protected-local-artifact scope and evidence.
- **Disposition:** corrected in this authorized closing session and verified
  by a zero-match public private-path search. The correction is bounded
  Harness documentation work and does not require H-001.2.

The User / Product Owner and Mentor / Orchestrator accepted this disposition.
This write-authorized Codex A / Closing Session recorded the result after
correction and targeted re-verification.

## Completion evidence

- HEAD remains `a9ac934fe6b4b9bf677ab89c94764123b2c016ea`; no commit was
  created.
- `bash -n scripts/verify` passed.
- `./scripts/verify fast`, `full`, `msrv`, and `all` each passed.
- Calling the absolute `scripts/verify fast` path from `/tmp` passed, proving
  repository-root discovery is independent of the caller's current directory.
- `GROUP_VERIFY_OFFLINE=1 ./scripts/verify fast` passed from the existing local
  Cargo cache. Source inspection confirms the script exports
  `CARGO_NET_OFFLINE=true` before Cargo runs, so a missing cache cannot fall
  back to the network.
- `cargo metadata --locked --no-deps --format-version 1` passed and reported
  Rust 1.85 for Core, Model, Tool, SQLite, and Observability and Rust 1.88 for
  Genai and MCP.
- Searches found no other Rust 1.85 workspace command that excludes Genai
  without also excluding MCP. Role, writeback, and Stage 21 searches matched
  only the intended current contracts, historical findings, and explicit
  non-goals; no Stage 21 Plan exists.
- `git diff --check` passed. `git diff -- crates` and
  `git diff -- Cargo.toml Cargo.lock rust-toolchain.toml` remain empty.
- The public private-path search returned zero matches.
- A one-time read-only check resolved 130 relative targets and four anchors
  across 32 public Markdown files.
- Protected local-only artifacts remained unchanged during implementation and
  review.
- Product code, Cargo manifests, toolchain configuration, migrations, and
  excluded local artifacts were outside the task scope and remained unchanged.
- The active Plan directory contains only its README; no Stage 21 or Prebuilt
  Agent Plan was created.
- `git mv` could not operate because the H-001 Harness files are not yet
  tracked at the current HEAD. The Closing Session used a controlled file move
  without staging unrelated Harness work; the completed path and navigation
  are correct.
- No Git commit was created.
- This was documentation and shell orchestration only. Full benchmark
  compilation passed; no product runtime or performance path changed.
- The accepted Review Findings were written back before this Plan was marked
  Completed and archived.
