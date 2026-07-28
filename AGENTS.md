# Group Repository Instructions

## Repository Purpose

Group is a strongly typed, durable, provider-neutral Rust agent graph runtime.
It provides deterministic async graph execution, durable execution ports,
model and Tool abstractions, and external adapters.

Group does not contain product RAG, PDF/OCR ingestion, memory extraction, UI,
authorization, or prompt policy. Core owns graph execution; provider,
transport, persistence, and product behavior remain in adapters or the
application.

## Sources of Truth

- [`README.md`](README.md): five-minute user entrypoint.
- [`ARCHITECTURE.md`](ARCHITECTURE.md): current architecture and dependency
  boundaries.
- [`docs/index.md`](docs/index.md): documentation navigation.
- [`docs/design/`](docs/design/): stable design details.
- [`docs/adr/`](docs/adr/): durable architecture decisions.
- [`docs/exec-plans/active/`](docs/exec-plans/active/): current complex work.
- [`docs/quality.md`](docs/quality.md): known debt and release status.

Current code and executable tests outrank stale documentation. Historical
context lives in [`docs/history/`](docs/history/), not in this file.

## Architecture Invariants

- Core does not depend on Model, Tool, Provider, MCP, SQLx, or adapters.
- A Node reads immutable `&State` and returns a typed Update.
- Runtime alone applies Updates; full State Clone is not required.
- Do not introduce `Arc<RwLock<State>>` or a global run lock.
- Do not spawn one task per Node or ToolCall.
- Parallel super-step merge and successor order are deterministic.
- Routing is synchronous, read-only, and happens after State commit.
- Durable Record, Codec, Store, content idempotency, and lineage CAS remain
  separate.
- Resume is latest-only, Replay is exact and read-only, and Fork is the only
  writable historical branch operation.
- Provider differences remain in provider adapters.
- ToolRuntime owns schema validation, timeout, batch, fail-fast, side-effect
  policy, and ToolMessage identity.
- MCP is a Tool backend, not a second Tool Runtime.
- No layer performs hidden retry or claims exactly-once execution.
- Default Group error and event formatting does not expose payload or secret
  content.
- Core, Model, Tool, SQLite, and Observability use Rust 1.85.
- Genai and MCP adapters use Rust 1.88.

Detailed rationale belongs in
[`ARCHITECTURE.md`](ARCHITECTURE.md) and
[`docs/design/`](docs/design/).

## Standard Workflow

1. Read this file and the relevant architecture, design, ADR, quality, and
   active Plan documents.
2. Record `git status --short`, the relevant diff, HEAD, and protected
   artifact hashes.
3. Decide whether the task requires an Execution Plan.
4. Implement the smallest independently verifiable slice.
5. Add direct behavior tests for behavior changes.
6. Run the unified verification command and task-specific checks.
7. Self-review the diff, performance, security, compatibility, and failure
   paths.
8. Update the active Plan and the authoritative documentation.
9. Report commands actually run, evidence, skipped checks, and remaining risk.
10. Hand complex work to an independent read-only review.

Preserve unrelated user changes in a dirty worktree. Scope exclusions are hard
boundaries, not suggestions.

## Task Sizing

A small, local, low-risk fix uses a short Task Brief.

Create a tracked Execution Plan for work involving multiple crates, public API,
concurrency, cancellation, persistence, checkpoint format, database behavior,
provider or MCP protocol, security-sensitive logging, or multiple sessions.

Split non-mechanical complex work into slices that can be implemented,
verified, and reviewed independently. Update the Plan while working.

Do not create a product Stage for ordinary fixes or repository engineering.
Use `Stage x.1` only for a Major correction to a product Stage.

See [`docs/exec-plans/README.md`](docs/exec-plans/README.md).

## Standard Commands

Primary entrypoints:

```bash
./scripts/verify fast
./scripts/verify full
./scripts/verify msrv
./scripts/verify all
```

Key native commands:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo bench --workspace --no-run
cargo check --workspace --all-targets --all-features
cargo tree --workspace
cargo metadata --no-deps --format-version 1
git diff --check
```

Run actual benchmarks with `cargo bench --workspace` only when measurement is
required. Benchmark compilation alone is not performance evidence.

## Testing Rules

- Every behavior change needs a direct test at the real public boundary.
- For a bug fix, reproduce the failure before or with the correction when
  practical.
- Concurrency tests prefer markers, barriers, channels, or `Notify`; do not
  rely only on sleeps.
- Test behavior is offline by default; Cargo dependency fetching follows the
  verification contract. Do not consume provider quota without explicit
  opt-in and confirmation.
- Do not test only a helper when the risk is at a Runtime, Store, adapter,
  process, or public API boundary.
- Failure tests must verify classification, source reachability, state
  mutation, and lifecycle effects as applicable.
- Review always includes a proportionate performance check.
- Never describe an unexecuted command as passing.

## Rust and Safety Rules

- Use Rust 2024 and preserve each crate's declared MSRV.
- Unsafe code is forbidden.
- Keep public errors typed and preserve concrete source chains.
- Do not put `anyhow::Error` in framework public APIs.
- Do not log State, messages, Tool arguments/results, raw provider/MCP payloads,
  environment values, credentials, or panic payloads by default.
- Explicit full-source logging and upstream dependency logging require
  application-level filtering.

## Escalation Conditions

Stop and report before proceeding when:

- the task requires a public API change not explicitly authorized;
- a checkpoint format, Codec identity, migration, or durable lineage changes;
- a new high-risk dependency or feature is required;
- requirements have mutually exclusive interpretations;
- a critical validation command cannot run;
- the change may cause data loss, secret exposure, unsafe remote retry, or
  compatibility breakage;
- a requested document move requires product code or doctest changes outside
  scope.

Low-risk implementation details within an accepted Plan are autonomous
decisions. Record meaningful choices in the Decision log.

## Completion Definition

Work is complete only when:

- acceptance criteria are satisfied;
- required commands pass;
- every skip and residual risk is reported;
- authoritative docs and the Execution Plan are synchronized;
- the final changed-file scope and worktree status are reported;
- independent review requirements are met;
- no Git commit is created unless the user explicitly requested one.
