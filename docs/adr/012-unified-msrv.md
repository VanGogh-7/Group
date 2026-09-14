# ADR-012: All workspace crates share Rust 1.88 as their MSRV

## Status

Accepted on 2026-09-15. Supersedes [ADR-011](011-layered-msrv.md).

## Context

The workspace previously supported foundation crates on Rust 1.85 and Genai
and MCP adapters on Rust 1.88. That preserved an older consumer floor but
required separate compiler policies, manifest declarations, and verification
branches. The User chose a common floor while publication remains cancelled.

## Decision

- Set `[workspace.package].rust-version = "1.88"`.
- Make all eight crates inherit `rust-version.workspace = true`.
- Verify all targets and features and run workspace tests including doctests
  on Rust 1.88 through `./scripts/verify msrv`.
- Run `./scripts/verify full` with stable Rust in a separate CI job.
- Keep Rust 2024, existing dependency versions, and runtime behavior unchanged.
- Raise MSRV only through an explicit compatibility decision when project or
  dependency requirements justify it, not on every stable toolchain update.

## Consequences

Foundation consumers must now use Rust 1.88 or newer; support for Rust 1.85
through 1.87 is intentionally discontinued. The adapter compiler floor does
not change. CI retains two purposes (minimum compatibility and current-toolchain
quality) while eliminating the crate-specific MSRV matrix.

Historical candidate records remain historical and retain their original
compiler versions. No publication, tag movement, or registry operation follows
from this decision.

## Alternatives

- Retain the layered policy if concrete older-toolchain consumers require it.
- Raise all crates to the newest development compiler, which is unnecessary
  for the current code and dependencies.
- Lower adapters to Rust 1.85 by patching upstream dependencies, which would
  add maintenance outside this task.
