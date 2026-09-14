# 027 Unify the workspace MSRV

## Status

Completed on 2026-09-15 following User authorization.

## Goal and compatibility

All eight crates inherit Rust 1.88 from workspace.package.rust-version.
Foundation support for Rust 1.85 through 1.87 is intentionally discontinued.
CI verifies the complete workspace at the minimum version and runs full
quality gates on stable. No runtime behavior, dependency version, record
format, public Rust API, tag, or publication change is in scope.

## Baseline and references

HEAD: `84b99720749805f4a1278244e0ce327e628721f7`; worktree initially clean.
Protected hashes: `/tmp/group-msrv-baseline.json`.
Read AGENTS.md, ARCHITECTURE.md, ADR-011, scripts/verify, CI, current compiler
support documentation, and the completed Plan 026 validation evidence.
Publication remains cancelled; this is repository maintenance, not a Stage.

## Design and slices

- [x] Set workspace rust-version to 1.88 and make both adapters inherit it.
- [x] Replace layered checks with whole-workspace Rust 1.88 check and tests,
      including all features and doctests.
- [x] Keep CI job identifiers full/msrv; full uses stable, msrv installs 1.88.
- [x] Supersede ADR-011 with ADR-012; synchronize current docs and instructions.
      Preserve historical candidate evidence and completed plans.
- [x] Verify all manifests through Cargo metadata, shell/YAML configuration,
      full and MSRV gates, and diff checks.
- [x] Complete independent read-only review and record accepted disposition.

## Verification

Use `CARGO_BUILD_JOBS=2 CC=/usr/bin/gcc ./scripts/verify all` locally.
Check the parsed Cargo metadata for eight rust_version values of 1.88,
run `bash -n scripts/verify`, and inspect the actual CI command routing.
No new behavior unit tests are needed for the configuration and mechanical
syntax migration;
real compiler and existing public-boundary suites validate the contract.
No performance measurement is claimed; the verification matrix is simplified
without changing production hot paths. GitHub-hosted CI is not executed locally.

## Decision log

- 2026-09-15: The User approved one Rust 1.88 floor to reduce maintenance
  complexity; development can use newer stable releases. Do not automatically
  raise MSRV with stable toolchain releases.
- 2026-09-15: Keep historical ADR content and mark it superseded; record the
  new compatibility policy in ADR-012 rather than rewriting historical evidence.

- 2026-09-15: Raising rust-version enables Clippy let-chain suggestions in
  former foundation crates. Apply only the required behavior-preserving syntax
  simplifications and verify existing suites; do not suppress these warnings.

## Review findings

Independent reviewer `/root/msrv_review` returned PASS with no Major or Minor
implementation findings. The primary agent accepts the disposition. The
reviewer checked manifests, CI/script routing, current/historical documentation,
and all nine mechanical let-chain changes; no behavior or structural performance
regression was identified. Independent shell syntax and diff checks passed.
The reviewer did not run Cargo; the primary agent subsequently completed the
entire local verification matrix. During execution four unrelated tracked
`.vscode/` files became deleted: extensions.json, launch.json, settings.json,
and tasks.json. The User confirmed these deletions were intentional after switching to Zed.
They were not made by this task and remain preserved outside the MSRV scope.

## Completion evidence

- `CARGO_BUILD_JOBS=2 CC=/usr/bin/gcc ./scripts/verify all`: PASS.
  Stable Rust 1.98.1 full gates and Rust 1.88.0 workspace MSRV gates passed.
- Workspace tests including doctests: 546 passed, 0 failed, 0 ignored on each
  compiler. Strict Clippy, formatting, all-target/all-feature checking, and
  benchmark compilation passed. No tests or warnings were suppressed.
- Cargo metadata confirms eight packages with rust_version 1.88. Each manifest
  inherits from the workspace. Cargo.lock and dependency requirements unchanged.
- `bash -n scripts/verify`, parsed CI YAML routing, local documentation links,
  and `git diff --check`: PASS. Historical records were not rewritten.
- Benchmark linking retains the host linker's non-fatal deprecated optimization
  setting warning. Actual benchmarks were not run; no speedup is claimed.
- GitHub-hosted CI has not run for these uncommitted changes; local checks do
  not establish a hosted result. Log: `/tmp/group-unified-msrv-verify.log`.
- Final HEAD remains `84b99720749805f4a1278244e0ce327e628721f7`. No commit,
  push, tag, or publication operation. Legacy release state remains blocked.
- Changed scope: workspace and two adapter manifests, scripts/verify, CI,
  four source files with nine mechanical let-chain rewrites, current compiler
  documentation, ADR-012 and ADR index, Plan 027 and indexes, and Harness
  project-state prose/protected hash snapshot. The four User-deleted .vscode
  files remain deleted, outside this task's implementation scope.
- Compatibility change: all consumers now need Rust 1.88 or newer. Runtime
  semantics, public Rust APIs, checkpoint formats, and database behavior remain
  unchanged.
