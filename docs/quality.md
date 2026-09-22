# Quality and Release Status

This file is the current quality ledger. It records known debt and release
readiness without turning those items into product stages.

## Current architecture review result

The full repository architecture review found no blocking responsibility
inversion, dependency cycle, durable data-integrity defect, cross-layer
control conflict, or structural Core hot-path regression.

Compatibility-first base APIs:

- Core State, Node, compiled graph, control, event, and error semantics;
- durable Record, Codec, Store, CAS, Resume, Replay, Fork, and branch lineage;
- Model messages, requests, responses, validated facade, collector, and
  extensions;
- Tool trait, behavior, Registry, Runtime, execution reports, observer, and
  ToolMessage helpers.

Experimental surfaces:

- Genai provider-specific configuration and extension keys;
- stable-target policy and provider compatibility mapping;
- MCP transport constructors and discovery configuration;
- any future MCP HTTP or OAuth interface;
- the `group-agent-prebuilt` public API and its current private graph topology.

The repository contains an experimental prebuilt Tool-calling loop over the
stable Core, Model, and Tool boundaries. This capability does not make Prebuilt
stable or make the repository production-ready. Under Plan 028, Prebuilt gained
opt-in durable execution (checkpointed invoke, latest-head Resume, read-only
Replay, writable Fork) with a crate-owned canonical JSON `AgentSnapshotCodec`;
the durable surface is experimental and is not a production-readiness claim.
Under Plan 029, Prebuilt gained opt-in durable Tool approval
(`AgentApprovalRequest`/`AgentApprovalDecision` over Core interrupt resume
values) and Core gained additive read-only `run_config()` accessors on the
Resume/Replay/Fork configurations; both surfaces remain experimental and are
not a production-readiness claim. Under Plan 030, Prebuilt gained streaming
execution (`ToolCallingAgent::stream`, `invoke_with_stream_sink`,
`AgentEventStream`, `AgentEventSink`, `AgentStreamEvent`) with payload-safe
debug formatting and zero detached tasks; this surface remains experimental.
[Plan 030](exec-plans/completed/030-prebuilt-streaming.md) review corrections
preserve supplied Tool observers through the additive `with_additional_event_sink`
API, validate deltas before delivery, and emit truthful Tool terminal events.
Fourteen added regression/composition/control tests passed, as did the complete
`./scripts/verify all` gate and offline streaming example. Independent correction
review returned PASS. Hosted CI and live-provider integration were not run.

[Plan 031](exec-plans/completed/031-durable-streaming-approval.md) adds experimental
checkpointed streaming and streaming Resume, including durable approval handoff
through `AgentStreamEvent::Interrupted`. The explicitly authorized additive Core
`resume_with_state_initializer` attaches transient resources after validated
restore; it does not alter snapshot formats or lineage. Focused public-boundary
tests cover save failures, approval/rejection, invocation isolation, cancellation,
observer preservation, and SQLite reopen with a new Agent. The offline example
passes. `./scripts/verify all` passed after an approved out-of-sandbox rerun
allowed existing loopback HTTP fixtures to bind ports. Stable Rust 1.98.1 and
MSRV Rust 1.88.0 each passed 641 workspace tests including doctests, with zero
failures or ignored tests. Independent review returned PASS with no required
corrections. Hosted CI, live-provider integration, and runtime performance
measurement were not run. This remains experimental and is not a
production-readiness claim.

## Current compiler policy

All eight crates now share Rust 1.88 as the MSRV under
[ADR-012](adr/012-unified-msrv.md). CI runs full quality gates on stable and
whole-workspace compatibility checks/tests on Rust 1.88. The previous Rust
1.85 foundation policy is discontinued. Compiler versions in the historical
release and Plan 026 evidence below describe those earlier checks.

[Plan 027](exec-plans/completed/027-unify-msrv.md) completed the migration.
`./scripts/verify all` passed locally: full gates on Rust 1.98.1 and workspace
MSRV gates on Rust 1.88.0, with 546 tests including doctests passing on each
compiler, zero failures, and zero ignored tests. Independent review returned
PASS. Hosted CI has not yet run for this uncommitted migration. Benchmark
compilation retains a non-fatal host linker deprecation warning; no runtime
performance measurement is claimed.

## Release evidence and blockers

The historical Phase 2 clean candidate
`9b069d430cae02e74134f37edb8d05b83c2cc6c7` passed `./scripts/verify all` and
the eight-archive Phase 2 package audit. All archives were generated without
`--allow-dirty` in dependency order and passed package-list, safe-extraction,
regular-file, normalized-manifest, internal-edge, README, byte-identical
license, intended-content, and filename-only secret-indicator checks. The one
indicator hit, `group-agent-genai/src/error.rs`, contains expected
authentication error identifiers and no credential value. The candidate
remained clean; non-persistent command-line patches were not written to any
manifest.

GitHub Actions run `30848946632` completed successfully for that exact SHA.
Both `Full workspace (Rust 1.88)` and `Layered MSRV (Rust 1.85 and 1.88)`
succeeded. This establishes hosted CI for that bounded historical candidate,
not for a later commit, a release, or a crates.io-resolution result.

The release-facing README changed after that evidence was recorded. Under the
Release Runbook's identity rule, any later final candidate must repeat the full
local matrix, eight-archive clean audit, and both hosted CI jobs for its exact
SHA. Plan 024 is the authoritative record of whether those T3.2 gates passed and
which SHA is the sole proposed `v0.1.0` tag target.

Publication preparation was cancelled by the User on 2026-09-15. Plan 025 was
deleted, not completed, and there is no active publication plan. Earlier
publication authorization is superseded. Any future release requires a new
approved plan and fresh candidate, registry, and consumer verification.

The local annotated `v0.1.0` tag exists and points to candidate
`0cb9b9c334320c6e39881d5b14b7ca2122021d81`. Historical preflight evidence is
retained under `docs/release/`; remote tag, registry, and GitHub Release state
were not rechecked during cancellation.

The 2026-09-15 read-only review identified MCP idempotency-key forwarding,
Genai post-error stream events, unbounded decoded checkpoint retention, and a
strict Clippy failure under Rust 1.98.1. These findings were corrected in
[Plan 026](exec-plans/completed/026-review-corrections.md): MCP rejects required
keys without a protocol mapping, stream errors terminate permanently, decoded
checkpoints are weakly cached with amortized expired-record cleanup, and the
redundant import is removed. Independent review returned PASS. The corrected
worktree passed `./scripts/verify full` on Rust 1.98.1 and
`./scripts/verify msrv` on Rust 1.85/1.88, with 546 passing workspace tests
including doctests and no failures or ignored tests. Benchmark compilation
passed with a non-fatal host linker deprecation warning; no performance
measurement or publication readiness is claimed. See Durable Execution Design
for the cache's live-handle and delayed encoded-record cleanup limits.

Plan 022 completed the metadata, portable license, internal path-plus-version,
SQLite benchmark teardown, production logging guidance, release procedure, and
diagnostic dirty-tree preflight work. Plan 023 records the clean candidate,
hosted CI, full verification, and clean eight-archive evidence for `9b069d4`.
Plan 024 prepares and verifies the later final candidate without relabeling the
Plan 023 evidence. None of these Plans authorizes a tag, GitHub Release,
`cargo publish`, or registry credential use. See the
[Release Runbook](runbooks/release.md) for the exact-candidate rule and the
separately authorized tag, publication/index, and fresh-consumer gates.

## Stage 21 relationship

Stage 21 is completed. It delivered the experimental Prebuilt Agent, offline
example and doctests, and benchmark-build coverage without changing the
reviewed stable Core, Durable, Model, or Tool contracts. Its independent review
was accepted, and its Plan moved to completed plans. This is not a
production-readiness claim.

## Validation expectations

Primary commands:

```bash
./scripts/verify fast
./scripts/verify full
./scripts/verify msrv
```

Test behavior is offline: provider coverage uses local fixtures, loopback HTTP,
duplex transports, or local child processes rather than live quota. Cargo may
still download dependencies fixed by `Cargo.lock` when its local registry or
cache is missing. Set `GROUP_VERIFY_OFFLINE=1` to require cached dependencies
and prohibit Cargo network fallback.

Performance claims require repeatable benchmarks. `cargo bench --workspace
--no-run` is a build gate; actual comparative claims require measured runs and
documented environment details.

## Logging and secrets

Group-owned default formatting is payload-safe, but full source-chain logging
and upstream dependency targets are application-controlled. Production
systems should start from the fail-closed
[Production tracing policy](design/error-cancellation-observability.md#production-tracing-policy).
It disables upstream `genai` and `rmcp` targets until the application has
audited the exact environment and every sink; Group does not promise universal
redaction of upstream events or already-formatted source chains.

## Updating this ledger

Update this document when:

- a release blocker is fixed or discovered;
- a public surface changes stability class;
- a required validation gate changes;
- an architecture review changes the repository-level assessment.

Product capability history belongs in
[Stages 01-20](history/stages-01-20.md), not here.
