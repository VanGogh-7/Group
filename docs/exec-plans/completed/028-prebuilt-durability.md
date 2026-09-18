# 028 Prebuilt durable execution integration

## Status

Completed

- [x] Baseline recorded.
- [x] Design reviewed.
- [x] Slices implemented.
- [x] Verification passed.
- [x] Independent review completed.
- [x] Review accepted and written back by a write-authorized role.
- [x] Completion evidence recorded.

## Goal

`ToolCallingAgent` in `group-agent-prebuilt` gains opt-in durable execution
over the existing stable Core durable ports: checkpointed invoke, latest-head
resume, exact read-only replay, and writable fork, with an application-usable
snapshot codec and direct offline behavior tests for every operation.

## Non-goals

- Interruptible Nodes, human approval, and resume values.
- Streaming orchestration, structured output, middleware, Multi-Agent.
- Branch merge or branch deletion.
- Any behavior change to Core, Durable, Tool, or the SQLite adapter.
- Built-in codecs in Core; v0.1.0 publication work.

## Context

- Starting HEAD: `3b209b66ec8db9d9ee46db6c64fdf27bf9c47cec` (`master`), clean
  worktree, in sync with `origin/master`.
- [ARCHITECTURE.md](../../../ARCHITECTURE.md),
  [Durable Execution Design](../../design/durable-execution.md),
  [Model and Tools Design](../../design/model-and-tools.md),
  [ADR-004](../../adr/004-storage-neutral-checkpoints.md),
  [ADR-010](../../adr/010-stable-base-experimental-adapters.md).
- Core durable entry points live in
  `impl<S: CheckpointState> CompiledGraph<S>`
  (`crates/group-agent-core/src/runtime.rs`); `invoke_with_checkpoint`,
  `resume`, `replay`, `fork` all require a graph version and a
  `CheckpointCodec<S::Snapshot>`.
- The Prebuilt private graph currently has no `GraphVersion`, and
  `group-agent-model` types have no serialization surface.
- Prior related Plans: 021 (Prebuilt Agent), 026 (review corrections), 027
  (unified MSRV).

## Roles and permissions

- **User / Product Owner:** approved the goal, the serde-feature codec
  strategy on `group-agent-model`, and the four-operation scope on
  2026-09-18. Owns final acceptance.
- **Mentor / Orchestrator:** reviewed the technical direction and slice
  boundaries before implementation.
- **Codex A / Implementer:** implements the slices below, maintains this Plan,
  runs verification, self-reviews.
- **Codex B / Independent Reviewer:** strictly read-only; emits a standalone
  report per [review.md](../../runbooks/review.md).

## Invariants

- Core, Durable, Model, Tool stable contracts evolve compatibility-first;
  the Model serde surface is additive and feature-gated.
- `AgentState` and the private graph topology stay private (the existing
  `compile_fail` doctest must stay green).
- Default error and event formatting remains payload-free; new public types
  redact message content in `Debug`/`Display`.
- No hidden retry, no new run lock, no per-Node task spawning; durable write
  conflicts fail fast through the existing CAS path.
- Codec output is deterministic canonical bytes for equal logical snapshots;
  `CodecDescriptor` and `GraphVersion` are durable compatibility identities.
- All tests remain offline (in-memory checkpointer, scripted models, local
  SQLite); MSRV stays Rust 1.88; unsafe code stays forbidden.

## Proposed design

1. `group-agent-model` gains an optional `serde` dependency (workspace serde
   plus its `rc` feature for `Arc<str>` id newtypes) behind a `serde` crate
   feature, with derive-gated `Serialize`/`Deserialize` on the snapshot-
   reachable types only: `Message` family and `Role`, `ContentPart`,
   `ToolCall`/`ToolResult` and id newtypes, `Extensions`, `TokenUsage`.
2. `group-agent-prebuilt` gains a public opaque `AgentSnapshot`
   (private fields, redacted `Debug`), an
   `impl CheckpointState for AgentState`, and a `GraphVersion` constant on the
   constructor-compiled graph.
3. `group-agent-prebuilt` gains `AgentSnapshotCodec`, a
   `CheckpointCodec<AgentSnapshot>` over canonical JSON
   (`CodecDescriptor::new("group-agent-prebuilt-agent-state", 1, "json")`);
   interrupt encode/decode keep the default reject.
4. `ToolCallingAgent` gains `invoke_with_checkpoint` (+ control variant),
   `resume`, `replay`, `fork`, reusing Core config types generic over
   `AgentSnapshot`. Results are eagerly converted into new opaque wrappers
   (`AgentRunOutcome`, `AgentInterrupted`, `AgentReplayReport`,
   `AgentForkReport`) so `AgentState` never appears in the public API.

## Implementation slices

### Slice 1: model serde surface

- [x] Implementation
- [x] Direct behavior tests (per-type serde roundtrips)
- [x] Slice verification (`./scripts/verify fast`)
- [x] Slice self-review

### Slice 2: snapshot, CheckpointState, graph version

- [x] Implementation
- [x] Direct behavior tests (snapshot/restore roundtrip after model and tool
  super-steps)
- [x] Slice verification
- [x] Slice self-review

### Slice 3: codec and public durable API

- [x] Implementation (`AgentSnapshotCodec`, four durable methods, wrapper
  outcome/report types)
- [x] Direct behavior tests (codec determinism, roundtrip, garbage decode,
  descriptor stability)
- [x] Slice verification
- [x] Slice self-review

### Slice 4: behavior and integration tests, example

- [x] In-memory behavior tests: durable invoke events, failure-then-resume,
  replay, fork, unknown-thread error classification
- [x] SQLite restart-style integration test (dev-dependency)
- [x] Offline `durable_agent` example
- [x] Slice verification

### Slice 5: documentation and closure

- [x] README, ARCHITECTURE, quality ledger, crate docs synchronized
- [x] `./scripts/verify full` and `./scripts/verify msrv`
- [ ] Independent review and writeback

## Acceptance criteria

- [ ] All four durable operations work end-to-end offline through the public
  `ToolCallingAgent` API with in-memory and SQLite checkpointers.
- [ ] `AgentState` stays private; payload redaction rules hold on every new
  public type.
- [ ] Resume of an unknown thread and decode of corrupt bytes surface typed,
  source-reachable errors.
- [ ] README, ARCHITECTURE, and the quality ledger no longer list Prebuilt
  durability as absent; no publication readiness is implied.

## Verification

```text
./scripts/verify fast      # per slice
./scripts/verify full      # before review
./scripts/verify msrv      # before review
```

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-09-18 | Codec built on a feature-gated serde surface in `group-agent-model` instead of a hand-rolled mapping in Prebuilt | User-authorized; less code, no drift risk against `#[non_exhaustive]` model types; additive and compatibility-first |
| 2026-09-18 | Scope is invoke_with_checkpoint + resume + replay + fork; interrupts/approval excluded | User-authorized; interrupt support requires InterruptibleNode redesign and belongs to a later approval slice |
| 2026-09-18 | Both review Minor findings corrected immediately instead of deferred to debt | User-authorized disposition; typed-error wildcard arm and validated decode keep the experimental surface fail-closed |
| 2026-09-18 | Codec descriptor `("group-agent-prebuilt-agent-state", 1, "json")` and a namespaced Prebuilt `GraphVersion` constant | Both are durable compatibility identities once data exists; namespacing avoids collisions with application codecs |

## Review findings

Independent review (Codex B, strictly read-only, 2026-09-18): **PASS WITH
MINOR FIXES**. The reviewer independently ran `git status --short` /
`git diff --stat`, model and prebuilt test suites, the durable integration
suites, and `./scripts/verify fast`; it verified dependency direction,
`AgentState` privacy, redaction, codec canonicality, failure paths, and
documentation accuracy from actual code. `verify full` and `verify msrv`
were not independently rerun by the reviewer (implementer evidence accepted
for those gates).

Findings and dispositions (both accepted by the User / Product Owner on
2026-09-18 and corrected immediately):

1. Minor — `AgentRunOutcome::from_execution` routed a wildcard
   `ExecutionOutcome` arm through `as_interrupted().expect(...)`, a latent
   panic if Core ever adds a third variant
   (`crates/group-agent-prebuilt/src/outcome.rs`). Corrected: `Completed` and
   `Interrupted` are matched explicitly; the wildcard now returns a typed
   `AgentError` backed by a new private `UnknownExecutionOutcome` source
   (`AgentError` internals became a private `AgentErrorSource` enum; public
   shape, payload-free Display/Debug, and existing error-chain doctests
   unchanged).
2. Minor — derived `Deserialize` bypassed validating constructors, so
   parseable-but-invalid checkpoint bytes (empty `ToolCallId`, inconsistent
   `TokenUsage`) decoded silently. Corrected in `group-agent-model` behind
   the `serde` feature: manual validated `Deserialize` for the
   `tool_string_id!` newtypes, `TokenUsage`, and `Extensions`; remaining
   derived types audited and confirmed to have no validating constructors.
   New direct tests reject parseable-but-invalid bytes at both the model
   boundary and the `AgentSnapshotCodec` boundary; the codec rustdoc now
   documents that decode enforces Model invariants. The canonical byte
   format for valid values is unchanged.

Post-correction gates (all with `CC=/usr/bin/gcc`): `./scripts/verify fast`,
`./scripts/verify full`, and `./scripts/verify msrv` all passed again. No
residual findings; no correction Plan or product Stage required.

## Completion evidence

Implementation-phase evidence (independent review and closure pending):

- Worktree: uncommitted changes over baseline
  `3b209b66ec8db9d9ee46db6c64fdf27bf9c47cec`; no commits created.
- `group-agent-model`: optional workspace `serde` (with `rc`) behind crate
  feature `serde`; derives on `Role`, the four message structs, `Message`,
  `ContentPart`, `ToolName`, `ToolCallId`, `ToolCall`, `ToolResult`,
  `Extensions`, `TokenUsage`; feature-gated roundtrip tests in
  `tests/serde_roundtrip.rs`.
- `group-agent-prebuilt`: public `AgentSnapshot` (opaque, redacted `Debug`),
  `impl CheckpointState for AgentState`, durable `GraphVersion`
  `group-agent-prebuilt/tool-calling-agent/1`; `AgentSnapshotCodec`
  (descriptor `("group-agent-prebuilt-agent-state", 1, "json")`); durable
  methods `invoke_with_checkpoint`, `invoke_with_checkpoint_control`,
  `resume`, `replay`, `fork`; wrappers `AgentRunOutcome`, `AgentInterrupted`,
  `AgentReplayReport`, `AgentForkReport`.
- Tests: codec determinism/roundtrip/garbage/descriptor/interrupt-reject;
  snapshot roundtrip and redaction; in-memory durable behavior suite
  (`tests/durable.rs`, 5 tests); SQLite restart test
  (`tests/durable_sqlite.rs`); offline `examples/durable_agent.rs`.
- Docs synchronized: README.md, ARCHITECTURE.md, docs/quality.md,
  docs/design/model-and-tools.md, crate docs.
- Gates actually run (all with `CC=/usr/bin/gcc`; a pre-existing
  clang-shim in `~/.local/bin` breaks `aws-lc-sys`/`libsqlite3-sys`
  compilation, reproduced on the pristine baseline):
  - `./scripts/verify fast` — passed after every slice.
  - `./scripts/verify full` — exit 0 (fmt, strict clippy `-D warnings`,
    workspace tests, doctests including Prebuilt compile-fail privacy
    doctests, benchmark compilation with only the known non-fatal linker
    deprecation warning).
  - `./scripts/verify msrv` — exit 0 on Rust 1.88.0.
  - Workspace totals: 574 passed, 0 failed, 0 ignored on stable and on
    1.88.0.
- Review corrections applied (see Review findings); all gates rerun green
  afterwards (fast, full, msrv; 574+ workspace tests, 0 failures, 0
  ignored).
- Final worktree: uncommitted changes only, on top of baseline
  `3b209b66ec8db9d9ee46db6c64fdf27bf9c47cec`; no Git commit was created.
- Remaining risk: `AgentRunOutcome::Interrupted` is unreachable until a
  future interruptible-node slice; resume/replay/fork use Core's default
  per-call `RunConfig` (1000 additional steps) unless the caller sets
  `with_run_config`, because Core configs expose no run-config getter.
  The `AgentSnapshotCodec` decode enforces Model invariants, but durable
  stores remain application-trusted data protected by the codec descriptor
  and graph-version identities.
