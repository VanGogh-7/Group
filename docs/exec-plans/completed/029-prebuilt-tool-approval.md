# 029 Prebuilt tool approval via durable interrupts

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

The experimental `ToolCallingAgent` gains opt-in human-in-the-loop tool
approval: an approval-enabled agent durably suspends before executing any
tool side effect, persists the pending calls as an interrupt payload, and
later resumes with an approve/reject decision. Includes additive read-only
`run_config()` getters on Core's `ResumeConfig`/`ReplayConfig`/`ForkConfig`
so Prebuilt resume/replay/fork inherit the agent's stored step budget.

## Non-goals

- Per-call (granular) approval decisions; this slice approves or rejects the
  whole pending batch.
- Streaming orchestration, structured output, middleware, Multi-Agent.
- Any behavior change to Core execution, Durable, Tool, or SQLite semantics
  (Core receives additive accessors only).
- Changes to non-approval agent behavior; publication work.

## Context

- Starting HEAD: `07eff14` (`master`, pushed; Plan 028 durable execution).
  Clean worktree.
- [Plan 028](../completed/028-prebuilt-durability.md) delivered the durable
  base this plan extends; its leftover items (unreachable
  `AgentRunOutcome::Interrupted`, codec interrupt default-reject, resume
  default run-config limitation) are resolved here.
- Core interrupt mechanics verified from source: `InterruptibleNode`
  (`crates/group-agent-core/src/node.rs`), `NodeOutcome::interrupt` with
  type-erased `InterruptPayload`, mandatory interrupt checkpoints of the
  pre-node committed state, positional single-attempt resume values
  (`NodeContext::require_resume_value::<T>()`), codec interrupt
  encode/decode sharing the snapshot descriptor's `encoding` identity, and
  the `InterruptRequiresCheckpoint` / `MissingResumeValue` /
  `UnexpectedResumeValue` / `ReplayInterruptUnsupported` failure taxonomy.
- [ARCHITECTURE.md](../../../ARCHITECTURE.md),
  [Durable Execution Design](../../design/durable-execution.md),
  [Model and Tools Design](../../design/model-and-tools.md),
  [ADR-008](../../adr/008-conservative-remote-tool-behavior.md).

## Roles and permissions

- **User / Product Owner:** approved the goal, the Core getter addition, and
  the reject-continues-the-loop semantics on 2026-09-18. Owns final
  acceptance.
- **Mentor / Orchestrator:** reviewed technical direction and slice
  boundaries.
- **Codex A / Implementer:** implements, tests, verifies, maintains this
  Plan.
- **Codex B / Independent Reviewer:** strictly read-only; standalone report
  per [review.md](../../runbooks/review.md).

## Invariants

- Core changes are additive, compatibility-first accessors only.
- Non-approval agents remain behavior-identical; every Plan 028 test passes
  unmodified.
- `AgentState` and the private graph topology stay private (compile_fail
  doctests stay green); all new public types redact payloads in
  `Debug`/`Display`.
- No approval is ever silently skipped: approval-enabled agents on the
  non-durable invoke paths fail closed with `InterruptRequiresCheckpoint`.
- Interrupt payload codec descriptor shares the snapshot codec's `json`
  encoding identity; unknown payload types reject with
  `UnsupportedInterruptPayload`.
- Approval and base graphs use distinct `GraphVersion` identities; both are
  durable compatibility identities pinned by tests.
- Offline tests only; MSRV Rust 1.88; unsafe forbidden; no hidden retry.

## Proposed design

1. Core: `pub const fn run_config(&self) -> &RunConfig` on `ResumeConfig`,
   `ReplayConfig`, `ForkConfig`. Prebuilt `resume`/`replay`/`fork` inject
   the agent's stored `RunConfig` (`2 * max_rounds`) when the caller left
   the Core default in place, and respect a caller-set value otherwise.
2. Prebuilt approval types: `AgentApprovalRequest` (interrupt payload:
   pending `Vec<ToolCall>`; Clone, serde with validated decode, redacted
   `Debug`) and `AgentApprovalDecision` (`Approve`/`Reject`,
   `#[non_exhaustive]`, in-memory resume value, never serialized).
   Reject converts every pending call into a business-error `ToolMessage`
   and the loop continues; Approve executes the batch as today.
3. `AgentConfig` gains a private `tool_approval: bool` (default false) with
   a const builder and getter. `compile_agent_graph` registers `tools` via
   `add_interruptible_node` when enabled (same `ToolNode` implements both
   `Node` and `InterruptibleNode`), otherwise unchanged.
4. Approval graphs compile with `GraphVersion`
   `group-agent-prebuilt/tool-calling-agent/approval/1`; base graphs keep
   `group-agent-prebuilt/tool-calling-agent/1`.
5. `AgentSnapshotCodec` implements `encode_interrupt`/`decode_interrupt`
   for `AgentApprovalRequest` with descriptor
   `("group-agent-prebuilt-tool-approval", 1, "json")`; unknown payload
   types keep rejecting.
6. `AgentInterrupted` stores the downcast request and exposes
   `approval_request() -> Option<&AgentApprovalRequest>`.

## Implementation slices

### Slice 1: Core run_config getters + Prebuilt budget inheritance

- [x] Implementation
- [x] Direct behavior tests
- [x] Slice verification
- [x] Slice self-review

### Slice 2: approval types + codec interrupt methods

- [x] Implementation
- [x] Direct behavior tests
- [x] Slice verification
- [x] Slice self-review

### Slice 3: interruptible ToolNode + config flag + version split

- [x] Implementation
- [x] Direct behavior tests (both graph versions pinned)
- [x] Slice verification
- [x] Slice self-review

### Slice 4: behavior and integration tests, example

- [x] In-memory approval suite (interrupt, approve-once, reject-continues,
  missing/unexpected/wrong-type resume values, non-durable invoke failure,
  replay and fork of interrupted checkpoints)
- [x] SQLite restart-style approval resume
- [x] Offline `durable_approval` example
- [x] Slice verification

### Slice 5: documentation and closure

- [x] README, ARCHITECTURE, quality ledger, design docs, crate docs
- [x] `./scripts/verify full` and `./scripts/verify msrv`
- [ ] Independent review and writeback

## Acceptance criteria

- [ ] Approval-enabled agent durably suspends before any tool side effect
  and resumes to completion under both decisions, in-memory and SQLite,
  fully offline.
- [ ] Every failure mode above surfaces a typed, source-reachable error;
  events and Debug stay payload-free.
- [ ] Non-approval agents are behavior-identical (Plan 028 suite unmodified
  and green).
- [ ] Docs synchronized; no publication-readiness implication.

## Verification

```text
./scripts/verify fast      # per slice
./scripts/verify full      # before review
./scripts/verify msrv      # before review
```

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-09-18 | Batch-level approval with reject-continues-the-loop | User-approved scope; per-call granularity adds resume-value alignment complexity for little gain; reject-as-business-error matches the existing continue-after-business-error contract |
| 2026-09-18 | Approval is construction-time via `AgentConfig::with_tool_approval`, with a split `GraphVersion` (`.../approval/1`) | Node kind is baked into the compiled graph; the version split makes cross-configuration resume fail closed at the documented identity boundary |
| 2026-09-18 | Additive `run_config()` getters in stable Core, authorized by the User | Fixes the Plan 028 resume-budget limitation without a Core behavior change; matches existing accessor style |
| 2026-09-18 | Approval-enabled agents fail closed (`InterruptRequiresCheckpoint`) on non-durable invoke paths | Skipping approval silently would violate the security posture of the feature |

## Review findings

Independent review (Codex B, strictly read-only, 2026-09-18): **PASS** — no
Major and no Minor findings. The reviewer independently ran the Core and
Prebuilt test suites, the approval and SQLite integration suites, the
offline example, and `./scripts/verify fast`; it verified from actual code
that the Core diff is exactly additive (40 lines, zero deletions), the
`ToolNode` refactor is semantics-preserving for non-approval agents,
approval cannot be silently skipped, redaction holds, the codec fails
closed on unknown payloads and invalid bytes, the failure taxonomy is
typed and source-reachable, and the exhaustive-match deviation is sound
(compile-time guard, strictly stronger than a runtime typed error).
`verify full` and `verify msrv` were not independently rerun (implementer
evidence accepted; no dependency or lockfile changes in the diff).

Suggestions and dispositions (User / Product Owner decision, 2026-09-18):

1. Cross-configuration resume lacked a direct test. Disposition: corrected
   before closure — `tests/durable_approval.rs` gained
   `base_agent_resume_of_an_approval_interrupted_thread_fails_closed` and
   `approval_agent_resume_of_a_base_completed_thread_fails_closed`, both
   asserting `GraphRunError::CheckpointIncompatible` with
   `CheckpointIncompatibility::GraphVersionMismatch` and an unchanged
   thread head. `verify fast` and strict clippy rerun green.
2. The budget-inheritance edge (an explicit caller-set `RunConfig::default()`
   is treated as unset) stands as documented in the method rustdoc and this
   Plan's decision log; accepted without correction.

## Completion evidence

Implementation-phase evidence (independent review and closure pending):

- Worktree: uncommitted changes over baseline `07eff14`; no commits created.
- Core (additive only): `run_config()` getters on `ResumeConfig`,
  `ReplayConfig`, `ForkConfig` + getter unit test.
- Prebuilt: `AgentConfig::with_tool_approval` + getter; `AgentApprovalRequest`
  (serde, validated, redacted Debug) and `AgentApprovalDecision`;
  `AgentSnapshotCodec` interrupt encode/decode (descriptor
  `("group-agent-prebuilt-tool-approval", 1, "json")`, shared `json`
  encoding identity); interruptible `ToolNode` (plain `Node` behavior
  unchanged; Reject commits business-error ToolMessages
  "tool call rejected by approval" and continues); approval graph version
  `group-agent-prebuilt/tool-calling-agent/approval/1`;
  `AgentInterrupted::approval_request()`; resume/replay/fork inherit the
  agent's stored step budget when the caller leaves the Core default.
- Tests: Core getter test; approval codec suite (roundtrip, identity,
  fail-closed unknown payloads, descriptor/garbage/invariant rejection);
  version pinning for both graph flavors; `tests/durable_approval.rs`
  (9 tests: interrupt-before-side-effects with event assertions,
  approve-once, reject-continues, missing/unexpected/wrong-type resume
  values with single-attempt confirmation, fail-closed non-durable invoke,
  read-only replay of an interrupted checkpoint, fork with decision);
  SQLite restart approval resume; offline `durable_approval` example.
  All Plan 028 tests unmodified and green.
- Docs synchronized: prebuilt crate docs, README.md, ARCHITECTURE.md,
  docs/quality.md, docs/design/model-and-tools.md.
- Gates actually run (all `CC=/usr/bin/gcc`; pre-existing clang-shim issue
  documented in Plan 028): `verify fast` per slice; `verify full` exit 0;
  `verify msrv` exit 0 on Rust 1.88.0. Workspace totals: 600 passed, 0
  failed, 0 ignored on stable and on 1.88.0.
- Design deviation recorded: the future-decision-variant guard is an
  exhaustive match (compile-time failure) rather than a wildcard typed
  error, because in-crate `#[non_exhaustive]` matching makes a wildcard
  arm an unreachable-pattern warning under `-D warnings`; this is strictly
  stronger and can never silently approve.
- Accepted suggestion implemented: cross-configuration resume now has two
  direct fail-closed tests (GraphVersionMismatch both directions); all
  gates rerun green (11 durable_approval tests total).
- Final worktree: uncommitted changes only, on top of baseline `07eff14`;
  no Git commit was created.
- Remaining risk: per-call (granular) approval is out of scope; approval
  payloads persist ToolCall arguments in checkpoints exactly as the
  transcript itself already does (application-trusted durable store); the
  documented budget-inheritance edge stands as accepted.
