# 030 Prebuilt agent streaming output and token events

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

Provide experimental streaming output from `ToolCallingAgent` through an async
`AgentEventStream` or synchronous `AgentEventSink`, including validated Model
deltas, Tool lifecycle events, and final outcomes. Preserve the existing
non-streaming paths, caller-supplied observers, typed error chains, and owned
future cancellation without detached tasks.

## Non-goals

No Core, Model, durable format, provider, MCP, or dependency-version changes.
No durable streaming/resumable streaming approval, retries, rollback,
exactly-once execution, product policy, or publication. The only stable API
addition is the explicitly approved Tool observer composition method.

## Context and baseline

Initial HEAD: `abb6a35b19741f7d2fe6d86c21cb46127f29003c`.
The initial streaming implementation followed Plans 021, 028, and 029.
Independent review reopened this Plan with one Major and three Minor findings.
The user then authorized correction and explicitly approved
`ToolRuntime::with_additional_event_sink`.

Correction baseline: 11 modified tracked files and 4 untracked files, all
preserved. A complete pre-correction file snapshot was retained locally at
`/tmp/group-030-before-fix-90_oywdi`.

Protected correction-baseline hashes, unchanged at completion:

- Cargo.lock: `1cf17e56d6f0860e7a4b5e4d487d0ff1381f8b3dc8ed7709b4bbb35901d369f9`
- AGENTS.md: `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`

Authoritative contracts:

- [Architecture](../../../ARCHITECTURE.md)
- [Model and Tools](../../design/model-and-tools.md)
- [Error, Cancellation, and Observability](../../design/error-cancellation-observability.md)
- [Quality Ledger](../../quality.md)

## Roles and permissions

The user owns product acceptance, authorized implementation/correction, and
approved the additive Tool API. The primary agent implemented production
corrections and documentation. Separate test agents wrote the public regression
and observer-composition tests. An independent read-only reviewer
(`correction_review`) reviewed the final code and evidence. The primary agent
accepted that PASS disposition and performs this authorized writeback.
No Git commit or publication was authorized or performed.

## Invariants and design

- Core, Model, Tool, and Prebuilt retain their dependency boundaries. Runtime
  remains the sole State update owner. There are no detached tasks or retries.
- Both streaming interfaces call `ChatModel::stream`. Every event passes
  `ChatStreamCollector` before its corresponding Agent delta is published.
  Concrete protocol and unsupported-capability sources remain reachable.
- `AgentEventStream` owns and polls its invocation directly, draining events
  from an unbounded channel with cooperative per-delta yielding. It terminates
  after completion or a single error and drops owned work on destruction.
- The per-invocation sink remains private in AgentState and is excluded from
  snapshots. No codec identity, checkpoint format, or durable lineage changes.
- `with_additional_event_sink` appends observers in installation order. Start
  errors/panics stop further delivery and prevent execution. Terminal events
  reach all observers; the first failure remains the existing secondary
  diagnostic. Each callback panic is caught independently by ToolRuntime.
  `with_event_sink` still replaces all observers; composing a clone is isolated.
- Prebuilt appends its observer instead of replacing the supplied observer.
  `ToolCompleted { is_error: true }` includes started Tool failures; the typed
  AgentError retains infrastructure classification and the batch report.
  Validation failures and start-observer rejection fabricate no execution
  lifecycle. Dropped/cancelled pending work need not emit Tool completion.
- Default event formatting redacts text and arguments. Typed event access is
  explicit payload access; full source logging remains application-owned.
- Streaming entrypoints are non-durable. ApprovalRequired on these paths is
  followed by the typed interrupt-requires-checkpoint failure; use the separate
  durable APIs for resumable Tool approval.
- Rust 1.88 is preserved. Tests and examples are offline; unsafe is forbidden.

## Implementation slices and acceptance

- [x] Stream event types, safe formatting, sink, and fused stream handle.
- [x] Model streaming, validated deltas, usage collection, cooperative yielding.
- [x] Tool lifecycle integration and caller-observer preservation.
- [x] Public streaming methods and offline example.
- [x] Direct regressions reproduce the reported defects before production fixes.
- [x] Ordered Tool observer composition preserves start and terminal semantics.
- [x] Rejected protocol deltas never reach either Agent streaming interface.
- [x] Started Tool failures emit a terminal event before the typed Agent error.
- [x] Both interfaces cover pending Model and Tool cancellation, run timeout,
  Node timeout, and immediate future drop using markers and paused time.
- [x] Concrete error sources, no later model round, and cleanup are asserted.
- [x] Original invocation/durable tests and full stable/MSRV gates pass.
- [x] Authoritative documentation reflects actual capabilities and limitations.
- [x] Independent review returns PASS; findings are resolved and written back.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-09-20 | Stream plus synchronous sink interfaces | Support async consumption and inline observation without background execution. |
| 2026-09-20 | Reopen initial completion after review | Passing existing gates did not cover three reproducible behavior defects. |
| 2026-09-20 | Add user-approved Tool observer composition | Prebuilt cannot preserve a private observer through the replacement-only API. Keep panic/error policy in ToolRuntime. |
| 2026-09-20 | Validate before dispatch | Consumers must not receive deltas rejected by the collector. |
| 2026-09-20 | Map only actual Tool outcomes | Pre-execution rejection and dropped work must not be reported as completed execution. |
| 2026-09-20 | Borrow runtime on non-streaming path | Avoid the initial implementation's unnecessary per-batch runtime clone. |

## Review findings and disposition

The initial implementation's PASS writeback was superseded by independent
REQUIRES FIX review. All four findings were corrected:

1. **Major:** Streaming replaced the caller's observer and bypassed start
   rejection. Ordered composition now preserves delivery, failure, panic, and
   terminal diagnostic semantics. Covered through Tool and both Agent APIs.
2. **Minor:** Deltas were dispatched before validation. Rejected post-Finished,
   duplicate-field, and excessive-index deltas now produce typed errors without
   publication or completion events.
3. **Minor:** Tool execution failure omitted its terminal Agent event. Started
   failure emits ToolCompleted(true), retains call identity and batch report,
   and terminates with AgentError. Unknown/unstarted calls emit no lifecycle.
4. **Minor:** Completion evidence claimed untested control paths. Four direct
   test matrices now exercise both interfaces and pending Model/Tool work,
   asserting typed sources and immediate cleanup.

Independent correction review: **PASS**, no remaining Major or Minor findings.
The reviewer independently ran the 6 Prebuilt regression tests, 4 streaming
control tests, and 4 Tool composition tests, plus the offline example and
`git diff --check`. The reviewer inspected the implementer's completed full
verification log; it did not independently rerun the entire full/MSRV gate.

## Verification and completion evidence

Actual commands and outcomes:

- Before correction, `cargo test -p group-agent-prebuilt --test streaming_regressions`:
  five defect regressions failed as expected; a later unstarted-call case passed.
- After correction, `cargo test --locked -p group-agent-prebuilt --test streaming_regressions --test streaming_control`:
  10 tests passed, including both interfaces and all four control matrices.
- `cargo test -p group-agent-tool --test observer_composition`: 4 tests passed.
- `./scripts/verify all`: passed, including strict Clippy, workspace
  tests/doctests, benchmark compilation, all targets/features, and Rust 1.88
  checks/tests. The first attempt caught a redundant match guard; it was fixed
  and the full gate rerun successfully.
- `cargo run --locked --offline -p group-agent-prebuilt --example streaming_agent`:
  passed.
- Original external regression probes: all three passed after correction.
- Final documentation-only writeback: `git diff --check` and local Markdown
  link validation passed.

Local evidence logs (ephemeral, not repository dependencies):

- `/tmp/group-030-correction-tests.log`
- `/tmp/group-030-fix-verify.log`
- `/tmp/group-030-fixed-probes.log`
- `/tmp/group-030-fixed-example.log`

Final HEAD is unchanged. The worktree remains uncommitted: 14 modified tracked
files and 7 untracked files, combining the original implementation and these
corrections. Production corrections affect Prebuilt streaming and Tool observer
composition; tests and the authoritative documentation account for the rest.
No Core/Model/provider source, manifest, lockfile, or durable artifact was
changed by the correction pass.

Performance inspection found linear observer dispatch and an empty,
allocation-free additional-observer vector for ordinary runtimes. Streaming
composition allocates locally per Tool batch; the non-streaming path borrows
its runtime. No comparative runtime benchmark or live-provider test was run;
benchmark compilation is not performance evidence. Hosted CI was not run.
Local cancellation drops owned work but cannot prove remote rollback.
