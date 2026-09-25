# 033 Process termination recovery verification

## Status

Completed

- [x] Baseline recorded.
- [x] Design reviewed against existing public boundaries.
- [x] Slices implemented.
- [x] Verification passed.
- [x] Independent review completed and accepted.
- [x] Completion evidence recorded.

## Goal

Verify an offline Prebuilt streaming Agent can recover SQLite checkpoints in a
fresh process after its predecessor is forcibly terminated at a known boundary.
The user selected this direction on 2026-09-26.

## Non-goals and invariants

No production API, dependency, checkpoint format, codec, migration, graph identity,
or lineage changes. No provider requests, credentials, or package publication.
Git commit/push were outside implementation scope and separately authorized by
the user after completion.
No power-loss, arbitrary transaction interruption, or exactly-once claim. External
side effects between execution and checkpoint commit remain application-owned.

## Context and baseline

- HEAD: `2cd4cc9c696b0b9b7d9b1f5ba888cf7762841250`; clean worktree, empty diff.
- AGENTS.md SHA-256: `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
- Cargo.lock SHA-256: `1b1cf4b431f50bf933585ac3d598a74c750852a8b8c420de61a42057a15321c1`.
- [Architecture](../../../ARCHITECTURE.md), [durability](../../design/durable-execution.md),
  [quality](../../quality.md), and completed Plans 031/032 define existing contracts.
- Existing `durable_streaming_sqlite` tests reopen within one process.

## Roles and permissions

Implementer owns tests and documentation. Independent reviewer is read-only and
checks real process lifecycle, checkpoint boundaries, assertions, and cleanup.
The orchestrator records the review disposition; final product acceptance stays
with the user. Any production defect requiring material changes is escalated.

## Design and slices

### Slice 1: Approval interruption

- [x] Add a self-spawned integration-test worker using the existing offline model.
- [x] Signal readiness only after a persisted Interrupted event; keep SQLite alive.
- [x] Parent kills and reaps that child; fresh child resumes with approve/reject.
- [x] Verify durable completion, transcript, rounds, and file-backed tool count.

### Slice 2: Saved tool result

- [x] Pause the model at its second request, after the tool super-step was saved.
- [x] Kill and reap; fresh process resumes without repeating the saved tool call.
- [x] Bound process waits, clean up on failure, and document a runnable example
      through the focused integration-test command.

## Acceptance and verification

- [x] Three recovery scenarios pass at public Agent/SQLite boundaries.
- [x] Child failure or missing readiness fails rather than hanging indefinitely.
- [x] `cargo test --locked -p group-agent-prebuilt --test process_recovery`.
- [x] `./scripts/verify all` (stable and Rust 1.88).
- [x] `git diff --check`; protected hashes unchanged; independent read-only review.
- [x] Document exact evidence and limits in the durable design and quality ledger.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-09-26 | Reuse the test executable as the child entrypoint | No public binary, dependencies, or production hooks needed. |
| 2026-09-26 | Use explicit readiness markers and bounded polling | Kill only after the intended public boundary; sleeps do not establish correctness. |
| 2026-09-26 | Add coverage before considering runtime changes | No demonstrated production bug; this is a verification gap. |

## Review findings

Independent read-only reviewer `review_033` returned **PASS**. The orchestrator
accepted the technical disposition on 2026-09-26; final product acceptance remains
with the user. One preliminary Minor identified that checking only `is_error`
could miss a damaged saved Tool result because the offline model accepts any Tool
message. The implementation now checks exact `ToolResult::text("SECRET_RESULT")`,
ToolCall identity linkage, original user message, and usage length. The reviewer
independently reran the focused suite after the correction: 4 passed, zero failures
or ignored tests. No remaining Major, Minor, or Suggestions.

Review covered real forced termination/fresh executable construction, durable
boundary markers, cross-process journal evidence, bounded waits and RAII cleanup,
architecture, security, and structural performance. The reviewer did not rerun the
full workspace matrix and explicitly relied on implementer evidence for that gate.

Reviewed test SHA-256:
`c0d7f5e7d77eec6d8e9e85e81dea8e25fbfde2ee01ae0e04b9278078dbc07fce`.
Reviewed fixture SHA-256:
`57ed81134341ced525cc116c74517e387fb1c2d8e49b7497c71ea06d53c8f190`.

## Completion evidence

- Initial focused approval tests: 3 passed (two scenarios and worker entrypoint).
- Expanded focused suite: 4 passed (three scenarios and worker entrypoint).
- Sensitivity experiment: temporarily selecting `FinalOnly` instead of
  `EverySuperstep` caused the saved-result test to fail before readiness because
  no checkpoint existed. The original policy was restored. This is a deliberate
  negative test, not evidence of a production bug. Log:
  `/tmp/group-033-negative.log`.
- Final `GROUP_VERIFY_OFFLINE=1 ./scripts/verify all`: passed. Stable Rust 1.98.1
  and MSRV Rust 1.88.0 each passed 664 workspace tests including doctests, zero
  failures and zero ignored tests. Strict Clippy, formatting, all-target and
  all-feature checks, and benchmark compilation passed. Log:
  `/tmp/group-033-verify-all.log`.
- `git diff --check`: passed. Protected AGENTS.md and Cargo.lock hashes unchanged.
- Documentation now includes the runnable test example and exact boundary limits
  in Durable Execution Design, and current evidence in the quality ledger.
- Production performance is unchanged: only test processes, SQLite fixtures, and
  small synchronous journal writes were added. Benchmark compilation is not a
  measured performance result.

At implementation completion, HEAD was
`2cd4cc9c696b0b9b7d9b1f5ba888cf7762841250`. Changed scope:
two new Prebuilt test/support files; durable design, quality, active/completed Plan
indexes, and this Plan. Changes were uncommitted at that checkpoint. The user
subsequently authorized committing and pushing this accepted scope on 2026-09-26.

Skipped/outside scope: live providers, hosted CI, power failure, termination
inside a transaction, and performance measurement. Recovery before the external
Tool effect has been checkpointed remains an application-level ambiguity; these
tests do not close that window or claim exactly-once execution.
