# <ID> <Task name>

## Status

Proposed

- [ ] Baseline recorded.
- [ ] Design reviewed.
- [ ] Slices implemented.
- [ ] Verification passed.
- [ ] Independent review completed.
- [ ] Review accepted and written back by a write-authorized role.
- [ ] Completion evidence recorded.

## Goal

State the measurable outcome.

## Non-goals

List adjacent work that is explicitly outside this Plan.

## Context

Link current architecture, design, ADR, quality, code, tests, and prior Plans
that the task depends on. Record the starting HEAD and worktree state.

## Roles and permissions

- **User / Product Owner:** defines goals and product trade-offs, authorizes
  material changes such as public API, persistence format, or high-risk
  dependencies, and owns final acceptance without prescribing every
  implementation detail.
- **Mentor / Orchestrator:** checks technical direction, task and review
  boundaries, and correction needs; prepares or reviews Codex instructions
  without replacing the User's product decisions.
- **Codex A / Implementer:** modifies only authorized scope, updates this Plan,
  Decision log, and Completion evidence, runs verification and self-review,
  and escalates unauthorized high-risk expansion.
- **Codex B / Independent Reviewer:** remains strictly read-only by default,
  independently inspects actual evidence, and emits a standalone conclusion
  without modifying source, documentation, or this Plan.

## Invariants

List architecture, compatibility, data-integrity, control, security, MSRV, and
scope boundaries that must remain true.

## Proposed design

Describe only the task-specific delta. Use a small diagram when relationships
would otherwise be ambiguous.

## Implementation slices

### Slice 1: <name>

- [ ] Implementation
- [ ] Direct behavior tests
- [ ] Slice verification
- [ ] Slice self-review

Add independently reviewable slices as needed.

## Acceptance criteria

- [ ] Observable result
- [ ] Compatibility result
- [ ] Failure-path result
- [ ] Documentation result

## Verification

```text
./scripts/verify fast
./scripts/verify full
./scripts/verify msrv
```

Add task-specific tests, benchmarks, dependency checks, and diff checks.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| YYYY-MM-DD | Initial design choice | Evidence and trade-off |

## Review findings

Before review, state that no independent conclusion has occurred. Codex B emits
a standalone read-only report. After the User / Product Owner and Mentor /
Orchestrator accept its disposition, Codex A or a separate explicitly
write-authorized closure session records the reviewer, conclusion, findings
with severity, location, risk, required correction, and skipped checks here.
Do not require Codex B to modify this Plan.

## Completion evidence

Final HEAD, worktree summary, commands actually run and their outcomes,
remaining risk, links to corrected documentation, and accepted-review
writeback. Keep the Plan active until required corrections and re-verification
are complete; only then mark it Completed and move it to `completed/`.
