# Execution Plans

Execution Plans are tracked, living artifacts for complex Group work. They
make intent, invariants, slices, decisions, verification, and review evidence
available in the repository instead of only in a prompt or chat transcript.

## When a Plan is required

Use an Execution Plan for work involving any of:

- multiple crates or multiple documentation responsibility areas;
- public API or compatibility;
- concurrency, cancellation, timeout, or scheduling;
- checkpoint format, Store behavior, CAS, migration, or durable lineage;
- provider or MCP protocols and lifecycle;
- multi-session behavior;
- security-sensitive error or logging changes;
- a non-mechanical migration that needs independently reviewable slices.

Small, local, low-risk fixes use a short Task Brief instead.

## Lifecycle

1. Copy [TEMPLATE.md](TEMPLATE.md) into `active/` with the next stable number.
2. Record the baseline, invariants, slices, acceptance, and verification before
   implementation.
3. Codex A / Implementer updates checklists, the Decision log, verification,
   and Completion evidence while implementing and self-reviewing.
4. Codex B / Independent Reviewer performs a strictly read-only review and
   emits a standalone report; Codex B does not edit the Plan.
5. The User / Product Owner and Mentor / Orchestrator evaluate that report and
   decide its disposition. The User retains final product acceptance.
6. Apply required corrections and rerun affected gates.
7. Codex A or a separate explicitly write-authorized closure session writes
   the accepted conclusion and Review Findings into the active Plan.
8. Only then mark Status `Completed`, finalize completion evidence, and move
   the file to `completed/`.

The move happens only when required work, accepted independent-review
evidence, corrections, and re-verification are complete.

## Roles and authority

- **User / Product Owner** defines goals and product trade-offs, authorizes
  public API, persistence-format, high-risk dependency, and comparable
  material changes, and makes the final acceptance decision. The User need not
  prescribe all implementation details.
- **Mentor / Orchestrator** checks technical direction, corrects unsound
  requirements or architecture, divides tasks and review boundaries, prepares
  or reviews Codex instructions, and explains whether review results require a
  correction task. The Mentor does not replace the User's product decisions.
- **Codex A / Implementer** modifies authorized files, maintains the Plan,
  Decision log, and Completion evidence, runs verification and self-review,
  and does not expand high-risk scope without authorization.
- **Codex B / Independent Reviewer** is read-only by default, independently
  inspects the diff, source, tests, and verification evidence, and emits PASS,
  PASS WITH MINOR FIXES, or REQUIRES FIX without modifying source,
  documentation, or the Plan.

## Naming

Use:

```text
NNN-short-task-name.md
```

Plan numbers are monotonic within this repository Harness. Product Stage
numbers are a separate concept. Ordinary repository engineering does not
become a product Stage.

## Content rules

- Plans are written in English.
- Plans describe task-specific deltas and link stable repository knowledge.
- Do not paste the full architecture into a Plan.
- Record meaningful decisions when they are made, not only in the final report.
- Every skipped command includes a reason and residual risk.
- Completion evidence contains actual commands and outcomes.
- A Plan is not a substitute for direct tests or current documentation.

## Review

Review uses [the review runbook](../runbooks/review.md) and one of:

- PASS
- PASS WITH MINOR FIXES
- REQUIRES FIX

Major findings prevent Plan completion. Minor findings must be fixed or
explicitly assigned before closure. The accepted report is written into the
Plan by Codex A or another write-authorized closure session, never by requiring
the read-only reviewer to edit repository files.
