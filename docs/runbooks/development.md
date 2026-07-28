# Development Runbook

Group uses a four-role Harness workflow for complex work. One person or session
may hold more than one non-review role when authority is explicit, but the
Independent Reviewer remains separate from implementation reasoning and
read-only by default.

## Roles

### User / Product Owner

The User / Product Owner:

- states goals, product trade-offs, and the final acceptance decision;
- authorizes public API, persistence-format, high-risk dependency, and other
  material scope changes;
- does not need to prescribe every engineering implementation detail.

### Mentor / Orchestrator

The Mentor / Orchestrator:

- checks technical direction and corrects unsound requirements or
  architecture;
- divides work into tasks, Plans, implementation slices, and review
  boundaries;
- writes or reviews task instructions for Codex;
- explains independent-review results and determines whether a correction task
  is needed;
- does not replace the User / Product Owner's final product trade-offs.

### Codex A / Implementer

Codex A:

- reads the repository entrypoints and relevant active Plan;
- records the baseline and preserves unrelated worktree changes;
- implements the smallest reviewable slice;
- adds direct behavior tests;
- runs verification and performance checks;
- self-reviews scope, failure paths, security, and compatibility;
- updates the Execution Plan, Decision log, Completion evidence, and current
  documentation;
- stays within authorized scope and escalates material high-risk expansion;
- hands evidence to Codex B.

### Codex B / Independent Reviewer

Codex B:

- is strictly read-only by default and does not modify source, documentation,
  or the Execution Plan;
- does not rely only on the development report;
- reads current source, tests, manifests, dependency source when relevant, and
  the worktree diff;
- checks the Plan's invariants and acceptance;
- reruns proportionate gates;
- classifies findings using the review runbook;
- emits a standalone report with PASS, PASS WITH MINOR FIXES, or REQUIRES FIX.

The roles may be performed in separate sessions. The reviewer must remain
independent of the implementation reasoning.

## Workflow

```text
Plan
  -> Implement
  -> Verify
  -> Self-review
  -> Independent Review
  -> Accept Review
  -> Correct
  -> Write Back
  -> Close
```

### 1. Plan

Use a short Task Brief for a small local fix. Create an Execution Plan for
cross-crate, public API, concurrency, durable, protocol, security-sensitive, or
multi-session work.

Record scope, non-goals, invariants, slices, acceptance, and verification
before changing implementation.

### 2. Implement

Implement one minimal slice. Preserve dependency direction and unrelated
worktree changes. Do not broaden task authority because an adjacent idea is
useful.

### 3. Verify

Run the unified entrypoint:

```bash
./scripts/verify fast
```

Run task-specific direct tests while iterating. Before review, normally run:

```bash
./scripts/verify full
./scripts/verify msrv
```

The verification gates never contact a live Model Provider, MCP Server, or
external test service. Cargo may download dependencies already fixed by
`Cargo.lock` when the local registry or cache is missing. Set
`GROUP_VERIFY_OFFLINE=1` to export `CARGO_NET_OFFLINE=true`; missing cached
dependencies then fail without a network fallback.

### 4. Self-review

Inspect:

- full diff and changed-file scope;
- public API and dependency impact;
- cancellation, timeout, and failure paths;
- payload and secret exposure;
- deterministic ordering and side effects;
- durable compatibility and migration impact;
- performance structure and benchmark relevance;
- documentation and Plan synchronization.

### 5. Independent Review

Codex B follows [review.md](review.md). A report includes exact evidence and
does not convert unexecuted checks into passes. Codex B does not write the
report into repository files.

### 6. Accept Review

The User / Product Owner and Mentor / Orchestrator evaluate the standalone
report. The User retains final product acceptance; the Mentor assesses the
technical disposition and whether a bounded correction task or re-review is
needed.

### 7. Correct

Major findings require correction and re-review. Minor findings are fixed in a
bounded correction or explicitly assigned with rationale. Rerun affected
gates, not only the new test.

### 8. Write Back

After the report is accepted, Codex A or a separate session with explicit
write authority records the conclusion and Review Findings in the Plan. That
writer also updates the current source of truth, related ADR when the decision
changed, the quality ledger when debt changed, and the Plan's Decision log and
Completion evidence. Do not ask the read-only reviewer to perform this
writeback, and do not copy the same detailed rule into multiple documents.

### 9. Close

Only after accepted review, required corrections, re-verification, and
writeback are complete, record remaining risk, HEAD, and worktree status. Mark
the Plan Completed and move it from active to completed.

Do not create a Git commit unless the user explicitly requests one.
