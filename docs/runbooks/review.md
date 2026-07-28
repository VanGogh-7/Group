# Independent Review Runbook

Independent review is read-only unless a separate correction request
authorizes changes. Codex B / Independent Reviewer inspects and reports; Codex
A / Implementer or a separate write-authorized closure session performs any
later repository writeback.

The User / Product Owner owns goals, product trade-offs, material-change
authorization, and final acceptance. The Mentor / Orchestrator checks technical
direction, review boundaries, and correction needs without replacing the
User's product decisions. See the
[Development Runbook](development.md#roles) for the complete role contract.

## Conclusions

Use exactly one:

- **PASS**: acceptance is met and no corrective finding remains.
- **PASS WITH MINOR FIXES**: architecture is sound; bounded non-blocking
  corrections are required or explicitly assigned.
- **REQUIRES FIX**: at least one Major finding prevents acceptance.

## Severity

### Major

A Major includes:

- data corruption or durable lineage break;
- public compatibility break outside authorized scope;
- responsibility inversion or dependency cycle;
- nondeterministic State semantics;
- cancellation or shutdown path that abandons owned work incorrectly;
- secret or payload exposure in default formatting;
- unsafe retry of possibly non-idempotent work;
- false capability claim or fail-open protocol handling;
- required validation that cannot run and leaves a critical boundary unknown;
- implementation outside the authorized task.

### Minor

A Minor is bounded and does not invalidate the architecture, for example:

- missing direct edge-case coverage;
- inaccurate but non-dangerous documentation wording;
- low-risk diagnostic or benchmark noise;
- incomplete release engineering that does not alter runtime correctness.

### Suggestion

A Suggestion is optional improvement with no acceptance impact. Do not inflate
preferences into findings.

## Review discipline

1. Record starting `git status --short`, diff summary, HEAD, and protected
   artifact hashes.
2. Read the active Plan and relevant architecture, design, ADR, and quality
   documents.
3. Inspect current source, tests, examples, benches, migrations, manifests,
   lockfile, and upstream dependency source as required by the task.
4. Trace public API and dependency direction from actual code.
5. Verify success and failure paths, not only the happy-path helper.
6. Run commands independently and report actual output.
7. Inspect the final diff and repeat the baseline checks.
8. Emit a standalone conclusion and findings report for the User and Mentor.

Do not:

- judge only from README, AGENTS, or a development report;
- modify source, documentation, or the Execution Plan during a read-only
  review;
- create a commit;
- consume live provider quota without explicit opt-in;
- describe an unexecuted gate as passing;
- stringify a source chain to prove redaction safety;
- assume a child process fixture or timing test exercised the intended branch.

## Finding format

Every finding contains:

- severity;
- location;
- observed evidence;
- risk;
- required correction;
- whether a correction Plan or product corrective Stage is necessary.

Use a product `Stage x.1` only for a Major correction to a product Stage.
Ordinary repository, documentation, or bounded follow-up work is not a product
Stage.

## Performance review

Every independent review considers performance in proportion to risk:

- allocation and Clone behavior;
- algorithmic complexity;
- lock and transaction scope;
- spawn, channel, and hidden worker creation;
- benchmark setup and teardown inside measured regions;
- whether a comparative claim has measured evidence.

`cargo bench --no-run` proves benchmark compilation, not performance.

## Handoff

Codex B's standalone review report includes:

- conclusion;
- verification matrix;
- Major, Minor, and Suggestions;
- architecture and dependency assessment;
- control, error, security, and performance assessment;
- test and documentation assessment;
- final worktree evidence;
- recommended next step.

The post-review writeback sequence is:

1. Codex B emits the read-only report.
2. The User / Product Owner and Mentor / Orchestrator decide whether to accept
   it and what correction or re-review is required.
3. Codex A or a separate explicitly write-authorized closure session records
   the accepted conclusion and Review Findings in the active Plan.
4. Only after required corrections and re-verification pass may that writer
   mark the Plan Completed and move it from `active/` to `completed/`.

The reviewer is never required to modify the Plan. The Plan remains active
until the accepted report, corrections, verification, and writeback are
recorded.
