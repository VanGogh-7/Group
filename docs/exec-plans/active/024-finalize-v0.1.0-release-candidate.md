# 024 Finalize v0.1.0 Release Content and Candidate Identity

## Status

Approved by the User on 2026-08-04. In Progress.

- [x] Baseline recorded.
- [x] Design reviewed and approved by the User.
- [ ] Slices implemented.
- [ ] Verification passed.
- [ ] Independent review completed.
- [ ] Review accepted and written back by a write-authorized role.
- [ ] Completion evidence recorded.

## Goal

Produce one immutable, release-facing `v0.1.0` candidate commit whose README and
quality documentation accurately describe the completed Phase 2 evidence and
remaining release blockers, then bind full local verification, clean package
archives, hosted CI, and independent review to that exact commit.

This Stage prepares the sole future `v0.1.0` tag target. It does not authorize or
perform a tag, GitHub Release, crates.io publication, registry authentication, or
fresh crates.io consumer verification.

## Non-goals

- Change Rust production code, tests, public APIs, package manifests,
  dependencies, features, `Cargo.lock`, checkpoint formats, migrations,
  scheduling, concurrency, cancellation, or persistence behavior.
- Create or push a Git tag, create a GitHub Release, run `cargo publish`, reserve
  crate names, or use crates.io credentials.
- Treat CI or package evidence from a different commit as evidence for the final
  candidate.
- Broaden Group into product RAG, Memory, UI, authorization, or prompt policy.

## Context

- [Architecture](../../../ARCHITECTURE.md)
- [README](../../../README.md)
- [Quality and Release Status](../../quality.md)
- [Release Runbook](../../runbooks/release.md)
- [Execution Plan workflow](../README.md)
- [Plan 022: v0.1.0 Local Release Preparation](../completed/022-v0.1.0-release-readiness.md)
- [Plan 023: Clean v0.1.0 Release-Candidate Verification](../completed/023-clean-v0.1.0-release-candidate-verification.md)

Starting baseline recorded on 2026-08-04:

- Branch: `master`.
- HEAD and `origin/master`:
  `7d0b8a6c9c952d409cbef8daff4fdf8f0149309c`.
- Worktree: clean before this Plan was created.
- Harness: compatible and healthy; prior workflow state `STAGE_COMPLETED` for
  T2/T2.2.
- GitHub Actions run `30899612143` for the starting HEAD was still in progress
  at Stage creation. Because Slice T3.1 changes release-facing documentation,
  that run cannot establish hosted-CI evidence for the later final candidate.
- Plan 023 bound Phase 2 evidence to
  `9b069d430cae02e74134f37edb8d05b83c2cc6c7`; this Stage must not relabel that
  evidence as proof for a new SHA.

## Roles and permissions

- **User / Product Owner:** approved this Stage design on 2026-08-04. Commit,
  push, tag, GitHub Release, crates.io publication, and credential use remain
  separate operations. The Orchestrator must stop at the relevant Human
  Checkpoint unless the User explicitly authorizes the exact operation.
- **Mentor / Orchestrator:** enforces candidate identity, evidence boundaries,
  Harness ordering, review independence, and stop conditions. It may perform
  read-only original-source CI lookup and host-side offline verification when a
  Codex sandbox cannot run a required loopback test.
- **Codex A / Implementer:** may modify only the authorized documentation and
  Plan scope, run read-only checks, and produce structured handoffs. It must not
  commit, push, tag, publish, use credentials, or change package inputs.
- **Codex B / Independent Reviewer:** remains strictly read-only and verifies
  documentation truthfulness, candidate identity, evidence provenance, command
  outcomes, and scope.

## Invariants

- The final candidate SHA is selected only after all candidate-content changes
  are committed with explicit authorization.
- Candidate verification runs in a clean detached checkout of that exact SHA;
  the checkout remains clean before and after accepted commands.
- Candidate SHA, archive source, hosted-CI SHA, and future tag target are
  identical.
- Historical evidence for `9b069d4` and starting-HEAD evidence for `7d0b8a6`
  remain correctly scoped and are not reused as final-candidate proof.
- All eight packages remain version `0.1.0` and preserve the fixed release order:
  Core, Model, Tool, SQLite, Observability, Genai, MCP, Prebuilt.
- No package-relevant source, manifest, dependency, lockfile, API, or behavior
  change is permitted.
- No release-side effect or credential use occurs in this Stage.

## Proposed design

First reconcile only release-facing and Harness status documentation so the
future package README states what Phase 2 proved and what still blocks release.
After deterministic gates and independent review, stop for explicit commit and
push authorization. The resulting clean commit becomes the immutable final
candidate.

Then verify the exact candidate in an external detached checkout. Run the full
repository matrix and repeat the eight-archive clean audit without
`--allow-dirty`, retain bounded checksummed evidence, and require successful
hosted CI for the same SHA. No writeback may mutate the candidate; Stage closure
records evidence in a later repository commit while preserving the future tag
target as the immutable verified candidate SHA.

## Implementation slices

### Slice T3.1: Reconcile release-facing documentation

- [x] Update the README release-status section to record completed clean-candidate
  and hosted-CI evidence while retaining tag, publication/index, and fresh
  consumer blockers.
- [x] Correct stale Harness project-status prose through the canonical,
  fail-closed `harness sync-project-state` command without manually rewriting
  protected Harness control state.
- [x] Reconcile README, `docs/quality.md`, Plan 023, and the Release Runbook so
  their evidence boundaries and remaining blockers agree.
- [x] Keep changes documentation-only; do not alter package inputs other than the
  root README that is intentionally release-facing.
- [x] Run focused documentation checks.
- [ ] Run configured Harness fast and Slice gates.
- [ ] Obtain independent read-only review with no unresolved finding.
- [x] Stop before commit or push unless the User separately authorizes both.

Configured gates and independent review are Orchestrator-owned post-handoff
steps. Their pending status is not an Implementer blocker. If the authorized
documentation work is complete and no product correction is required, the
Implementer handoff must use `status = completed` with an empty `blockers`
array; it must not classify pending post-handoff gates or review as
implementation blockers.

### Slice T3.2: Cut and verify the immutable final candidate

- [ ] After explicit authorization, create and push the corrected candidate commit
  from the accepted T3.1 worktree and record its exact SHA.
- [ ] Create a fresh detached external checkout at that SHA and prove clean status
  before and after every accepted gate.
- [ ] Run `./scripts/verify all` for the exact candidate and retain bounded raw
  evidence.
- [ ] Repeat all eight clean package-list, archive, extraction,
  normalized-manifest, dependency-edge, README, license, intended-content, and
  filename-only secret-indicator checks without `--allow-dirty`.
- [ ] Record archive hashes and a complete checksum manifest in an owner-only
  evidence directory with an explicit retention disposition.
- [ ] Require successful hosted full-workspace and layered-MSRV jobs for the
  exact candidate SHA.
- [ ] Run Harness fast, Slice, and Stage gates and obtain final independent
  read-only review.
- [ ] Record the immutable candidate SHA as the sole future `v0.1.0` tag target;
  perform no tag, publication, GitHub Release, credential use, or registry
  consumer operation.

## Acceptance criteria

- [x] README, quality ledger, completed Plans, and Release Runbook accurately
  distinguish completed Phase 2 evidence from pending Phase 3 work.
- [ ] One exact immutable candidate SHA is recorded as the sole future tag target.
- [ ] Candidate SHA, archive source, hosted CI, and proposed tag target match.
- [ ] `./scripts/verify all` passes for the exact final candidate.
- [ ] Both required hosted CI jobs pass for the exact final candidate.
- [ ] All eight clean archives pass the complete release audit and have retained
  hashes and checksummed evidence.
- [ ] Candidate checkout remains clean before and after all accepted commands.
- [ ] No production code, tests, APIs, manifests, dependencies, lockfile, or
  package behavior changes are introduced.
- [ ] No tag, GitHub Release, crates.io publication, registry authentication, or
  fresh registry consumer operation occurs.
- [ ] Harness Stage gates and independent review pass with no unresolved finding.

## Verification

```text
git status --short
git rev-parse HEAD
git rev-parse origin/master
git diff --check
./scripts/verify fast
./scripts/verify all
cargo package --locked -p <crate> --list
cargo package --locked -p <crate> [documented non-persistent patches]
gh run view <run-id> --json status,conclusion,headSha,jobs,url
sha256sum -c evidence.sha256
```

Package extraction, normalized-manifest parsing, dependency checks, README and
license equality, safe regular-file checks, and filename-only secret screening
follow `docs/runbooks/release.md`. Harness verification uses configured fast,
Slice, and Stage gates.

## Stop conditions

Stop and request direction if:

- commit or push is required but not explicitly authorized;
- tag creation/push, GitHub Release creation, publication, registry credential
  use, or a fresh registry consumer operation would occur;
- candidate identity differs among Git, archive source, hosted CI, or proposed
  tag target;
- any required verification, package audit, hosted-CI job, evidence checksum, or
  independent review fails or cannot run;
- continuing requires any production, test, API, manifest, dependency, lockfile,
  package-input, CI workflow, durable format, or release-scope change;
- an archive contains unexpected metadata, paths, files, license content, or a
  possible credential indicator without a safe documented disposition.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-08-04 | Use a separate final-candidate Stage before crates.io publication. | The previously verified SHA differs from current `master`, and the package README contains stale release-status text. |
| 2026-08-04 | Separate T3.1 documentation reconciliation from T3.2 immutable-candidate verification. | Candidate content must settle before evidence is bound to its exact SHA. |
| 2026-08-04 | Require a Human Checkpoint before commit and push, and exclude all tag/publication operations. | Repository and release policy require separate explicit authority for external or history-changing effects. |
| 2026-08-04 | Synchronize protected `PROJECT_STATE.md` prose through `harness sync-project-state`. | The approved T3.1 criterion must be satisfied without manual writes or weakening the protected-state baseline. |
| 2026-08-04 | Treat pending gates and review as post-handoff work, not Implementer blockers. | Harness runs those steps only after accepting a completed Implementer handoff. |
| 2026-08-04 | Keep T2 evidence SHA-bound. | README is a package input. |
| 2026-08-05 | Bind T3.2 verification to candidate `512c187e2c08a78d39a35e623fa0df8e2d66f3f2`. | `HEAD` and `origin/master` match the explicitly authorized candidate commit; all later verification and future tag-target evidence must use this SHA. |
| 2026-08-05 | Supersede `512c187e2c08a78d39a35e623fa0df8e2d66f3f2` before tagging. | Final review found that its release-facing README and quality ledger still described the final candidate as pending; its evidence remains historical and cannot authorize a tag. |

## Review findings

The first final independent review requested four evidence corrections: retain
the successful host `verify all` result, the complete eight-archive audit, exact-
SHA hosted-CI JSON, and a temporally accurate checkout-cleanliness statement.
The evidence and prose corrections below address those findings. Final rereview
after refreshed deterministic gates remains pending. A later rereview accepted
the host verification, package audit, and hosted-CI evidence but rejected
`512c187e2c08a78d39a35e623fa0df8e2d66f3f2` because its release-facing status
remained stale and external no-release-side-effect evidence was incomplete. The
User authorized a corrected candidate commit and complete evidence rebinding.

## Completion evidence

Slice T3.1 Implementer reconciliation updated only `README.md`,
`docs/quality.md`, `docs/runbooks/release.md`, the active Plan index, this Plan,
and the historical boundary note in Plan 023. The Orchestrator synchronized
the stale protected Harness project-status prose through the canonical
`harness sync-project-state` command; the Implementer did not manually rewrite
canonical Harness control state. Fast and Slice gates plus independent review
remain Orchestrator-owned after handoff. Attempt 2 also corrected the lower
README release section, which still described clean-candidate and hosted-CI
evidence as wholly pending. `git diff --check`, focused `markdownlint-cli2`
with the repository's pre-existing long table and code lines excluded from
MD013, and `./scripts/verify fast` passed. The first default-lint invocation
reported only those pre-existing MD013 lines and stopped before running the
fast gate. A later lint-only recheck used an unsupported process-substitution
configuration path and exited before linting. The accepted scoped reruns used
a supported temporary JSON configuration, passed with zero issues, and the
first of those reruns then ran the fast gate. HEAD and `origin/master` remained
`7d0b8a6c9c952d409cbef8daff4fdf8f0149309c`. Attempt 3 corrected the stale
completion-evidence sentence after canonical Harness synchronization, then
repeated the focused six-file Markdown lint and `./scripts/verify fast`; both
passed. No commit or push occurred.

Stage completion evidence remains pending. Record the final candidate SHA,
before/after worktree state, actual commands and outcomes, retained evidence
path and checksum manifest, hosted-CI source JSON, review disposition, skipped
checks, and remaining Phase 3 risks.

Slice T3.2 attempt 2 confirmed that the Orchestrator-created and pushed
candidate commit is
`512c187e2c08a78d39a35e623fa0df8e2d66f3f2`: primary-worktree `HEAD`,
`origin/master`, detached-checkout `HEAD`, and detached-checkout
`origin/master` all resolved to that exact SHA. The primary worktree already
contained an unrelated protected `.harness/state.json` modification, which the
Implementer preserved without reading or editing. A fresh owner-only standalone
local clone was detached at the candidate under
`/home/van-gogh/project/Rust_code/Group/target/t3-release-artifacts/candidate.fCWkU2`.
The retained status captures prove that the checkout was clean before and after
the `./scripts/verify all` attempt and after the later package-list checks. The
failed package-audit path did not retain an immediate post-failure status
capture, so no stronger per-command cleanliness claim is made for that attempt.

Bounded evidence is retained owner-only at
`/home/van-gogh/project/Rust_code/Group/target/t3-release-artifacts/evidence.sHbvPD`
through final Harness gates and independent review; the Orchestrator may remove
only these exact candidate and evidence directories after Stage disposition.
`TMPDIR=/tmp ./scripts/verify all` reached the offline Genai continuation test
and exited `101` only because the Codex sandbox denied loopback server creation
with `Operation not permitted`. Clean package generation without
`--allow-dirty` then exited `101` because the network-isolated sandbox could not
resolve `index.crates.io`; no archive result is accepted from that attempt.
These execution-boundary results require the Plan-assigned Orchestrator host
reruns and are not classified as candidate failures or passing evidence. All
eight `cargo package --locked -p <crate> --list` commands passed in fixed order,
focused Markdown lint reported zero issues, `git diff --check` passed, and
`./scripts/verify fast` passed. The retained final status capture was clean.
The Implementer retained command logs, exit codes, identity, the available
bounded status captures, the adapted package-audit script, and all eight
package lists. The diagnostic evidence inventory and its checksum manifest
passed `sha256sum -c`; their final inventory contains 48 files, and the
manifest SHA-256 is
`0d6f57205d6af40d65b2f63afb694ddd3932fe01f40657ea20a91b552048061a`.
The successful host `./scripts/verify all`, complete eight-archive audit,
archive hashes and complete accepted-evidence checksum manifest,
original-source hosted-CI JSON for the exact candidate, Harness gates, and
independent review remain Orchestrator-owned post-handoff work and must be
added before Stage completion.

Slice T3.2 attempt 3 resolved the reviewer's medium evidence-overstatement
finding by narrowing the checkout-cleanliness prose to the retained status
captures and explicitly recording the missing immediate post-failure capture.
The detached checkout was still clean at the candidate SHA, all 48 retained
diagnostic files again passed `sha256sum -c evidence.sha256`, focused Markdown
lint reported zero issues, `git diff --check` passed, and
`./scripts/verify fast` passed. No candidate, archive, hosted-CI, tag, or
publication identity was changed.

The Orchestrator then completed and retained the host-owned acceptance evidence
at `/tmp/group-t3.2-evidence.FoKnCx`, an owner-only mode-0700 directory retained
by the local `van-gogh` user through Stage approval and closure reporting. Its
final root manifest covers 262 files, passes `sha256sum -c evidence.sha256`, and
has SHA-256
`733d3648a45472c3a831f86f6e4e1607a3e6fe5828857b2c599bec9fba05f2d3`.
The nested package-audit manifest also passes independently.

In a fresh detached checkout at
`512c187e2c08a78d39a35e623fa0df8e2d66f3f2`, the host reran
`./scripts/verify all` with `CARGO_BUILD_JOBS=2` and a disk-backed `TMPDIR`; the
accepted rerun exited 0 and ended with `verification mode 'all' passed`. Raw
output, exit status, candidate identity, empty post-run `git status --short`,
and passing post-run `git diff --check` are retained. An earlier host attempt is
retained only as diagnostic evidence: it failed when the linker received
`SIGBUS` while tmpfs and swap were exhausted. After disposable candidate build
directories were removed and `TMPDIR` moved to disk, the focused MCP doctest and
the complete rerun passed. The failure is not accepted as product evidence.

The accepted package audit contains all eight clean archives built without
`--allow-dirty`, their SHA-256 hashes, extracted trees, package lists,
normalized-manifest and internal-dependency checks, README and license checks,
intended-content checks, safe-path and regular-file checks, and filename-only
secret-indicator results. The only indicator was the expected
`group-agent-genai/src/error.rs` identifier and has a retained safe disposition.
The audit report records `status = passed`, candidate-clean before and after,
and candidate SHA
`512c187e2c08a78d39a35e623fa0df8e2d66f3f2`.

Original-source GitHub Actions JSON is retained as
`hosted-ci-30935008094.json`. Run
`https://github.com/VanGogh-7/Group/actions/runs/30935008094` completed
successfully for the exact candidate SHA. Both required jobs passed:
`Full workspace (Rust 1.88)` and
`Layered MSRV (Rust 1.85 and 1.88)`. For the superseded candidate, Git identity,
archive source, and hosted CI all bound to
`512c187e2c08a78d39a35e623fa0df8e2d66f3f2`. Final review rejected that SHA
as the future tag target because its own release-facing status was stale. Its
evidence remains historical and must not be reused for the corrected candidate.

After the Human Checkpoint, the User explicitly authorized corrections to
`README.md` and `docs/quality.md`, creation and push of a new candidate commit,
and complete T3.2 rebinding. Before that operation, the Orchestrator retained
raw remote-tag, GitHub Release, and crates.io sparse-index state in the
owner-only `/tmp/group-t3.2-evidence-v2.G90zjS` directory. Corrected-candidate
identity and all acceptance evidence remain pending.
