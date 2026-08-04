# 023 Clean v0.1.0 Release-Candidate Verification

## Status

Completed on 2026-08-04. Both slices passed their required Harness gates and
independent read-only review; the write-authorized Orchestrator accepted Stage
T2. No release-side effect was performed.

- [x] Baseline recorded.
- [x] Design reviewed and approved by the User.
- [x] Slices implemented.
- [x] Verification passed.
- [x] Independent review completed.
- [x] Review accepted and written back by a write-authorized role.
- [x] Completion evidence recorded.

## Goal

Establish reproducible, read-only release-candidate evidence for commit
`9b069d430cae02e74134f37edb8d05b83c2cc6c7`: prove that the exact clean commit
passes the repository verification contract, produces the intended eight local
`0.1.0` package archives without `--allow-dirty`, passes archive and normalized
manifest inspection, and has successful GitHub-hosted full-workspace and
layered-MSRV jobs for the same commit.

This Stage proves only the clean candidate and hosted-CI gates in Phase 2 of the
Release Runbook. It does not authorize or perform a tag, GitHub Release,
crates.io publication, registry ownership operation, or fresh registry consumer
verification.

## Non-goals

- Change production code, tests, public APIs, dependency versions, features,
  checkpoint formats, migrations, durable lineage, scheduling, or concurrency.
- Modify package manifests, package contents, CI behavior, or the verification
  scripts to make the candidate pass.
- Run `cargo publish`, create or push a tag, create a GitHub Release, reserve a
  crate name, or use crates.io credentials.
- Claim that any crate has been published or that v0.1.0 has been released.
- Use a live Model Provider, remote MCP Server, provider credential, or external
  quota-consuming test service.
- Treat evidence from a different commit or a dirty tree as candidate evidence.

## Context

- [Architecture](../../../ARCHITECTURE.md)
- [README](../../../README.md)
- [Quality and Release Status](../../quality.md)
- [Release Runbook](../../runbooks/release.md)
- [Execution Plan workflow](../README.md)
- [Plan 022: v0.1.0 Local Release Preparation](../completed/022-v0.1.0-release-readiness.md)
- [T1 public-boundary offline smoke coverage](../../../.harness/CURRENT_STAGE.md)

Starting evidence recorded on 2026-08-04:

- Candidate commit: `9b069d430cae02e74134f37edb8d05b83c2cc6c7`.
- Branch: `master`; `origin/master` resolved to the same commit.
- The primary worktree was clean before this Plan and the T2 Harness state were
  created.
- GitHub Actions run `30848946632` targets the exact candidate commit. The
  Orchestrator re-read the original GitHub source on 2026-08-04 after the run
  completed: the run conclusion is `success`; `Full workspace (Rust 1.88)`
  completed successfully at `2026-08-03T20:18:17Z`; and `Layered MSRV (Rust
  1.85 and 1.88)` completed successfully at `2026-08-03T20:14:51Z`. Run URL:
  `https://github.com/VanGogh-7/Group/actions/runs/30848946632`.
- Plan 022 completed the dirty-tree diagnostic package preflight and local
  release preparation. That evidence is not reused as a clean candidate pass.

## Roles and permissions

- **User / Product Owner:** approved this bounded Stage on 2026-08-04. Any tag,
  publication, GitHub Release, credential use, commit, or push still requires
  separate explicit authorization.
- **Mentor / Orchestrator:** enforces candidate identity, evidence boundaries,
  Harness ordering, review independence, and stop conditions. It owns live
  GitHub-hosted CI lookup from the original source because the isolated Codex
  Worker is not granted GitHub API credentials or network access. It also owns
  the host-side `./scripts/verify all` invocation when the Codex sandbox blocks
  the repository's required offline loopback test server.
- **Codex A / Implementer:** may create disposable external worktrees and
  evidence directories, run read-only verification and package inspection, and
  update only this Plan plus `docs/quality.md` when supported by completed
  evidence. It must not change candidate package inputs or production files,
  and it must consume the recorded Orchestrator CI evidence rather than retry a
  live GitHub API call from its isolated sandbox. It must also consume the
  recorded host-side full-verification evidence rather than rerun that command
  inside the sandbox.
- **Codex B / Independent Reviewer:** remains strictly read-only and verifies
  commit identity, clean-tree evidence, command outcomes, archive inspection,
  hosted CI provenance, documentation accuracy, and scope.

## Invariants

- The candidate under test is exactly
  `9b069d430cae02e74134f37edb8d05b83c2cc6c7`.
- Candidate commands run in a fresh detached Git worktree outside the primary
  repository worktree. Its `git status --short` must be empty before and after
  every accepted candidate gate.
- Evidence is stored in a fresh external directory. Generated archives,
  extraction directories, logs, and hashes are not committed.
- All eight crates remain at `0.1.0` and are checked in the fixed order from the
  Release Runbook.
- Dependent archive generation may use only the documented non-persistent
  command-line path patches and `--no-verify` while exact internal versions are
  unpublished. No patch is written to any manifest or normalized archive.
- Every normalized internal dependency remains exactly `0.1.0` with no `path`,
  Git, registry, or registry-index override.
- Each archive contains its intended README and byte-identical MIT and Apache
  license files, and excludes repository-only, generated, environment, secret,
  and credential material.
- GitHub-hosted evidence is accepted only when both required jobs succeed for
  the exact candidate SHA.
- No failed, skipped, cancelled, stale, dirty-tree, or different-commit result
  is relabeled as passing evidence.
- Any package-relevant change creates a new candidate and invalidates prior
  Phase 2 evidence.

## Proposed design

Use a temporary detached Git worktree pinned to the candidate SHA so Harness
state and Plan writeback in the primary worktree cannot contaminate the tested
candidate. Candidate compilation and extraction must use the root-backed
`/home/van-gogh/.cache/group-harness-tmp/` area, not the 7.5 GiB `/tmp` tmpfs.
Create a separate owner-only evidence directory, record candidate
identity and clean status, run the repository verification contract, generate
and inspect all eight archives without `--allow-dirty`, and record artifact
hashes and bounded command outcomes. The Orchestrator separately reconciles
those results with the original GitHub source for the same SHA and records the
result in this Plan. When the Codex sandbox cannot create the offline loopback
server required by the Genai tests, the Orchestrator runs the exact full
verification command against the same clean detached candidate on the host and
records its result here. The isolated Codex Worker validates the recorded
identity, cleanliness, and evidence without repeating sandbox-incompatible
commands. Update the quality ledger only with claims supported by complete
evidence.

The disposable worktree and evidence directory are operational artifacts, not
repository deliverables. Their absolute paths and cleanup disposition must be
recorded without embedding secrets or raw matching credential content.

## Implementation slices

### Slice T2.1: Candidate identity, hosted CI, and verification

- [x] Create a fresh detached worktree at the exact candidate SHA and prove it
  is clean.
- [x] Confirm `origin/master` and GitHub Actions run `30848946632` refer to the
  exact candidate SHA.
- [x] Require successful GitHub-hosted `Full workspace (Rust 1.88)` and
  `Layered MSRV (Rust 1.85 and 1.88)` jobs.
- [x] Run `./scripts/verify all` in the detached candidate worktree and preserve
  bounded evidence.
- [x] Confirm the candidate remains clean and record actual outcomes.
- [x] After the Implementer reports `completed`, the Orchestrator runs Harness
  fast and slice gates and launches the independent read-only Reviewer. The
  isolated Implementer must not invoke these Harness controller commands and
  must not treat their pending state as an implementation blocker.

### Slice T2.2: Eight-archive clean package audit and release-ledger writeback

- [x] Generate all eight package archives from the clean candidate without
  `--allow-dirty`, in the fixed release order.
- [x] Save and inspect every package list, archive inventory, extracted regular
  files, normalized manifest, internal dependency edge, README, and both
  license copies.
- [x] Perform filename-only secret-indicator screening and record every hit
  disposition without exposing matching content.
- [x] Record SHA-256 hashes for the candidate identity and all eight archives.
- [x] Confirm the candidate worktree remains clean and no manifest patch was
  persisted.
- [x] Update `docs/quality.md` and this Plan with precise supported claims,
  skipped checks, remaining release blockers, and cleanup disposition.
- [x] Run Harness fast, Slice, and Stage gates after the completed T2.2
  Implementer handoff.
- [x] Obtain final independent read-only review against the unchanged gated
  worktree.

## Acceptance criteria

- [x] The exact candidate SHA is recorded consistently across Git, local
  verification, package evidence, GitHub Actions, and the proposed future tag
  target.
- [x] A fresh detached candidate worktree is clean before and after all accepted
  commands.
- [x] `./scripts/verify all` passes for the exact candidate.
- [x] Both required GitHub-hosted jobs pass for the exact candidate.
- [x] All eight archives are generated without `--allow-dirty` and pass package
  list, inventory, normalized-manifest, dependency, license, and filename-only
  secret screening checks.
- [x] Archive SHA-256 hashes and evidence paths are recorded.
- [x] No production, test, manifest, lockfile, CI, API, or dependency change is
  introduced by this Stage.
- [x] Documentation does not claim tag creation, publication, registry
  resolution, or complete release.
- [x] Stage gates pass.
- [x] Independent review passes with no unresolved finding.

## Verification

```text
git status --short
git rev-parse HEAD
git rev-parse origin/master
# Orchestrator-only original-source lookup; not a Codex Worker requirement:
gh run view 30848946632 --json status,conclusion,headSha,jobs,url
./scripts/verify all
cargo package --locked -p <crate> --list
cargo package --locked -p <crate> [documented non-persistent patches]
git diff --check
```

Package extraction, normalized-manifest parsing, regular-file checks,
byte-for-byte license comparisons, archive hashing, and filename-only secret
screening follow `docs/runbooks/release.md` Phase 2. Harness verification uses
configured fast, slice, and stage gates.

## Stop conditions

Stop and request direction if:

- either hosted CI job fails, is cancelled, or targets another commit;
- the detached candidate worktree is dirty or changes during verification;
- any required verification or package command fails;
- an archive inventory, normalized manifest, internal version, license, README,
  or filename-only secret screen is unexpected;
- continuing requires a production, test, API, manifest, lockfile, dependency,
  CI, package-input, credential, tag, publication, or registry change;
- candidate identity differs among local Git, archive source, hosted CI, or the
  proposed tag target.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-08-04 | Use `9b069d430cae02e74134f37edb8d05b83c2cc6c7` as the bounded Phase 2 candidate. | The primary worktree was clean, `master` and `origin/master` matched, and the current hosted CI run targets this commit. |
| 2026-08-04 | Verify in a detached external worktree. | Harness and evidence writeback modify the primary worktree; an external detached worktree preserves the clean candidate boundary. |
| 2026-08-04 | Separate candidate verification from irreversible release operations. | Tagging, GitHub Release creation, crates.io publication, and fresh registry consumption require distinct explicit authorization. |
| 2026-08-04 | Assign live hosted-CI lookup to the Orchestrator and local candidate verification to the isolated Codex Worker. | Attempt 2 established that the credential-isolated Worker cannot reach the GitHub API. The User explicitly approved this responsibility split, and the Orchestrator verified both required jobs from the original GitHub source for the exact candidate SHA. |
| 2026-08-04 | Stop Slice T2.1 attempt 3 after `./scripts/verify all` exhausted the worker disk quota. | The required gate exited `101` while compiling with `Disk quota exceeded` and `No space left on device`; the Plan requires stopping on any required verification failure. |
| 2026-08-04 | After the Human Checkpoint, clean the 5.9 GiB failed candidate and retry with root-backed temporary storage. | The User explicitly authorized cleanup and retry. `/tmp` had only 1.4 GiB free while the root filesystem had 117 GiB free; after bounded cleanup `/tmp` returned to 7.3 GiB free, and `/home/van-gogh/.cache/group-harness-tmp/` was created owner-only for the retry. |
| 2026-08-04 | Run the required full verification on the Orchestrator host and require the isolated Worker to consume that evidence. | The root-backed retry proved candidate identity and cleanliness, but the Codex sandbox denied the offline Genai loopback server with `Operation not permitted`. The same exact command passed outside that sandbox against the same clean candidate; no live provider or remote service was used. |
| 2026-08-04 | Preserve bounded host-verification and original-source GitHub JSON under the retained evidence directory. | The first independent review correctly found that prose alone did not bind the successful host command or hosted-CI result to independently inspectable artifacts. |
| 2026-08-04 | Run clean archive generation and inspection on the Orchestrator host, then require the isolated Worker to validate retained evidence. | The credential- and network-isolated Worker completed all eight package lists but could not download crates.io `config.json` for archive generation. The host-side command used no registry credential, performed no publication, and retained every command, archive, extraction, report, and hash. |

## Review findings

The first independent T2.1 review required three corrections: retain bounded
host-side `./scripts/verify all` output, retain the original-source GitHub JSON,
and disambiguate the post-Human-Checkpoint attempt numbering. Those artifacts
and clarifications are now recorded below. Independent rereview remains
required before Slice approval.

The first T2.2 review found the retained archive evidence internally consistent
but correctly noted that this Plan still described Harness gates as pending.
Fast, Slice, and Stage gates had passed before that review; the stale Plan text
is corrected below. The gates must be rerun after this documentation correction
before final independent rereview.

## Completion evidence

Slice T2.1 attempt 3 created an owner-only detached candidate checkout at
`/tmp/group-t2.1-candidate.noRgIA` and owner-only evidence at
`/tmp/group-t2.1-evidence.U9Zxjd`. A direct linked-worktree creation attempt
was rejected because the isolated worker cannot write the primary repository's
`.git/worktrees` metadata, so the accepted checkout was created as a fresh
standalone local clone, detached at
`9b069d430cae02e74134f37edb8d05b83c2cc6c7`. Its `HEAD` and
`origin/master` both resolved to that SHA, and `git status --short` was empty
before verification.

`./scripts/verify all` exited `101` during compilation after the worker quota
was exhausted. The bounded log records `Disk quota exceeded` and `No space
left on device`; the checkout's generated `target/` occupied approximately
5.9 GiB. `git status --short` remained empty after the failure. Identity,
before/after status, the bounded verification log, outcome classification, and
SHA-256 evidence inventory remain under the evidence path above. No Harness
gate or independent review was run because the required candidate gate failed
and triggered the explicit stop condition. The disposable checkout and
evidence directory were retained for diagnosis and review.

After preserving the 48 KiB evidence directory, the Orchestrator removed only
the exact failed candidate path `/tmp/group-t2.1-candidate.noRgIA` with the
User's approval. The retry must use
`TMPDIR=/home/van-gogh/.cache/group-harness-tmp` and must not recreate its
candidate under `/tmp`.

The root-backed retry created the standalone clean candidate at
`/home/van-gogh/.cache/group-harness-tmp/group-t2.1-candidate.EF3Oka` and
evidence directory at
`/home/van-gogh/.cache/group-harness-tmp/group-t2.1-evidence.fSIbu6`. The
candidate `HEAD` and `origin/master` both resolved to
`9b069d430cae02e74134f37edb8d05b83c2cc6c7`, and `git status --short` was empty
before and after verification. The isolated Worker could not complete the
offline Genai loopback test because its sandbox denied server creation.

The Orchestrator then ran
`TMPDIR=/home/van-gogh/.cache/group-harness-tmp ./scripts/verify all` outside
the Codex sandbox in that exact candidate checkout. It exited `0` with
`verification mode 'all' passed`, including the full workspace and layered
MSRV suites. The candidate remained clean afterward. This is local host-side
evidence only; it is separate from the already-recorded successful GitHub-hosted
run for the same SHA.

The independently inspectable host artifacts are retained under
`/home/van-gogh/.cache/group-harness-tmp/group-t2.1-evidence.fSIbu6/host-validation/`:

- `candidate-head.txt` and `origin-master.txt` bind both Git references to the
  candidate SHA;
- empty `status-before.txt` and `status-after.txt` files prove clean status;
- `verify-all.log` contains the bounded command output and ends with
  `verification mode 'all' passed`;
- `verify-all.exit` records exit code `0`;
- `github-run-30848946632.json` is the retained output of
  `gh run view 30848946632 -R VanGogh-7/Group --json
  status,conclusion,headSha,jobs,url` and records the exact candidate SHA plus
  successful conclusions for both required jobs; and
- `evidence.sha256` validates all seven artifacts with `sha256sum -c`. Its own
  SHA-256 is
  `584de2486970c964a03ce6b607826d6a86dce6b3ed80556f0e4f79741a6b411e`.

Post-Human-Checkpoint Slice T2.1 attempt 2 independently rechecked the retained
evidence inventory; it immediately preceded the final attempt 3 completed
handoff and is not an earlier pre-reset attempt. Attempt 2 ran
with `sha256sum -c evidence.sha256`, confirmed that the detached candidate
`HEAD` and `origin/master` still match the exact candidate SHA, and confirmed
that `git status --short` and `git diff --check` remain clean. The configured
underlying fast checks, `harness _rust-affected-check` and
`./scripts/verify fast`, passed in the primary worktree. The Harness controller
could not start `harness run-gates --level fast` because the isolated worker
has read-only access to `.git/harness-control/state.lock`. A direct diagnostic
run of the remaining slice command, `cargo test --locked --workspace`, again
reached `group-agent-genai` and failed only because the sandbox denied creation
of the offline loopback server with `Operation not permitted`. These results
are not recorded as passing Harness gates. The Orchestrator must run and record
the fast and slice gates outside this restricted worker before independent
review.

Orchestrator-run Harness fast and Slice gates passed after the final completed
T2.1 Implementer handoff. Independent rereview approved the unchanged gated
worktree with no actionable findings, and the Orchestrator accepted Slice T2.1.

Slice T2.2 first created a clean standalone candidate at
`/home/van-gogh/.cache/group-harness-tmp/group-t2.2-candidate.rrG2nX` and
evidence directory at
`/home/van-gogh/.cache/group-harness-tmp/group-t2.2-evidence.qBht1C`. The
isolated Worker retained all eight successful package lists but stopped when
the first archive-generation command could not resolve crates.io DNS.

The Orchestrator then ran the retained `audit-script.py` against that exact
candidate outside the network-restricted sandbox. It generated all eight
archives without `--allow-dirty`, used only the release runbook's
non-persistent command-line patch layers for unpublished internal dependencies,
and performed no publication. Every package list matched its archive regular
files; safe extraction, normalized package metadata, exact `0.1.0` internal
versions without source overrides, README equality, byte-identical licenses,
forbidden-path screening, and filename-only secret-indicator screening passed.
The only indicator filename was `group-agent-genai/src/error.rs`; inspection
confirmed expected authentication error enum identifiers and no credential
value. The candidate remained clean before and after.

The complete 229-file evidence set is retained under
`/home/van-gogh/.cache/group-harness-tmp/group-t2.2-evidence.qBht1C/host-package-audit/`.
`sha256sum -c evidence.sha256` passes for every retained artifact. The
inventory SHA-256 is
`50ae96f1186d1ee8b5eb98f806632f6076a924d22e4262ef363f42da932979a1`;
the retained audit-script SHA-256 is
`b9f60689c5b3116f818f80d1ad0816cdaab81ca9ffa70bec2f6d7cf5003618a8`.
Archive SHA-256 values in fixed order are:

- `group-agent-core`: `fe3c6067e361568ca96ecaf0fb184fb43870424595c34cd48e74059329eccefc`
- `group-agent-model`: `dc14cc6a89bf379af2ba26b75d9a1412cf3b84a1f7385cc92058b26e87857601`
- `group-agent-tool`: `eec9046bbb7fa34eac06d381073908a261ae89222f72b2ca5b59b166a0d18bc4`
- `group-agent-checkpoint-sqlite`: `f745cd74a3963a5888f09245eebf440df9abce0d5cd1d8462f13418b2e86a2cf`
- `group-agent-observability-tokio`: `597432b9f4e054b55869dd0e6431cf498ed8dda96cc86e73b204287894518e12`
- `group-agent-genai`: `aa3e3f9fd524c09877dfa805f5e75256fc6911987e3ab88892b3aa376027aba5`
- `group-agent-mcp`: `f59a8dc753fb77a593a1cfeced6bbdd3dac611207e59f362252810b0fd813c4d`
- `group-agent-prebuilt`: `9b81a5e2ce9580dbaea604146dc573e8c5bfb78f86bbfacfd5b465df4d9df77c`

Harness fast, Slice, and Stage gates passed after the completed T2.2
Implementer handoff. After the Plan correction, all three levels passed again
against the final worktree. No tag, GitHub Release, registry authentication,
`cargo publish`, or fresh-consumer registry check was performed.

Slice T2.2 attempt 2 independently validated the retained host evidence without
regenerating or modifying it. `sha256sum -c evidence.sha256` passed all 229
entries. A separate read-only parser confirmed candidate `HEAD` and
`origin/master`, clean status, fixed crate order, archive hashes, safe regular
file inventories equal to the saved package lists, required package metadata,
exact internal `0.1.0` dependency versions without source overrides, README
and license equality, and forbidden-path screening. It reproduced only the
recorded `group-agent-genai/src/error.rs` indicator filename and classified its
two matches as expected API-key error identifiers; the access-token, bearer
header, and private-key indicators had zero matches. The candidate and evidence
directories remain retained at their recorded owner-only paths for Harness
gates and independent review; no cleanup was performed in this attempt.

Slice T2.2 attempt 3 repeated the independent validation after the Plan's gate
status correction. `sha256sum -c evidence.sha256` again passed all 229 entries;
the candidate `HEAD` and `origin/master` still matched the exact candidate SHA,
and its status and diff remained clean. The read-only archive parser rechecked
all eight archives and 190 regular files, including safe paths, package-list
equality, required normalized metadata, exact internal `0.1.0` edges without
source overrides, README and license equality, forbidden paths, archive hashes,
and the single expected filename-only indicator disposition. `git diff --check`
passed for the primary worktree. No archive was regenerated and no cleanup was
performed. The corrected worktree is ready for Orchestrator-run fast, Slice,
and Stage gates followed by final independent read-only review.

The final T2.2 independent Reviewer approved the unchanged gated worktree with
no findings. It independently verified the candidate identity and cleanliness,
all 229 retained evidence hashes, eight archives containing 190 regular files,
13 exact normalized internal dependency edges, safe paths, package-list
equality, archive hashes, README and license equality, and the documented
`group-agent-genai/src/error.rs` indicator disposition. Harness then accepted
Stage T2 as completed on 2026-08-04.
