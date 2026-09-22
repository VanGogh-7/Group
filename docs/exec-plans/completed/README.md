# Completed Execution Plans

This directory is the durable record of completed complex repository work.

A completed Plan includes:

- final Status;
- checked implementation slices and acceptance criteria;
- commands actually run and results;
- decision log;
- independent review conclusion and findings;
- remaining risks or explicitly assigned follow-up.

Completion does not make historical statements authoritative over current
code, tests, architecture, or quality documentation.

## Plans

- [H-001 Repository Harness Migration](000-repository-harness-migration.md) -
  established the documentation, planning, review, and verification Harness.
- [H-001.1 Repository Harness Corrections](001-repository-harness-corrections.md)
  - corrected MSRV guidance, verification portability, and the independent
    review lifecycle.
- [Stage 21 - Prebuilt Tool-calling Agent](021-prebuilt-tool-calling-agent.md) -
  added the experimental provider-neutral Prebuilt Agent composition, offline
  evidence, Rust 1.85 gates, and accepted independent review.
- [022 v0.1.0 Local Release Preparation](022-v0.1.0-release-readiness.md) -
  completed bounded local release preparation with accepted independent review;
  hosted CI, a clean committed candidate, and remote release gates remain
  blocked.
- [023 Clean v0.1.0 Release-Candidate Verification](023-clean-v0.1.0-release-candidate-verification.md)
  - verified the exact clean candidate, successful hosted CI, full local gates,
    and all eight package archives with accepted independent review; tag,
    publication/index, and fresh-consumer gates remain separately authorized.
- [024 Finalize v0.1.0 Release Content and Candidate Identity](024-finalize-v0.1.0-release-candidate.md)
  - finalized the immutable release candidate, completed migrated Harness 1.0
    gates and independent review, and retained tag and publication as separately
    authorized operations.
- [026 Review corrections](026-review-corrections.md) - fixed terminal stream
  behavior, rejected unsupported MCP key requirements, reclaimed weak cache
  entries, and passed full/MSRV verification with independent review.
- [027 Unify the workspace MSRV](027-unify-msrv.md) - unified all crates at
  Rust 1.88 with stable quality gates, complete MSRV verification, and review.
- [028 Prebuilt agent durability](028-prebuilt-durability.md) - added opt-in
  durable execution (checkpointed invoke, Resume, Replay, Fork) for the
  experimental Prebuilt ToolCallingAgent with accepted independent review.
- [029 Prebuilt agent tool approval](029-prebuilt-tool-approval.md) - added
  opt-in durable human approval before Tool side effects with accepted review.
- [030 Prebuilt agent streaming](030-prebuilt-streaming.md) - added streaming
  events, preserved caller observers through additive Tool composition, and
  completed protocol/lifecycle corrections with full gates and independent PASS.
- [031 Durable streaming and resumable Tool approval](031-durable-streaming-approval.md)
  - integrated checkpointed streaming and approval Resume, with explicit transient
    State initialization, full stable/MSRV gates, and independent PASS review.
- [032 Reliable provider tool-call streaming](032-provider-tool-streaming.md)
  - added opt-in native OpenAI Chat tool streaming, bounded protocol validation,
    and HTTP-to-SQLite approval recovery with full gates and independent PASS.
