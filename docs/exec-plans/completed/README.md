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
