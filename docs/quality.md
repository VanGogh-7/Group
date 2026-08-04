# Quality and Release Status

This file is the current quality ledger. It records known debt and release
readiness without turning those items into product stages.

## Current architecture review result

The full repository architecture review found no blocking responsibility
inversion, dependency cycle, durable data-integrity defect, cross-layer
control conflict, or structural Core hot-path regression.

Compatibility-first base APIs:

- Core State, Node, compiled graph, control, event, and error semantics;
- durable Record, Codec, Store, CAS, Resume, Replay, Fork, and branch lineage;
- Model messages, requests, responses, validated facade, collector, and
  extensions;
- Tool trait, behavior, Registry, Runtime, execution reports, observer, and
  ToolMessage helpers.

Experimental surfaces:

- Genai provider-specific configuration and extension keys;
- stable-target policy and provider compatibility mapping;
- MCP transport constructors and discovery configuration;
- any future MCP HTTP or OAuth interface;
- the `group-agent-prebuilt` public API and its current private graph topology.

The repository contains an experimental non-streaming prebuilt Tool-calling
loop over the stable Core, Model, and Tool boundaries. This capability does not
make Prebuilt stable or make the repository production-ready.

## Release blockers

The exact clean candidate
`9b069d430cae02e74134f37edb8d05b83c2cc6c7` passed `./scripts/verify all` and
the eight-archive Phase 2 package audit. All archives were generated without
`--allow-dirty` in dependency order and passed package-list, safe-extraction,
regular-file, normalized-manifest, internal-edge, README, byte-identical
license, intended-content, and filename-only secret-indicator checks. The one
indicator hit, `group-agent-genai/src/error.rs`, contains expected
authentication error identifiers and no credential value. The candidate
remained clean; non-persistent command-line patches were not written to any
manifest.

GitHub Actions run `30848946632` completed successfully for that exact SHA.
Both `Full workspace (Rust 1.88)` and `Layered MSRV (Rust 1.85 and 1.88)`
succeeded. This establishes hosted CI for the bounded candidate, not a release
or crates.io-resolution result.

The following still block a responsible public v0.1.0 release:

- no v0.1.0 release tag has been separately authorized or prepared;
- the eight crates have not been published in dependency order or verified in
  the crates.io index; and
- no fresh consumer has resolved and built all eight exact `0.1.0` versions
  from crates.io.

Plan 022 completed the metadata, portable license, internal path-plus-version,
SQLite benchmark teardown, production logging guidance, release procedure, and
diagnostic dirty-tree preflight work. Plan 023 records the clean candidate,
hosted CI, full verification, and clean eight-archive evidence. Neither Plan
authorizes a tag, GitHub Release, `cargo publish`, or registry credential use.
See the [Release Runbook](runbooks/release.md) for the separately authorized
tag, publication/index, and fresh-consumer gates.

## Stage 21 relationship

Stage 21 is completed. It delivered the experimental Prebuilt Agent, offline
example and doctests, and benchmark-build coverage without changing the
reviewed stable Core, Durable, Model, or Tool contracts. Its independent review
was accepted, and its Plan moved to completed plans. This is not a
production-readiness claim.

## Validation expectations

Primary commands:

```bash
./scripts/verify fast
./scripts/verify full
./scripts/verify msrv
```

Test behavior is offline: provider coverage uses local fixtures, loopback HTTP,
duplex transports, or local child processes rather than live quota. Cargo may
still download dependencies fixed by `Cargo.lock` when its local registry or
cache is missing. Set `GROUP_VERIFY_OFFLINE=1` to require cached dependencies
and prohibit Cargo network fallback.

Performance claims require repeatable benchmarks. `cargo bench --workspace
--no-run` is a build gate; actual comparative claims require measured runs and
documented environment details.

## Logging and secrets

Group-owned default formatting is payload-safe, but full source-chain logging
and upstream dependency targets are application-controlled. Production
systems should start from the fail-closed
[Production tracing policy](design/error-cancellation-observability.md#production-tracing-policy).
It disables upstream `genai` and `rmcp` targets until the application has
audited the exact environment and every sink; Group does not promise universal
redaction of upstream events or already-formatted source chains.

## Updating this ledger

Update this document when:

- a release blocker is fixed or discovered;
- a public surface changes stability class;
- a required validation gate changes;
- an architecture review changes the repository-level assessment.

Product capability history belongs in
[Stages 01-20](history/stages-01-20.md), not here.
