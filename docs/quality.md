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

The following block a responsible public v0.1.0 release:

- internal path dependencies do not also declare publishable versions;
- package repository, homepage, and readme metadata is incomplete;
- the repository has no checked-in LICENSE text matching the manifest;
- CI is not established;
- production guidance does not yet fully enforce filtering of upstream
  `genai` and `rmcp` logging targets;
- the SQLite restart benchmark includes temporary-directory teardown noise in
  its measured region;
- all crates have not passed a final ordered `cargo package` release gate;
- no v0.1.0 release tag has been prepared.

H-001 documents these items; it does not fix Cargo metadata, licensing, CI, or
benchmark code because those are outside this documentation migration.

## Stage 21 relationship

Stage 21 is active and In Progress. Its implementation slices provide the
experimental Prebuilt Agent, offline example and doctests, and benchmark-build
coverage without changing the reviewed stable Core, Durable, Model, or Tool
contracts. Final independent review and Plan closure remain outstanding; this
is not a production-readiness claim.

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
systems must filter sensitive data and avoid unreviewed `genai=trace` or
`rmcp=debug` output.

## Updating this ledger

Update this document when:

- a release blocker is fixed or discovered;
- a public surface changes stability class;
- a required validation gate changes;
- an architecture review changes the repository-level assessment.

Product capability history belongs in
[Stages 01-20](history/stages-01-20.md), not here.
