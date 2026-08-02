# Group Documentation

This index routes readers to one authoritative document for each concern.
Code and executable tests remain the final source of truth.

## Start here

- [README](../README.md): five-minute user entrypoint.
- [Architecture](../ARCHITECTURE.md): current cross-crate structure and
  stability boundaries.
- [Quality and Release Status](quality.md): current debt, experimental
  surfaces, and release readiness.

## Stable design

- [Core Runtime](design/core-runtime.md)
- [Durable Execution](design/durable-execution.md)
- [Model and Tools](design/model-and-tools.md)
- [Error, Cancellation, and Observability](design/error-cancellation-observability.md)

These documents describe current behavior. They do not replace direct tests.

## Experimental composition

- [`group-agent-prebuilt` Tool-calling Agent](design/model-and-tools.md#experimental-prebuilt-tool-calling-agent)

Prebuilt composes stable Core, Model, and Tool boundaries but does not make its
private graph topology a public extension point. Its current public API remains
experimental.

## Adapters

- [Genai Adapter](adapters/genai.md)
- [MCP Adapter](adapters/mcp.md)

Adapter documents include current upstream-version, protocol, and lifecycle
limits. Genai and MCP public configuration surfaces remain experimental.

## Decisions

- [ADR Index and Format](adr/README.md)

ADRs explain why high-load architecture choices exist. They are not a stage
log and do not duplicate every historical correction.

## Execution Plans

- [Execution Plan Guide](exec-plans/README.md)
- [Execution Plan Template](exec-plans/TEMPLATE.md)
- [Active Plans](exec-plans/active/README.md)
- [Completed Plans](exec-plans/completed/README.md)

Complex, cross-layer, concurrent, durable, protocol, public-API, and
multi-session work must be driven by a tracked active plan.

## Runbooks

- [Development](runbooks/development.md)
- [Independent Review](runbooks/review.md)

The runbooks define how implementation and independent review exchange
evidence.

## History

- [Stages 01-20](history/stages-01-20.md)

History preserves the major capability and correction sequence. Current
contracts belong in Architecture, design, adapter, and ADR documents.
