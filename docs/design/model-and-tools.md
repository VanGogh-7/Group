# Model and Tools Design

Model defines provider-neutral data and calls. Tool owns execution policy.
Provider and MCP adapters depend on these layers; the layers do not depend
back on adapters.

## Model domain

`group-agent-model` owns:

- strongly typed System, User, Assistant, and Tool messages;
- ordered text and Tool content;
- Tool definitions, calls, results, choices, and call identifiers;
- chat requests and responses;
- model identity and capabilities;
- partial usage and finish reasons;
- continuation `Extensions`;
- typed model errors;
- non-streaming and streaming model ports.

It does not execute Tools, perform network requests, depend on Core, or own a
Tokio runtime.

## Validated facade

Applications call `ChatModel`, not a raw adapter method with an unchecked
request. The facade:

1. validates request structure and metadata;
2. validates common capabilities;
3. validates streaming-specific capability when applicable;
4. constructs `ValidatedChatRequest`;
5. dispatches exactly once to the adapter.

The validated wrapper has no public constructor or conversion path that would
allow ordinary callers to bypass validation. Raw adapter implementations can
inspect it through its public accessors.

## Stream collector

`ChatStreamCollector` accepts normalized model events and produces one
validated `ChatResponse`.

It provides:

- stable text and ToolCall order;
- bounded sparse ToolCall indices;
- incremental JSON argument completion;
- atomic delta validation and commit;
- partial cumulative Usage merging;
- idempotent continuation Extension merging;
- a permanent Failed state after the first error;
- mandatory logical finish.

An invalid event is not partially committed. Once the prefix can no longer be
trusted, the collector does not recover and synthesize success.

## Tool definitions and execution

`group-agent-tool` owns the object-safe local `Tool` port and the immutable
execution Registry.

Registration validates Tool identity, behavior, and definition, then compiles
one JSON Schema validator. Runtime reuses the compiled validator and validates
arguments before invoking the Tool.

`ToolBehavior` makes side effects explicit:

- `ReadOnly`;
- `IdempotentWrite`;
- `NonIdempotentWrite`.

Non-idempotent writes are serialized by default. An idempotency key is a value
passed to an implementation contract; ToolRuntime does not provide durable
deduplication or exactly-once behavior.

## Timeout and Future ownership

ToolRuntime uses the caller's Tokio runtime. It creates no runtime, detached
task, or channel. A per-call timeout drops the Tool Future. Dropping a single
or batch Runtime Future drops pending Tool Futures.

Future drop cannot undo side effects that occurred before the Future was
dropped. Runtime therefore does not relabel every unobserved call as safely
cancelled.

## Batch behavior

Batches are bounded, spawn-free, and deterministic at the report boundary.
Calls may finish in any order, but reports and ToolMessages return in input
order.

Collect-all continues scheduling within concurrency and side-effect limits.
Fail-fast means:

1. observe the first failure;
2. stop scheduling new calls;
3. continue polling every started call to its real outcome;
4. mark only never-started calls `NotStartedDueToFailFast`.

This preserves execution facts for non-idempotent effects.

## Observer contract

`ToolEventSink` is synchronous, fallible, and invoked outside Registry locks.
Callbacks must be lightweight.

- a failure or panic while observing `ExecutionStarted` prevents execution;
- terminal observer failure is a secondary diagnostic;
- terminal failure never replaces the already determined Tool success,
  failure, or timeout;
- panic payloads are not retained or formatted.

`ToolExecutionReport` exposes the primary result and optional secondary
observer diagnostic.

## ToolMessage identity

`ToolResult` is payload-domain data and does not independently invent a call
identity. Message helpers combine the original `ToolCallId` with the result to
produce a valid ToolMessage. Batch helpers preserve that association and input
order.

Runtime infrastructure failure remains an error; it is not converted to a
fake business ToolMessage.

## Adapter composition

Genai maps Model data to one provider SDK. MCP maps remote tools into the Tool
trait. Neither adapter reimplements Tool Registry, validation, timeout, batch,
fail-fast, or ToolMessage pairing.

The application owns the Agent loop that sends assistant ToolCalls to
ToolRuntime, appends ToolMessages, and asks ChatModel for the next response.
That orchestration is not currently a prebuilt Group component.

## Direct evidence

- `crates/group-agent-model/src/model.rs`
- `crates/group-agent-model/src/stream.rs`
- `crates/group-agent-model/src/extensions.rs`
- `crates/group-agent-model/tests/`
- `crates/group-agent-tool/src/tool.rs`
- `crates/group-agent-tool/src/registry.rs`
- `crates/group-agent-tool/src/runtime.rs`
- `crates/group-agent-tool/src/event.rs`
- `crates/group-agent-tool/tests/tool_runtime.rs`

Related decisions:

- [ADR-005](../adr/005-validated-model-facade.md)
- [ADR-006](../adr/006-tool-runtime-policy.md)

