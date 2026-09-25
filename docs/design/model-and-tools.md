# Model and Tools Design

Model defines provider-neutral data and calls. Tool owns execution policy.
Provider and MCP adapters depend on these layers; the layers do not depend
back on adapters.

## Optional structured output

With `structured-output`, construct `StructuredOutput::new(name, schema)` and
attach it with `ChatRequest::with_structured_output`. Metadata must explicitly
advertise `ModelCapabilities::with_structured_output(true)`. Unsupported
requests fail before dispatch. The facade validates complete responses and
holds streaming `Finished` until raw stream EOF and successful validation.
Text deltas are provisional. Invalid output produces one error then permanent
EOF; dropping the wrapper drops the raw stream. No retry or JSON repair occurs.

The closed `group-json-output/1` profile requires a root object, every property
required, and `additionalProperties: false` on each object. It admits nested
objects, arrays, primitive types, nullable types, string enums and descriptions.
Other keywords, references and composition are rejected. Limits are 64 KiB
encoded schema, depth 8, 256 properties, 256 enum entries, and 16 KiB combined
property-name/description/enum-string bytes. Names are 1-64 ASCII alphanumeric,
underscore or hyphen bytes. Every response round, including Tool commentary,
is limited to 1 MiB text. Final JSON rejects duplicate keys and depth over 64.

Intermediate ToolCalls responses retain Tool semantics. A final response must
finish with Stop and satisfy the contract. `ValidatedJsonOutput::deserialize<T>`
is local Rust extraction with a distinct typed error; it cannot rerun a model
or Tool. Schema validity does not establish business truth. Error formatting
hides payloads; explicitly traversing underlying source errors can expose them.
See [Stage 22](../specs/022-structured-output.md) for exact identity and limits.


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

Tool observers can be composed with `ToolRuntime::with_additional_event_sink`.
Installation order determines delivery order. A start callback failure prevents
Tool execution and stops delivery to later observers. Terminal delivery reaches
all observers and retains the first failure as the existing secondary diagnostic.
Each panic remains independently classified by ToolRuntime. `with_event_sink`
continues to replace all observers; composition on a clone does not alter the
original runtime.

## Experimental prebuilt Tool-calling Agent

`group-agent-prebuilt` provides a provider-neutral `ToolCallingAgent`
above the stable Core, Model, and Tool layers, supporting both standard
run-to-completion invocation and real-time streaming execution. The
application injects an already constructed `ChatModel`, an already constructed
`ToolRuntime`, and caller-owned messages. A private Core graph then repeats
Model -> Tool -> Model until a response has no ToolCalls (`FinalAnswer`) or the
configured committed-model-round limit ends after a complete Tool batch
(`MaxRounds`).

Each Model request owns the current canonical transcript and the complete
immutable ToolDefinition snapshot. ToolCalls, rather than provider-specific
finish-reason conventions, control routing. ToolRuntime remains the only
scheduler and provides schema validation, timeout, bounded batching,
fail-fast/side-effect policy, ToolMessage identity, and the shared execution
boundary for local and MCP-backed Tools. A business Tool error becomes a real
model-visible ToolMessage; batch or per-call infrastructure failure stops the
Agent and exposes the complete current batch report when available.

Construction compiles the private graph once. Invocation creates fresh State,
does not spawn one task per call, and performs no hidden retry. Per-round usage
remains aligned rather than merged as though independent responses were one
cumulative stream.

Streaming execution is provided via `agent.stream(...)` (returning an
asynchronous `AgentEventStream`) and `agent.invoke_with_stream_sink(...)`
(dispatching synchronously to an `AgentEventSink`). When streaming is active,
the internal ModelNode invokes `ChatModel::stream` and accumulates the response
using `ChatStreamCollector` while emitting `AgentStreamEvent` lifecycle events:
`ModelStarted`, `TextDelta`, `ToolCallDelta`, `ModelCompleted`, `ApprovalRequired`,
`ToolStarted`, `ToolCompleted`, `Completed`, and durable `Interrupted`. Debug logs redact token and argument
payloads, reporting only byte/item counts. Polling the stream drives graph
execution directly in the caller's task without background task spawning, and
dropping the stream or invocation future drops locally owned Model and Tool
futures. It does not prove that remote side effects stopped.

Each provider event passes `ChatStreamCollector` validation before its delta is
published. Streaming composes Agent lifecycle delivery with the supplied Tool
observer and preserves its start-failure and terminal-diagnostic behavior.
`ToolCompleted { is_error: true }` covers a started call's business error or
execution failure; infrastructure failures additionally return typed `AgentError`.
Rejected calls that never started produce no execution lifecycle. Cancellation,
graph timeout, or dropping a pending invocation need not produce a terminal
Tool event because the Tool future never returned an outcome.

Plain `stream` and `invoke_with_stream_sink` remain non-durable. With Tool approval
enabled, `ApprovalRequired` is followed by the typed non-durable interrupt error.
Use `stream_with_checkpoint` or `invoke_with_checkpoint_stream_sink` (including
their control variants) for checkpointed streaming and resumable approval.

`ApprovalRequired` is a provisional notification before saving. Successful
interrupt persistence emits the terminal `Interrupted(AgentInterrupted)` event,
whose thread/checkpoint identity and approval request define the durable handoff.
An interrupt save failure returns a typed error without an `Interrupted` event
or Tool execution. Normal `Completed` is also emitted only after required saves.
Model and Tool events can precede State commit; receiving a delta is not evidence
that its round is recoverable. Event delivery itself is not durable.

`resume_stream` and `resume_with_stream_sink` accept the existing
`ResumeConfig<AgentSnapshot>`, including Core controls, events, branch selection,
and the one-attempt `AgentApprovalDecision`. Prebuilt uses Core's generic
`resume_with_state_initializer` to attach the new sink after restore; it is not
part of the snapshot and is isolated across concurrent and nested calls. Existing
ordinary and streaming checkpoints are interchangeable within the same approval
graph version. Model/Tool events cover newly executed work only. A completed
checkpoint emits one terminal `Completed` without Model/Tool calls or a new save.
Each asynchronous stream ends after its successful terminal event or one typed
error following already buffered events. Sink APIs return the corresponding
`AgentRunOutcome` or error. No automatic retry or token-history replay occurs.
Streaming Replay and Fork are not provided.

The Prebuilt API is experimental. Private State, Update, Nodes, routers,
topology, and `CompiledGraph` are not public extension points. Durable
execution is opt-in through Core checkpoint ports and the crate-owned canonical
JSON `AgentSnapshotCodec`. Opt-in Tool approval durably suspends before any
Tool side effect with an `AgentApprovalRequest` payload and resumes with a
single-attempt `AgentApprovalDecision`: approve executes the pending batch,
while reject commits business-error ToolMessages and the loop continues.
Provider construction, MCP lifecycle, fallback/retry, rollback, exactly-once,
structured output, Memory/RAG/PDF/OCR, Multi-Agent, and middleware are not
implemented here. Provider adapters, MCP session setup and Tool registration,
persistence, product prompts/policy, RAG, Memory, and UI remain
application-owned.

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
- `crates/group-agent-prebuilt/src/`
- `crates/group-agent-prebuilt/src/stream.rs`
- `crates/group-agent-prebuilt/src/durable_stream.rs`
- `crates/group-agent-prebuilt/tests/streaming.rs`
- `crates/group-agent-prebuilt/tests/durable_streaming.rs`
- `crates/group-agent-prebuilt/tests/durable_streaming_failures.rs`
- `crates/group-agent-prebuilt/tests/durable_streaming_isolation.rs`
- `crates/group-agent-prebuilt/tests/durable_streaming_sqlite.rs`
- `crates/group-agent-prebuilt/examples/tool_calling_agent.rs`
- `crates/group-agent-prebuilt/examples/streaming_agent.rs`
- `crates/group-agent-prebuilt/examples/durable_streaming.rs`

Related decisions:

- [ADR-005](../adr/005-validated-model-facade.md)
- [ADR-006](../adr/006-tool-runtime-policy.md)
