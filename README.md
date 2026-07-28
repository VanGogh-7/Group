# Group

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. Its
name borrows the metaphor of a hierarchy of algebraic structures: Group is
intended to be the execution foundation beneath higher-level agent frameworks.
It does not claim to implement a mathematical group.

## Current stage

Stage 19.3 finalizes the independent `group-agent-mcp` client adapter, fixed to
`rmcp = 2.2.0` with default features disabled. One reusable MCP session
discovers every `tools/list` page with cursor-cycle and traversal limits, maps
only a complete result into an immutable `group-agent-tool` Registry snapshot,
and dispatches validated calls over child-process stdio or an injected async
read/write transport. Explicit stdio shutdown closes, bounds, kills when
needed, and reaps the direct child before publishing either success or failure
through one Session-owned completion that survives waiter cancellation. Drop
keeps only a best-effort runtime-independent termination fallback. Tool Runtime
continues to own schema validation, timeout, batch scheduling, side-effect
policy, fail-fast draining, and Tool-message pairing. MCP adds no
Core/Model/Tool reverse dependency, retry, exactly-once claim, HTTP, OAuth,
credential storage, sandbox, Resources, Prompts, Sampling, Roots, or Agent
loop.

Stage 18.1 hardens the independent `group-agent-tool` crate with concrete Schema
sources, stop-scheduling-and-drain fail-fast, panic-safe before/after observer
semantics, and call-ID-safe Tool messages.

Stage 17 added the independent `group-agent-genai` crate, fixed to `genai`
0.6.5. It maps the Stage 16.2 validated model boundary to an
application-injected `genai::Client`, including messages, tools, generation
controls, non-streaming responses, online stream normalization, partial usage,
thought-signature and response-ID continuation, and source-preserving errors.
Stage 17.1 keeps audited OpenAI Chat text-only streaming available while
rejecting OpenAI Chat tool streaming and all OpenAI Responses streaming before
network dispatch under genai 0.6.5.
The adapter reads no environment or credentials, creates no runtime, channel,
forwarding task, retry loop, or hidden session state. Stage 15.2 remains
the current Core checkpoint foundation: Resume continues the latest head of
either the default lineage or a selected branch; Replay remains strictly
read-only; Fork creates a new `BranchId` from one exact historical checkpoint.
The Stage 10.1 Record/Codec/content-idempotency contract remains unchanged:

```text
START -> prepare -> [local_search, web_search] -> synthesis -> END
                  conditional router -> [selected targets, END]
                  successful boundary -> Checkpoint
                  node interrupt -> Interrupted Checkpoint -> Resume value
historical Checkpoint -> read-only Replay -> no checkpoint writes
historical Checkpoint -> explicit Fork(BranchId) -> independent branch head
START -> prepare -> research.{search -> verify} -> answer -> END
```

Every node in one parallel frontier inspects the same immutable state snapshot.
The Runtime waits for the whole frontier, orders successful updates by compiled
node order, commits them through `GraphState::apply_batch`, and only then
calculates the next frontier. Sequential one-node frontiers continue to use
`GraphState::apply`.

Checkpoint-enabled invocations create a user-defined state snapshot only after
a super-step has committed and resolved its complete next frontier. Normal
`invoke` calls do not create snapshots, call storage, or take checkpoint locks.

The current workspace includes:

- an independent provider-neutral chat model abstraction with a cheaply cloned
  validated `ChatModel` facade over one `Arc<dyn ChatModelAdapter>`;
- a public but externally non-constructible `ValidatedChatRequest` raw-adapter
  boundary;
- strongly typed messages, tool definitions/calls/results, generation controls,
  model capabilities, metadata, token usage, and finish reasons;
- an independent local Tool Runtime with object-safe tools, cached JSON Schema
  validation, explicit side-effect behavior, and redacted typed errors;
- deterministic bounded Tool batches with collect-all or fail-fast policy,
  stable input-order results, and conservative non-idempotent-write scheduling;
- an independent MCP 2.2.0 client adapter with paginated Tool discovery,
  immutable Registry snapshots, conservative remote behavior, stdio lifecycle,
  content-policy enforcement, and source-preserving redacted errors;
- a provider-neutral stream-event protocol and reusable validating response
  collector;
- a separate `genai` 0.6.5 chat adapter with injected authentication and
  endpoint configuration;
- offline loopback HTTP coverage for non-streaming, SSE, continuation, Group
  Node integration, timeout, cancellation, and Future-drop ownership;
- asynchronous trait-based nodes;
- fixed edges, static and conditional fan-out, fan-in barriers, and conditional
  target whitelists;
- concurrent node futures without per-node task spawning;
- explicit, deterministic parallel state-update merging;
- opt-in snapshots and asynchronous replaceable checkpoint storage;
- storage-neutral `CheckpointRecord` values and explicit Snapshot/payload codecs;
- record-backed, thread-safe `InMemoryCheckpointer`;
- a production SQLite `CheckpointStore` adapter built on SQLx with embedded
  migrations, WAL defaults, transactional idempotency, and lineage CAS;
- latest and ordered history queries with CAS-protected checkpoint lineage;
- restoration of state, frontier, cumulative step, and super-step position;
- explicit graph-version compatibility and latest-only resume checks;
- read-only replay from an explicit historical checkpoint without lineage
  writes or implicit Fork;
- explicit forks from historical checkpoints with independent branch
  head/history queries and CAS-protected branch continuation;
- typed interrupt payloads and resume values without Serde bounds;
- interrupted checkpoints and completed-or-interrupted execution outcomes;
- shared-state `CompiledGraph<S>` mounting through `add_subgraph`;
- structured `GraphPath` and `NodePath` namespaces for nested execution;
- subgraph-aware events, errors, checkpoints, resume, and interrupts;
- explicit loops protected by a per-run `max_steps`;
- immutable, reusable, concurrently shareable compiled graphs;
- immediate lifecycle delivery through a thread-safe `EventSink`;
- an optional bounded Tokio broadcast adapter with explicit per-subscriber lag;
- independent full or disabled event retention for successful run reports;
- cooperative cancellation through Tokio Util `CancellationToken`;
- optional run-level and per-node Tokio deadlines;
- typed `RunFailed` events and per-invocation `RunId` values;
- ordered successful run reports and extensible lifecycle events;
- source-preserving structured errors;
- topology, edge-shape, whitelist, and reachability validation.

## Provider-neutral model boundary

`group-agent-model` has no dependency on `group-agent-core`, and Core has no
dependency on the model crate. Applications opt into both:

```text
application
├── group-agent-core
├── group-agent-model
├── group-agent-tool
│   └── group-agent-model
├── group-agent-mcp
│   ├── group-agent-model
│   ├── group-agent-tool
│   └── rmcp = 2.2.0
└── group-agent-genai
    ├── group-agent-model
    └── genai = 0.6.5
```

The model crate represents `Message::{System, User, Assistant, Tool}` explicitly
so a tool result cannot masquerade as user input. `ContentPart` currently
supports ordered text only; `as_text` returns `Option<&str>` so future non-text
parts do not need a fake empty string. Empty text is valid and is especially
useful for tool-only assistant turns. Text helpers concatenate only text parts
in order. `AssistantMessage` can contain text and zero or more `ToolCall`
values together. `ToolCallId` provides the stable link to
`ToolMessage` and `ToolResult`. `ToolDefinition` contains only a name,
description, and provider-neutral JSON Schema. These types remain data only
inside `group-agent-model`; execution is an opt-in responsibility of the
separate `group-agent-tool` crate.

`ChatRequest::validate` checks non-empty messages, finite provider-neutral
generation controls, stop sequences, duplicate tool definitions, named tool
choices, unique call identifiers, and earlier-call tool-result references.
`top_p` accepts the inclusive common range `[0, 1]`; temperature accepts finite
non-negative values without imposing one provider's maximum. Empty stop lists
and duplicate non-empty stops are allowed, and the common layer sets no
provider-specific count limit.

Provider-specific values use the ordered, validated `Extensions` container.
Keys are trimmed, empty and duplicate insertion is rejected, iteration order is
stable, and `Debug` reveals keys but not values. `AssistantMessage`,
`ToolCall`, `TokenUsage`, requests, responses, and stream metadata can preserve
provider-neutral data not otherwise modeled. No provider-specific key or SDK
type is defined here.

Applications call the cheaply cloned `ChatModel` facade. Provider adapters
implement the object-safe `Send + Sync` `ChatModelAdapter` raw port and are
wrapped once:

```rust
use std::sync::Arc;

use group_agent_model::{
    ChatModel, ChatModelAdapter, ChatRequest, Message,
};

async fn complete(
    adapter: Arc<dyn ChatModelAdapter>,
) -> Result<String, Box<dyn std::error::Error>> {
    let model = ChatModel::new(adapter)?;
    let response = model
        .complete(ChatRequest::new(vec![Message::user("Hello")]))
        .await?;
    Ok(response.message().text_content())
}
```

Facade construction validates and snapshots `ModelMetadata`. Cloning a facade
shares both the adapter and immutable metadata snapshot through `Arc`; it does
not rebuild or copy adapter state. In particular, parallel tool calls cannot be
declared without tool calling. Every public call then follows one
non-overridable order:

```text
ChatRequest::validate
-> ModelMetadata / ModelCapabilities validation
-> Streaming capability validation for stream()
-> privately construct ValidatedChatRequest
-> complete_raw or stream_raw
```

Tool definitions, tool selection, or tool-call/result history require
tool-calling support; a true `parallel_tool_calls` request requires both
tool-calling and parallel-tool-call support; streaming adds the streaming
capability check. Rejected requests never enter adapter code.
Structured-output capability is intentionally absent until a provider-neutral
request field exists.

`ValidatedChatRequest` is public only so an independent Provider crate can use
it as a `ChatModelAdapter` method parameter. Its fields and constructor are
private, and there is no public `new`, unchecked constructor, or conversion
from `ChatRequest`. Adapters use read-only `request`, `messages`, `tools`,
`tool_choice`, `generation`, and `extensions` accessors, or consume it with
`into_inner`. Applications continue to call only `ChatModel::complete` and
`ChatModel::stream`.

Stream initialization can fail asynchronously, and every later stream item is
also a `Result`. The provider-neutral events are `ResponseStarted`,
`TextDelta`, `ToolCallDelta`, `Usage`, and `Finished`. Tool-call deltas use a
stable bounded index plus optional ID/name and append-only argument fragments.
`ChatStreamCollector` stores sparse indices in a map, preserves text order,
sorts completed calls by index, parses arguments as JSON only at completion,
and retains idempotently merged per-call Extensions. `ResponseStarted` appears
at most once and carries optional response ID, actual model, and response
extensions; if absent, those response fields remain `None`.

Every `push` validates the complete event before committing any response data.
ID/name conflicts, argument or text limits, extension limits/conflicts, Usage
errors, duplicate starts, and invalid Finished events therefore cannot leave a
partial fragment behind. The first error while Active permanently changes a
manual collector to Failed; later events and `finish` return
`CollectorAlreadyFailed`. A successful Finished event first validates complete
ToolCall identity and JSON, changes the collector to Finished, and causes every
later event to return `EventAfterFinished`.

`Usage` is a cumulative snapshot, not an increment. Input, output, and total
tokens are independently optional. A `Some` field updates its prior value,
`None` does not clear known data, cumulative values cannot decrease, and
extension conflicts fail the protocol. Input plus output is computed with
checked addition when both exist. An explicit total may be larger than the
common sum but cannot be smaller than known accounting. Partial usage is kept
in the final response. `TokenUsage::merge_snapshot` validates all counters,
total consistency, and every extension conflict before updating anything. It
then updates counters and moves only new extension values into the existing
ordered map; it does not clone the accumulated Extensions.

The collector requires exactly one logical `Finished` and rejects every later
event. EOF without `Finished`, duplicate start, incomplete tool-call
identity, repeated or conflicting tool fields, conflicting extensions,
decreasing usage, excessive sparse indices, and invalid JSON return structured,
source-preserving errors. `collect_chat_stream` stops polling on the first
stream-item or collector error. Manual `ChatStreamCollector` use and
`collect_chat_stream` are alternatives; the helper creates its own collector.

`ModelError` separates invalid requests, unsupported capabilities,
authentication, permission, rate limit, provider availability, timeout,
protocol, decode, cancellation, and other failures. It retains concrete source
errors plus optional provider/model, HTTP status, retry-after, and retryability
metadata. The crate classifies retryability but implements no retry policy.
`Debug` and `Display` for `ModelError` do not echo its adapter-supplied message;
callers can inspect `as_message` only through an explicit access.

All content-bearing public data types use redacted `Debug`: variant names,
identifiers, counts, byte lengths, numeric usage, and extension keys remain
visible, while prompts, tool arguments/results, schemas, stream fragments,
extension values, and raw response content do not.

Cancellation is the caller-owned Future-drop boundary. The model crate does not
contain `RunControl`, `NodeContext`, a Tokio cancellation token, or a Tokio
Runtime. When a validated `ChatModel` is held by a normal Group node, Group
cancellation or node timeout drops the node Future and therefore the in-flight
raw adapter Future. See
[`examples/model_node.rs`](crates/group-agent-model/examples/model_node.rs).
That example uses only an offline mock model and preserves `ModelError` through
`NodeError::with_source`.

`group-agent-model` itself still has no provider SDK or HTTP dependency.
Stage 17 adds the separate
[`group-agent-genai`](crates/group-agent-genai) adapter without changing that
crate or Core. It provides no API-key loading, retry, fallback, tool runtime,
MCP, embeddings, RAG, memory, ReAct, or prebuilt agent.

## Local Tool Runtime

[`group-agent-tool`](crates/group-agent-tool) depends on
`group-agent-model`, while Model and Core do not depend on it. Core is used only
as a dev-dependency for the offline Node integration example and tests. The
crate's normal dependency graph contains no `group-agent-core`, `genai`, HTTP
client, SQLx, MCP, or provider SDK.

`Tool` is an object-safe asynchronous `Send + Sync` port stored as
`Arc<dyn Tool>`. Its borrowed `ToolInput` contains the validated call ID, tool
name, structured JSON arguments, optional opaque idempotency key, and
provider-neutral execution metadata. It deliberately contains no
`NodeContext`, Group cancellation token, runtime, channel, or background task.
`ToolOutput` wraps an existing `ToolResult` and optional Extensions. A
model-visible business rejection is an explicit `ToolResult` with
`is_error = true`; infrastructure failure remains a source-preserving
`ToolError`/`ToolRuntimeError` and is never silently converted to success.

`ToolRegistryBuilder` validates the advertised name, cached definition,
description, behavior consistency, duplicate names, and JSON Schema. It
compiles one `jsonschema::Validator` per successful registration, then freezes
entries into an immutable, cheaply cloned registry with indexed lookup and
lexically stable definitions. Schema compilation is never repeated during
execution. The validator is built with default features disabled, so Tool
Runtime does not acquire HTTP or filesystem schema resolvers.
Registration and argument-validation errors keep a concrete owned
`jsonschema::ValidationError` in their source chains. Their default Debug and
Display expose only Tool identity, instance path, schema path, and keyword—not
arguments, schema values, or the upstream source message. Explicit
`Error::source()` traversal can reach upstream details; applications are
responsible for filtering a complete source chain before logging it.

`ToolRuntime` follows one fixed path:

```text
ToolCall -> call identity -> registry lookup -> cached schema validation
         -> side-effect/idempotency policy -> Tool::execute -> ToolOutput
         -> ToolResult -> Message::tool(original ToolCallId, result)
```

Per-call timeouts use Tokio time from the caller's runtime. Timeout drops the
in-flight Tool Future and returns a structured `TimedOut` error retaining safe
call identity and its timeout source. Dropping a single or batch Runtime Future
drops all still-pending Tool Futures; no detached task remains. This is
cooperative Future-drop cancellation and cannot roll back external side
effects.

`ToolBehavior` classifies tools as `ReadOnly`, `IdempotentWrite`, or
`NonIdempotentWrite`, independently records whether overlap is allowed, and may
require an explicit idempotency key. There is no automatic retry, durable key
store, distributed lock, or exactly-once guarantee. Non-idempotent writes are
sequential by default and overlap only when the Tool explicitly opts in.

Batches reject duplicate `ToolCallId` values before execution, prevalidate
every item, use bounded `FuturesUnordered` scheduling without per-call spawn,
and reorder results in O(n) to the original input order. Collect-all is the
default. Fail-fast means stop scheduling after the first observed primary
failure and drain every already-started call to its real success or failure;
only calls never started are `NotStartedDueToFailFast`. It does not invent
`Cancelled` outcomes. Dropping the complete batch Future is different: it
drops pending Tool Futures and produces no execution report. Invalid-schema and
missing-tool items never enter a Tool.

Optional synchronous `ToolEventSink` callbacks are fallible, called outside
Registry locks, and have panic capture that never formats or retains panic
payloads. `ExecutionStarted` runs before Tool execution; its error or panic
prevents the Tool from running and returns `ObserverFailed`. Completed, failed,
and timed-out events run only after the primary outcome is known. Their error
or panic cannot overwrite a `ToolResult`, `ToolError`, or `TimedOut`; use
`execute_report` or the batch report's ordered observer diagnostics to inspect
that secondary failure. Events contain only call identity, batch index, outcome
class, and timeout duration—not arguments, output, metadata values, or source
text.

`ToolResult` remains payload-only Model data. Use `execute_message`,
`execute_message_report`, or `ToolBatchReport::into_tool_messages` to construct
`Message::Tool` with the original `ToolCallId` while preserving content and
`is_error`. The Runtime supplies no retry, exactly-once guarantee, or rollback
for external side effects.

See the offline
[`tool_runtime`](crates/group-agent-tool/examples/tool_runtime.rs) and
[`tool_node`](crates/group-agent-tool/examples/tool_node.rs) examples. The Node
example preserves Runtime failures through `NodeError::with_source`; Group
continues to own node cancellation and deadlines without any Core change.

## MCP Tool Adapter

[`group-agent-mcp`](crates/group-agent-mcp) is a client-only Tool backend.
It depends on Model, Tool, and the official crates.io
`rmcp = "=2.2.0"`. rmcp default features are disabled; only `client` and
`transport-async-rw` are enabled. The adapter owns direct-child construction
instead of enabling rmcp's child-process transport, so it can retain a
runtime-independent lifecycle guard. Server macros, HTTP transports, OAuth, and
credential handling are not compiled in. Core, Model, and Tool do not depend on
MCP; Core appears only in MCP examples and integration tests.

`McpClientSession` performs one initialize handshake and shares the resulting
rmcp peer and owned service lifecycle through `Arc`. Calls reuse that session;
the adapter never reconnects, creates a Runtime, or adds a per-call forwarding
task or channel. `connect_stdio` builds `std::process::Command` from a separate
executable and argument vector without shell parsing, retains the direct child,
and gives its pipes to rmcp's async-read/write transport. Explicit `shutdown`
atomically stops new calls, closes and joins rmcp, waits a configured grace
period for the child, then kills and waits again when necessary. It is
idempotent: one Session-owned cleanup task and completion are created, and all
concurrent or repeated callers await the same stored result. Cancelling one
shutdown Future cancels only that waiter, not rmcp close or child cleanup.
Service close and direct-child cleanup run in independent tasks that the
supervisor always awaits. Both success and failure are published only after the
direct child has exited and been reaped; the final result is stored, `CLOSED` is
published, and only then are completion waiters woken. Both an outer rmcp task
JoinError and `QuitReason::JoinError` are source-preserving `ShutdownFailed`;
service or child worker panic is also `ShutdownFailed` and cannot skip child
cleanup. If service close and child cleanup both fail, the service failure is
the primary returned source; the child path has nevertheless completed. rmcp
2.2.0 logs but does not return errors from its internal `transport.close()`
call, so Group cannot promise to surface those errors. Zero grace means one
non-blocking child exit check followed by immediate kill and wait/reap when it
is still running. A child that ignores stdin EOF is therefore still reaped.

Drop is deliberately different: it does not run graceful async close, wait for
the Session completion, or report a result. It synchronously requests direct
child termination and tries to hand reap to a standard thread, so the usual
fallback does not depend on a surviving Tokio runtime. If thread creation fails
because of resource exhaustion or an OS error, the direct-child kill has still
been attempted, but the current process cannot guarantee wait/reap and may
temporarily retain a zombie until parent exit or another OS cleanup mechanism.
Drop does not perform an unbounded synchronous wait and is only a best-effort
safety net; explicit `shutdown()` is the reliable and recommended lifecycle
path. When explicit cleanup already owns the child, Drop cannot take or
terminate it a second time. Only the direct child is covered, not its process
tree. Dropping one call Future promises only local ownership release—not
immediate remote termination or side-effect rollback.

Discovery first checks the initialized server's tools capability, then runs an
adapter-owned `tools/list` state machine. It tracks every returned cursor,
rejects same-cursor and longer cycles, and enforces configurable non-zero page
and accumulated-tool limits with checked arithmetic. Pages remain private until
the traversal reaches `nextCursor = null`; a later protocol failure, disconnect,
cycle, limit, duplicate Tool, name conflict, or invalid Schema publishes no
partial snapshot. Only the complete result is mapped into `ToolDefinition`,
sorted by local Tool name, registered through `ToolRegistry`, and frozen as
`McpToolSet`. Registry registration compiles each input Schema exactly once. A
later refresh is an explicit new `discover` call that produces another
snapshot; `tools/list_changed` never mutates an active Registry.

A single server may preserve remote names. `ServerNamespace` or an explicit
stable prefix supports multiple servers using `prefix__remote`; the frozen
`McpToolMapping` retains the exact local/server/remote tuple, so execution sends
the original remote name without parsing a prefix or modifying arguments.
Collisions and invalid local definitions fail structurally and never overwrite.

Every remote Tool defaults to `NonIdempotentWrite`, sequential execution, and
no retry. MCP annotations remain untrusted hints. Applications may explicitly
override one exact remote name during discovery; unknown or inconsistent
overrides fail before the snapshot is published. Duplicate entries are rejected
even when their behavior values match; configuration never uses
last-write-wins.

```text
ToolRuntime identity/schema/policy validation
-> MCP Tool
-> rmcp call_tool(original remote name, structured JSON arguments)
-> content mapping
-> ToolResult
-> ToolMessage(original ToolCallId)
```

Text blocks retain wire order. `structuredContent` is serialized once as one
JSON text part after ordinary text blocks. `isError = true` remains a business
`ToolResult`, not `CallFailed`. Image, audio, embedded resource, resource link,
and unknown future content fail closed as `UnsupportedContent`; nothing is
downloaded, discarded, or replaced with placeholder text. Transport,
JSON-RPC, discovery, process, and serialization failures retain concrete
sources. rmcp `ServiceError::McpError` and JSON-RPC MCP error responses are
classified exactly as `Protocol`, while I/O, connection closure, and transport
send failures are `Transport`. Default Debug/Display never print environment
values, arguments, output, raw protocol payloads, source messages, or
`McpToolSet` local-to-remote name mappings; original names remain available only
through explicit mapping accessors. Explicit source traversal may expose
upstream details and remains the caller's logging responsibility.

See the fully offline [`mcp_stdio`](crates/group-agent-mcp/examples/mcp_stdio.rs)
and [`mcp_tool_node`](crates/group-agent-mcp/examples/mcp_tool_node.rs)
examples. The first starts a local stdio fixture, discovers and executes a Tool,
builds a correctly paired Tool message, and shuts down. The second performs the
same call through an ordinary Group Node without changing Core.

### Stage 16.2 migration from Stage 16.1

- Change `ChatModelAdapter::{complete_raw, stream_raw}` parameters from
  `ChatRequest` to `ValidatedChatRequest`.
- Provider adapters inspect `request()`, `messages()`, `tools()`,
  `tool_choice()`, `generation()`, or `extensions()`, or call `into_inner()` to
  consume the original `ChatRequest` without cloning it. No public reverse
  construction exists.
- Application-facing calls remain `ChatModel::complete(ChatRequest)` and
  `ChatModel::stream(ChatRequest)`.
- Treat the first manual `ChatStreamCollector::push` error as terminal. Do not
  attempt to repair the collector; discard it. Failed extension and Usage
  merges now leave all prior state unchanged.
- Choose either manual `ChatStreamCollector::{push, finish}` usage or
  `collect_chat_stream(stream)`. The helper owns its own collector, stops on the
  first error, and does not require a separately constructed collector.

### Stage 17 genai adapter

Applications construct and configure one `genai::Client`, then inject it:

```rust
use genai::{Client, adapter::AdapterKind};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig,
    GenaiStreamingPolicy,
};
use group_agent_model::{
    ChatModel, ModelCapabilities, ModelId, ProviderId,
};

fn build_model() -> Result<ChatModel, Box<dyn std::error::Error>> {
    let client = Client::builder()
        .with_adapter_kind(AdapterKind::OpenAI)
        .build();
    let target = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("openai")?,
        ModelId::new("gpt-4o-mini")?,
        ModelCapabilities::new()
            .with_streaming(true)
            .with_tool_calling(true)
            .with_usage_reporting(true),
    )?;
    let adapter = GenaiChatModelAdapter::new(
        client,
        GenaiAdapterConfig::new(target)
            .with_streaming_policy(GenaiStreamingPolicy::AuditedTextOnly),
    )?;
    Ok(ChatModel::from_adapter(adapter)?)
}
```

Capabilities are explicit and conservative. `parallel_tool_calls` is rejected
because genai 0.6.5 has no provider-neutral request control for it. Binary and
Custom response parts are rejected. Reasoning may be retained only in redacted
`group.genai.reasoning_content` Extensions and is never answer text. Request
Extensions accept only documented adapter-owned keys; unknown
`group.genai.*` keys fail, other namespaces are ignored, and arbitrary headers
or `extra_body` are never exposed.

The adapter uses genai's actual `ChatRequest`, `ChatOptions`, response, usage,
and stream types. Streaming defaults to Disabled. Enabling it requires a
Client explicitly bound to `AdapterKind::OpenAI`; there is no caller-supplied
protocol claim. Each returned stream's actual resolved `model_iden` is checked
before its lazy HTTP stream is polled, so a custom or changing resolver cannot
redirect an audited call into Responses. Audited OpenAI Chat text-only
streaming is supported; requests with tools and all OpenAI Responses streaming
fail before HTTP dispatch because genai 0.6.5 can lose a second ToolCall in one
SSE event and its Responses streamer can skip malformed data, trace raw events,
and synthesize End at EOF.

Non-streaming requests that may produce ToolCalls require
`GenaiChatModelAdapter::new_with_stable_target`: it accepts a `ClientConfig`
without a `ServiceTargetResolver` and dispatches the same exact
`ServiceTarget` that was validated. Dynamic or otherwise unverifiable
resolvers fail closed before network dispatch for these requests; ordinary
non-streaming text remains supported. For OpenAI Responses tool calls, genai
first reads, parses, and may clone the complete raw response value. Group then
applies an 8 MiB-by-default post-capture parser admission limit and extracts
only ordered reasoning signatures and matching function-call identity. This
limit does not bound network reads, the HTTP body, or peak memory. Its
early-terminating counting serializer retains no serialized bytes. The raw
value in a successful response is taken, parsed, and released after mapping.
It is not placed in Group `ChatResponse`, Extensions, adapter mapping errors,
or their default Debug/Display output.

If genai itself fails while parsing a Provider response, Group retains the
concrete `genai::Error` through `Error::source()`. Default `ModelError`, Node,
and Graph Debug/Display remain redacted, but an application that explicitly
walks or records the complete source chain may encounter upstream error data
and must apply its own sensitive-data filtering.

Within each function call, identical signatures are deduplicated in first
occurrence order and distinct signatures preserve provider order. No
deduplication crosses function-call boundaries; empty signatures, checked
length overflow, and configured count or byte-limit violations fail. A real
two-turn local HTTP test verifies Provider-to-Group-to-Provider continuation
without injecting a signature in application code. A returned Group stream
directly owns the genai stream. Normal EOF without genai `End` on an allowed
path is an explicit Protocol error, and dropping the Future or stream releases
that ownership without a detached task. `ResponseId` Debug and Display are
redacted; `as_str()` is the deliberate raw-value accessor. See
[`docs/adapters/genai.md`](docs/adapters/genai.md) for the mapping table,
continuation example, extension contract, current limits, and the discovered
split compiler-support policy required by genai 0.6.5.

### Compiler support policy

Group uses a layered minimum supported Rust version (MSRV):

| Crate / layer | MSRV |
| --- | --- |
| `group-agent-core` | Rust 1.85 |
| `group-agent-model` | Rust 1.85 |
| `group-agent-tool` | Rust 1.85 |
| SQLite and observability adapters | Rust 1.85 |
| `group-agent-mcp` | Rust 1.88 |
| `group-agent-genai` | Rust 1.88 |
| Full workspace | Rust 1.88+ |

The workspace package default remains Rust 1.85. `group-agent-mcp` independently
declares Rust 1.88 because rmcp 2.2.0's published source uses let-chain syntax
that is not accepted by Rust 1.87. Stage 19.1 does not enable rmcp's
child-process feature; the adapter owns direct-child construction and uses
rmcp's async-read/write transport.
`group-agent-genai` independently declares Rust 1.88 because the published crates.io
source for genai 0.6.5 uses let-chain syntax, which became stable in Rust 1.88.
This is an effective MSRV inferred from the release's actual source, not an
explicit `rust-version = "1.88"` declaration by genai. Group's Runtime and
provider-neutral Model and Tool crates have not raised their MSRV. Applications
that do not select the optional provider adapter can continue to use those
layers with Rust 1.85; building the complete workspace requires Rust 1.88 or
newer.

## Shared-state subgraphs and execution namespaces

A compiled graph using the same `GraphState` can be mounted as a structural
item in a parent:

```rust
let research = research_builder.compile()?;

let mut graph = StateGraph::new();
graph.set_version("agent-v4");
graph.add_node("prepare", Prepare)?;
graph.add_subgraph("research", research)?;
graph.add_node("answer", Answer)?;
graph
    .add_edge(START, "prepare")
    .add_edge("prepare", "research")
    .add_edge("research", "answer")
    .add_edge("answer", END);
```

The mount is not a node, performs no user code, and consumes neither a step nor
a super-step. Child real nodes borrow and update the same Runtime-owned State,
use the same `RunId`, cancellation token, run deadline, event configuration,
and checkpoint lineage, and contribute to the parent's cumulative step and
super-step counters. Reaching child `END` follows the mount's parent
transition; it does not complete the parent run. `START -> END` children return
to the parent immediately. Nested subgraphs are supported.

`GraphPath` is a structured sequence of mount identifiers. `NodePath` is that
namespace plus one leaf `NodeId`; for example, `/research/verify` displays two
segments rather than a string that Runtime later parses. Display uses
slash-prefixed segments and percent-escapes `%` and `/` within identifiers, so
identifiers containing `.`, `/`, `%`, or an empty string remain unambiguous.
The root `GraphPath` displays as `<root>`. Both types are cheap to clone through
shared storage and implement `Display`, `Debug`, equality, and hashing.
Runtime lookup and `Eq`/`Hash` use the structured segments, not displayed text.
`NodeContext::node_path()` exposes the complete path while `node_id()` remains
a leaf compatibility accessor. Node lifecycle events, node-related run errors,
interrupt metadata, visited nodes, state batch sources, and checkpoint
frontiers use `NodePath`.

Entering and leaving a child emits `SubgraphStarted` and
`SubgraphCompleted`. A child does not create a second top-level `RunStarted` or
`RunCompleted`. Failure or interruption before child exit omits
`SubgraphCompleted`. On resume inside a child, event order begins
`RunStarted -> RunResumed -> SubgraphStarted` for each containing namespace,
then continues with node events.

Compilation pre-resolves child entry, exit, paths, and internal transitions.
`add_subgraph` takes ownership of an already immutable compiled child, so
direct and indirect reference cycles are unrepresentable through the safe
builder API; flattening still guards path uniqueness as a compiler invariant.
For Stage 9, a subgraph mount cannot run beside another active parent-frontier
item; such topology is rejected with `SubgraphInParallelFrontier`. `END` exits
only its own branch and is removed before this check, so a fan-out such as
`[END, child]` is valid because only the child remains active. A subgraph plus
an ordinary active node remains invalid, directly or through later
transitions. Parallel super-steps inside a child remain supported.
Parent/child State mapping and conditional fan-out directly into a subgraph
mount are not implemented. Conditional fan-out inside a child remains valid
when it selects ordinary child nodes or `END`. See
[`examples/subgraph.rs`](crates/group-agent-core/examples/subgraph.rs).

One root `GraphVersion` is the compatibility version of the complete composed
graph. Change it whenever a mounted child's topology, State/Snapshot schema,
batch reducer, router behavior, or interrupt meaning becomes incompatible with
saved checkpoints.

## Parallel super-steps

Static fan-out is one transition kind:

```rust
graph.add_fan_out("prepare", ["local_search", "web_search"])?;
graph
    .add_edge("local_search", "synthesis")
    .add_edge("web_search", "synthesis");
```

An executable node has exactly one fixed, static fan-out, single-target
conditional, or conditional fan-out transition. `START` continues to require
exactly one fixed successor.

The Runtime maintains an active frontier. Nodes in a multi-node frontier start
in compiled node order and are polled concurrently using `FuturesUnordered`,
without `tokio::spawn`. They all borrow the same `&State`. Only after every node
succeeds does the Runtime commit updates and calculate successors. Duplicate
successor indices are sorted and deduplicated, so a fan-in target executes once
in the next super-step. An `END` successor removes only that branch; other
branches continue until the frontier is empty.

`max_steps` counts real node executions across all frontiers. A parallel
frontier is atomic with respect to this limit: if the complete frontier does not
fit, none of its nodes starts and `MaxStepsExceeded` identifies the first stable
frontier position that would exceed the limit.

### Deterministic state merge

`GraphState::apply_batch` receives `Vec<NodeUpdate<S::Update>>` in compiled node
order. Each entry exposes its complete source `NodePath`, leaf `NodeId`, and
update:

```rust
fn apply_batch(
    &mut self,
    updates: Vec<NodeUpdate<Self::Update>>,
) -> Result<(), StateError> {
    // Validate the entire batch without mutating self.
    let validated = updates
        .iter()
        .map(|item| validate(item.node_path(), item.update()))
        .collect::<Result<Vec<_>, _>>()?;

    // Commit only after all validation succeeds.
    for update in validated {
        self.commit(update);
    }
    Ok(())
}
```

The default implementation rejects a batch containing multiple updates before
modifying state; it never silently applies last-write-wins. A custom batch
implementation must validate the complete batch before mutation because the
Runtime does not clone the complete state to provide rollback. One-node
frontiers continue to call `apply`, so existing sequential states need no
change. See
[`examples/parallel.rs`](crates/group-agent-core/examples/parallel.rs) for a
complete fan-out, merge, and fan-in example.

## Checkpoint foundation

Checkpoint capability is separate from `GraphState`, so ordinary states still
need neither `Clone` nor Serde:

```rust
impl CheckpointState for AgentState {
    type Snapshot = AgentSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(AgentSnapshot::from(self))
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self::from(snapshot))
    }
}
```

`restore` is called synchronously during resume and remains outside storage
locks. It is not preemptible by cancellation or timeout; control is observed
again immediately after it returns.

Checkpointing is enabled only through `invoke_with_checkpoint`:

```rust
use std::sync::Arc;

use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, Checkpointer, InMemoryCheckpointer,
};

// AgentCheckpointCodec implements CheckpointCodec<AgentSnapshot>.
let store = Arc::new(InMemoryCheckpointer::new(AgentCheckpointCodec));
let config = CheckpointConfig::new(
    "conversation-42",
    Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
    CheckpointPolicy::EverySuperstep,
);

let report = compiled
    .invoke_with_checkpoint(
        initial_state,
        RunConfig::default(),
        EventConfig::default(),
        RunControl::default(),
        config,
    )
    .await?;
```

### Durable record and codec boundary

`CheckpointRecord` is the storage-neutral persistence model. It contains a
separate `CheckpointFormatVersion`, checkpoint/thread/run/parent identifiers,
optional `GraphVersion`, fixed-width `u64` cumulative step and super-step,
structured
`NodePath` frontier, completion/interrupt metadata, encoded Snapshot bytes, and
optional encoded interrupt payload bytes. `CheckpointRecordParts` and
`CheckpointRecord::try_from_parts` let an external backend reconstruct and
validate records without private Runtime constructors.

`CheckpointCodec<T>` supplies a stable Snapshot `CodecDescriptor`
(`payload schema + schema version + codec/encoding identity`), byte
encode/decode methods, and optional durable interrupt payload methods. Thus
JSON, bincode, and other encodings cannot collide merely because they reuse a
schema name and version. Descriptor mismatch is rejected before decoding.
`EncodedValue` equality includes the complete descriptor and bytes.

The codec must emit deterministic, canonical bytes for the same logical value;
otherwise stable-content idempotency cannot be guaranteed. It does not require
`GraphState`, Snapshot, or payload types to implement Serde or Clone. Codec
calls are synchronous and always occur outside store locks. Format, descriptor,
counter-conversion, and decode failures are structured and retain complete
codec source chains.

Typed Runtime counters remain `usize`. Record reconstruction converts both
`u64` counters with `usize::try_from` and returns a structured incompatibility
when the current target cannot represent a value; it never silently truncates.

`CheckpointStore` is the asynchronous record port for third-party durable
backends. `RecordCheckpointer<T>` combines a store and codec into the typed
Runtime `Checkpointer<T>` boundary. `InMemoryCheckpointStore` implements the
same record CAS/idempotency contract; `InMemoryCheckpointer<T>` is its
convenient typed adapter. The in-memory store remains process-local, but its
records can be exported and reconstructed by a fresh store/adapter instance.

### SQLite durable checkpoint backend

`group-agent-checkpoint-sqlite` is an independent workspace crate. It depends
on `group-agent-core`; Core does not depend on SQLx or the SQLite adapter.
Applications continue to provide their own `CheckpointCodec`:

```rust
use std::sync::Arc;

use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{CheckpointCodec, CheckpointStore, RecordCheckpointer};

let sqlite = Arc::new(
    SqliteCheckpointStore::connect("sqlite://group-checkpoints.sqlite3").await?,
);
sqlite.migrate().await?;

let records: Arc<dyn CheckpointStore> = sqlite;
let checkpointer = Arc::new(RecordCheckpointer::new(
    records,
    Arc::new(AppCheckpointCodec) as Arc<dyn CheckpointCodec<AppSnapshot>>,
));
```

`connect` creates a missing file database and configures foreign keys, WAL
journaling, and a five-second busy timeout. `from_pool` accepts an
application-managed `sqlx::SqlitePool`; in that case the application owns those
connection settings. `migrate` uses migrations embedded in the crate, is safe
to repeat, and does not require `DATABASE_URL` while compiling.

The schema has an append-only checkpoint-record table and one current-head row
per `ThreadId`. UUID-backed identifiers use their exact 16-byte form. `step`
and `superstep` use eight-byte big-endian blobs, which represent every `u64`
without conversion through SQLite's signed integer type and retain
lexicographic sort order. Structured `NodePath` segments are stored as an
adapter-private JSON DTO; Snapshot and interrupt descriptors and bytes are
stored in separate lossless columns. Serde is therefore private to this
adapter and imposes no bound on `GraphState`, Snapshot, or Update.

Each save starts `BEGIN IMMEDIATE` and performs idempotency lookup before
lineage CAS, then inserts the record and advances the thread head in the same
SQLx-tracked transaction. Exact old-operation replay succeeds even after the
head advances. Reusing an ID with different stable Record content returns
`IdempotencyConflict`; two writers from one parent cannot create an implicit
Fork. SQLite busy/lock/database errors remain source-preserving storage errors,
not lineage conflicts. User Codec, snapshot, restore, and other application
code never executes inside the database transaction.

SQLx is pinned to `0.8.6` with only the Tokio, SQLite, migration, and macro
features. The 0.8 release line supports an MSRV below Group's Rust 1.85 floor;
the workspace validates the adapter with Rust 1.85. SQLx 0.9 is not selected
because it requires a newer compiler. File-database restart tests destroy the
original pool, typed checkpointer, checkpoint, and Snapshot handles, then
reconnect and reconstruct ordinary, conditional fan-out, nested-subgraph, and
durable-interrupt executions from records alone.

`CheckpointId`, `InterruptId`, and `RunId` are UUID v4-backed values. They
support display, parsing, hashing, and stable 16-byte reconstruction and do not
depend on process-local counters. `CheckpointFormatVersion` is independent of
`GraphVersion`: the former versions the record layout, while the latter
versions the complete graph and state semantics.

Graphs intended for checkpoint resume or replay must have an explicit
compatibility version before compilation:

```rust
graph.set_version("agent-graph-v3");
let compiled = graph.compile()?;
```

The root version is stored in every new checkpoint and covers the complete
composed graph. Update it whenever parent or child topology, state/Snapshot
schema, batch reducer behavior, router semantics, or interrupt meaning changes
in a way that makes an old frontier or state unsafe to continue.
Unversioned checkpoints and unversioned compiled graphs cannot resume.

`CheckpointConfig::new` explicitly starts from state with no parent checkpoint.
When the supplied state is based on an existing checkpoint, identify that
lineage rather than allowing storage insertion order to choose it:

```rust
let base = store
    .latest(&ThreadId::from("conversation-42"))
    .await?
    .expect("base checkpoint")
    .id();
let config = CheckpointConfig::new(
    "conversation-42",
    Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
    CheckpointPolicy::EverySuperstep,
)
.with_expected_parent(Some(base));
```

`CheckpointPolicy::EverySuperstep` saves after every successful super-step.
`FinalOnly` creates only the final completed checkpoint. Each immutable
`Checkpoint` records its `CheckpointId`, `ThreadId`, `RunId`, parent, graph
version, super-step, cumulative step count, shared `Arc<Snapshot>`, stable
`NodePath` next frontier, completed flag, and optional interrupt metadata.
`Checkpointer::latest`, `get`, and `history`
return shared `Arc<Checkpoint<_>>` values, so queries do not deep-copy
snapshots. `get` is scoped by both `ThreadId` and `CheckpointId`; it never
returns a checkpoint owned by another thread. History is ordered oldest to
newest.

A checkpoint parent represents the state lineage on which execution was based,
not the previous insertion by wall-clock order. Each record carries both a
Runtime-assigned `CheckpointId` idempotency key and an `expected_parent`.
`CheckpointStore::save` compares the thread's latest record with that expected
parent and inserts atomically. A mismatch returns
`GraphRunError::CheckpointConflict`; it never silently joins unrelated runs.
Consequently, concurrent runs using the same `ThreadId` and base normally race:
the first accepted write advances the lineage and the other conflicts.
Different thread identifiers remain isolated. Within one run, every successful
save becomes the expected parent of that run's next save.

The exact save boundary is:

1. every node in the frontier succeeds;
2. `apply` or `apply_batch` commits successfully;
3. all successor routing succeeds and the next frontier is stable;
4. the user snapshot is created and encoded outside storage locks;
5. the checkpointer atomically stores the record before Runtime enters the next
   super-step.

A failing node, batch merge, state apply, or router does not create a checkpoint
for that super-step. Snapshot, encoding, conflict, and storage failures return
structured `GraphRunError::SnapshotFailed`, `CheckpointEncodeFailed`,
`CheckpointConflict`, or `CheckpointSaveFailed`, emit one final `RunFailed`,
and stop execution. State already committed at the boundary and external node
side effects are not rolled back. Record reconstruction failures remain
structured sources of `CheckpointLoadFailed`.

Run cancellation and run timeout remain active while the asynchronous save
future is pending. Cancellation has priority over run timeout, and both have
priority over a simultaneously ready save result. Such failures use checkpoint
boundary context (`node_id = None`, with the cumulative completed step count),
emit exactly one `RunFailed`, and emit neither `CheckpointSaved` nor
`RunCompleted`. Dropping a save future cannot prove that a backend produced no
side effect: storage may have committed before its future returned. Custom
stores must therefore treat `CheckpointRecord::id()` as an idempotency key.
An exact replay with identical stable record content returns the original
record even if latest has advanced; Snapshot or payload `Arc` identity is
irrelevant. Reusing the same ID with different bytes, lineage, format/schema
version, graph version, frontier, completion, or interrupt metadata returns
`CheckpointWriteError::IdempotencyConflict`. Idempotency lookup precedes parent
CAS, and both checks plus insertion are atomic. `InMemoryCheckpointStore`
implements this contract.

Snapshot creation and codec work are synchronous and cannot be preempted. They
occur before entering storage and never under the in-memory store lock. A legal
`START -> END` graph saves exactly one completed checkpoint under either
policy, with `superstep = 0`, `step = 0`, an empty frontier, and the configured
expected parent. Its successful terminal order is `CheckpointSaved` followed
by `RunCompleted`. The in-memory implementation provides no database
durability.
See [`examples/checkpoint.rs`](crates/group-agent-core/examples/checkpoint.rs).

## Resume from checkpoint

`ResumeConfig` keeps checkpoint selection, checkpoint policy, additional step
budget, events, and execution controls in one configuration:

```rust
let report = compiled
    .resume(
        ResumeConfig::new(
            "conversation-42",
            Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
        )
        // Omit this to load latest.
        .with_checkpoint_id(checkpoint_id)
        .with_run_config(RunConfig::new(100))
        .with_checkpoint_policy(CheckpointPolicy::EverySuperstep)
        .with_event_config(EventConfig::default())
        .with_control(RunControl::default()),
    )
    .await?;
```

Resume loads a specified checkpoint through `get`, or uses `latest` by
default. A specified checkpoint must still equal current latest; otherwise
`ResumeConflict` is returned; selecting an older checkpoint never implicitly
creates a Fork. With `with_branch_id`, “latest” and the same explicit-target
check are scoped to that branch head instead of the default thread head. The
Runtime validates ThreadId, latest-only status, explicit graph version,
completed/frontier consistency, and every saved frontier `NodePath`, resolving
it to compiled internal indices in O(F). `START`, explicit
`END`, unknown or invalid namespaced nodes, unversioned data, and version
mismatches produce
`CheckpointIncompatible`. The frontier must also contain no duplicate path, be
ordered by compiled internal index, and remain within one `GraphPath`
namespace. These checks traverse only the actual frontier, do not scan the
compiled graph, and occur before `CheckpointState::restore`.

Only after every compatibility check and frontier resolution succeeds does the
Runtime call `CheckpointState::restore` outside the storage lock. The resolved
indices are reused directly for execution rather than parsed again. Events are
ordered `RunStarted`, `RunResumed`, then any containing `SubgraphStarted`
boundaries and the continued node lifecycle. Restore failure instead emits
`RunStarted` followed by one `RunFailed`. `RunResumed` identifies the thread,
checkpoint, cumulative step, and super-step position.

Steps and super-steps continue from the checkpoint. `RunConfig::max_steps`
means the additional number of nodes allowed by this resume call; error and
checkpoint positions still use cumulative lineage steps. A resumed save uses
the restored checkpoint as `expected_parent`, and later saves continue that
chain. Resuming a completed checkpoint restores state but executes no node and
does not create another completed checkpoint; its exact success sequence is
`RunStarted -> RunResumed -> RunCompleted`.

Cancellation and run timeout start at the `resume` call entry and remain active
while loading storage and executing. Restore itself is synchronous and
uninterruptible. Any load, compatibility, latest, restore, cancellation, or
timeout failure saves nothing new and emits exactly one `RunFailed`. See
[`examples/resume.rs`](crates/group-agent-core/examples/resume.rs).

## Read-only replay from history

`ReplayConfig` requires an exact `ThreadId` and `CheckpointId`; Replay never
falls back to latest selection:

```rust
let replay = compiled
    .replay(
        ReplayConfig::new(
            "conversation-42",
            historical_checkpoint_id,
            Arc::clone(&store) as Arc<dyn Checkpointer<AgentSnapshot>>,
        )
        .with_run_config(RunConfig::new(100))
        .with_event_config(EventConfig::default())
        .with_control(RunControl::default()),
    )
    .await?;
```

Replay loads that checkpoint through `Checkpointer::get`, validates the same
GraphVersion, completion, interrupt metadata, and canonical O(F) frontier
rules as Resume, and restores State outside storage locks. It then assigns a
new `RunId` and continues from the checkpoint's cumulative step, super-step,
and resolved internal frontier using the normal execution kernel.
`RunConfig::max_steps` is an additional node budget for this replay call.
A completed checkpoint is restored and returns a no-op `ReplayReport`.

Unlike Resume, Replay does not require the checkpoint to be latest and never
constructs a writable checkpoint configuration. It performs no checkpoint
save, parent CAS, head update, history insertion, or implicit branch creation.
The original thread may advance concurrently without affecting Replay. Its
successful event order begins `RunStarted -> ReplayStarted`, followed by any
continued subgraph/node events and `RunCompleted`. Preparation and execution
failures emit exactly one `RunFailed`.

An interrupted source checkpoint requires a correctly typed Resume value, and
a normal checkpoint rejects an unexpected value. If a replayed node interrupts
again, execution fails with `ReplayInterruptUnsupported`; read-only Replay
cannot save a new interrupted checkpoint and emits no `RunInterrupted`.

Replay is not Fork: it returns an in-memory `ReplayReport` and creates no
branch head or durable descendant. It also re-executes node code. Database
writes, network requests, tool calls, and other external side effects may
therefore occur again. Runtime provides no rollback, sandbox, or automatic
deduplication. See
[`examples/replay.rs`](crates/group-agent-core/examples/replay.rs).

## Explicit fork and branch heads

`ForkConfig` requires an exact source `ThreadId` and `CheckpointId`. It assigns
a new `BranchId` by default (or accepts an application-selected one), validates
and restores the source checkpoint using the same O(F) frontier rules as
Resume/Replay, creates the branch head at that source, and then reuses the
normal execution kernel:

```rust
let config = ForkConfig::new(
    thread_id.clone(),
    historical_checkpoint_id,
    checkpointer.clone(),
);
let branch_id = config.branch_id();
let fork = compiled.fork(config).await?;

let branch_history = checkpointer
    .branch_history(&thread_id, branch_id)
    .await?;
```

The source checkpoint need not be latest. Creating or advancing a branch never
changes the default thread head/history or another branch. Branch history
starts with the shared source checkpoint, followed by records written only to
that branch. Each descendant retains the ordinary `CheckpointRecord::parent_id`
chain; branch ownership and the branch head are additive Store metadata rather
than new Record fields.

Branch Resume is latest-only and explicit:

```rust
let outcome = compiled
    .resume(
        ResumeConfig::new(thread_id, checkpointer)
            .with_branch_id(branch_id),
    )
    .await?;
```

The Store applies idempotency before an independent branch-head CAS. Concurrent
writers based on one branch head therefore allow only one successor; they
cannot create an implicit fork. `CheckpointConfig::with_branch_id` routes
checkpoint-enabled execution to the same branch CAS and therefore requires an
`expected_parent` that is the current head of that exact branch.
`ForkStarted` identifies the new run, source checkpoint, historical counters,
and branch; branch Resume also emits `BranchResumed`. Interrupts, nested
subgraphs, conditional fan-out, and completed no-op checkpoints retain their
existing Runtime semantics.

`CheckpointStore` and `Checkpointer<T>` expose additive `create_branch`,
`save_branch`, `branch_head`, and `branch_history` capabilities. The in-memory
and SQLite adapters implement them. A `BranchId` has one owning `ThreadId`.
Duplicate `create_branch` calls return `BranchAlreadyExists`; they are not
idempotent success, including when the caller repeats the same source. An
absent branch, or a branch queried through the wrong thread, makes
`branch_head` return `None` and `branch_history` return an empty collection.

Branch creation is atomic: a load, validation, restore, cancellation, timeout,
or `create_branch` failure before successful creation leaves no Branch. Once
creation succeeds, a later node, control, routing, snapshot, encoding, CAS, or
storage failure keeps the Branch at its last confirmed head. In particular, a
Fork that fails before its first successful descendant save retains the source
checkpoint as its head and can be continued by explicit branch Resume.

SQLite migrations `0002_branch_heads.sql`, `0003_branch_ownership.sql`, and
`0004_branch_read_consistency.sql` persist branch metadata separately. The
ownership migration adds composite ThreadId constraints for source, head, and
membership. The consistency migration adds a branch-first membership index and
triggers requiring an initial source head, membership for every non-source
head, and a parent-continuous membership insertion. A branch save updates its
Record, membership row, and head in one `BEGIN IMMEDIATE` transaction, so any
failure rolls back all three.

SQLite `branch_head` and `branch_history` each use one read transaction and one
JOIN-based record query scoped by both `thread_id` and `branch_id`. The shared
decoder verifies source and head ownership, requires a non-source head to be a
member, and validates the complete stable `source -> descendants -> head`
parent chain before returning any Record. Missing, cross-thread, non-member,
duplicate, or discontinuous data returns a structured corruption error.
Concurrent saves therefore cannot expose a mixed metadata/Record snapshot.
File-database restart tests reconstruct branches without process caches.

Fork starts from the exact historical State and does not accept a State patch.
There is no branch merge, branch deletion, or implicit branch selection. See
[`examples/fork.rs`](crates/group-agent-core/examples/fork.rs).

Resume, Replay, and Fork remain separate operations: Resume continues only the
latest selected lineage, Replay executes one exact historical checkpoint
without any write, and Fork is the only operation that creates a new writable
branch.

## Suspension and human interrupt

Ordinary update-only nodes continue implementing `Node` with no signature
change. A node that may suspend implements `InterruptibleNode` and is registered
through `add_interruptible_node`:

```rust
#[async_trait]
impl InterruptibleNode<AgentState> for ApprovalNode {
    async fn run(
        &self,
        _state: &AgentState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<AgentUpdate>, NodeError> {
        if context.has_resume_value() {
            let decision = context
                .require_resume_value::<ApprovalDecision>()
                .map_err(|source| {
                    NodeError::with_source("invalid approval value", source)
                })?;
            return Ok(NodeOutcome::update(AgentUpdate::Approved(
                decision.clone(),
            )));
        }

        Ok(NodeOutcome::interrupt(ApprovalPrompt {
            summary: "Publish this draft?",
        }))
    }
}
```

`InterruptRequest` assigns a fresh `InterruptId`. Its typed payload is held
behind `Arc` and accessed with safe `downcast_ref`; neither State, Snapshot,
payload, nor Resume value requires Serde. Ordinary `Node` execution creates no
interrupt payload allocation.

`NodeContext::require_resume_value<T>()` distinguishes a missing value from a
concrete type mismatch through `ResumeValueError`; mismatch context includes
the expected and actual Rust type names and can be preserved as a
`NodeError` source. The older `resume_value<T>() -> Option<&T>` remains
available for optional inspection.

Checkpoint-enabled invocation and Resume now return
`ExecutionOutcome::{Completed, Interrupted}`. A singleton node interrupt:

1. applies no state update and performs no successor routing;
2. emits `NodeInterrupted`;
3. snapshots the unchanged committed state;
4. saves an incomplete interrupted checkpoint whose singleton frontier is the
   current node;
5. emits `CheckpointSaved` followed by `RunInterrupted`;
6. returns `ExecutionOutcome::Interrupted`, never `RunCompleted`.

The interrupted report exposes the shared payload, InterruptId, checkpoint and
thread identifiers, last committed State, cumulative committed step and
super-step counters, visited attempts, and retained events. Interrupt is a
successful suspension, not a `GraphRunError`. If checkpointing is disabled,
Runtime returns `InterruptRequiresCheckpoint`. A save failure, lineage
conflict, cancellation, or run timeout remains a failure: it emits one
`RunFailed` and returns no interrupted outcome.

Resume an interrupted checkpoint by supplying a typed value:

```rust
let outcome = graph
    .resume(
        ResumeConfig::new("conversation-42", store)
            .with_resume_value(ApprovalDecision::Approve),
    )
    .await?;
```

The checkpoint must be latest and graph-compatible as before. An interrupted
checkpoint without a value returns `MissingResumeValue`; a normal or completed
checkpoint rejects an unexpected value. Runtime restores State, re-executes the
interrupted node, and exposes the value only through that node's `NodeContext`.
The value is valid only for this one re-execution attempt. After the node
returns an Update, it is cleared before successor execution. If the node
interrupts again, the old value is not stored in the new checkpoint and is not
automatically reused; a later resume must supply a new value. The next save
uses the interrupted checkpoint as expected parent. Repeated interrupts create
fresh InterruptId and CheckpointId values along one continuous lineage.

Re-execution can repeat code and external side effects that ran before the
interrupt. Runtime does not roll those effects back or deduplicate them.
Pre-interrupt work must therefore be idempotent, and irreversible effects
should normally occur only after the node validates its Resume value.

Interrupts are supported only from singleton frontiers. An interrupt observed
in a parallel frontier drops remaining futures, commits none of that
super-step's updates, and returns `UnsupportedParallelInterrupt`. Payloads
created by `InterruptRequest` are typed and process-local until the configured
`CheckpointCodec` provides a durable encoding. A record-backed write with an
unsupported payload fails explicitly with `CheckpointEncodeFailed`; it never
silently drops the payload. See
[`examples/interrupt.rs`](crates/group-agent-core/examples/interrupt.rs).

## Event observation

`EventSink` is a small synchronous, infallible callback trait and does not
require a channel per invocation. `EventConfig` controls delivery and report
retention independently:

```rust
use std::sync::Arc;

use group_agent_core::{
    EventConfig, EventRetention, EventSink, GraphEvent, RunConfig,
};

let sink: Arc<dyn EventSink> = Arc::new(|event: &GraphEvent| {
    // Keep this callback short and non-blocking.
    eprintln!("run {}: {event:?}", event.run_id());
});

let report = compiled
    .invoke_with_events(
        initial_state,
        RunConfig::default(),
        EventConfig::new(EventRetention::None).with_sink(sink),
    )
    .await?;
assert!(report.events().is_empty());
```

All four event configurations are valid:

- `All` / no Sink: retain every event in a successful `RunReport`; this is the
  default and preserves earlier invocation behavior.
- `All` / Sink: deliver and retain the same ordered event sequence.
- `None` / Sink: deliver events immediately without retaining them.
- `None` / no Sink: neither deliver nor retain events. The Runtime skips event
  construction on this disabled path.

Every event carries a lightweight `RunId`, so concurrent invocations sharing one
sink remain distinguishable. Events never contain the complete state or an
update. A sink runs inline on the execution path, must be thread-safe, and
should not perform blocking or otherwise expensive work. `EventSink::on_event`
cannot return an error. If a sink panics, that panic propagates directly; it is
not converted into `GraphRunError`, and no later event delivery is guaranteed.
Core intentionally contains no channel or stream implementation. Applications
that want a Tokio stream can depend on the separate
`group-agent-observability-tokio` crate:

```rust
use group_agent_core::{EventConfig, EventRetention};
use group_agent_observability_tokio::EventBroadcast;
use tokio_stream::StreamExt;

let events = EventBroadcast::new(256)?;
let mut stream = events.subscribe();
let config = EventConfig::new(EventRetention::None).with_sink(events.sink());

let report = compiled
    .invoke_with_events(initial_state, RunConfig::default(), config)
    .await?;
drop(events);

while let Some(item) = stream.next().await {
    match item {
        Ok(event) => observe(event.run_id(), event),
        Err(error) => record_gap(error),
    }
}
```

`EventBroadcast::new` rejects capacity zero and capacities that cannot be
safely represented instead of allowing Tokio's constructor to panic. It uses
`checked_next_power_of_two`; `capacity()` returns the effective power-of-two
capacity of Tokio broadcast's shared bounded ring buffer. For example, a
requested capacity of three has an effective capacity of four. Its sink
performs one synchronous Tokio broadcast send and never awaits or blocks for
capacity. When a subscriber falls behind, overwritten events are reported as
`EventStreamError::Lagged { skipped }`; the stream can then continue with newer
events. Lag is never hidden as a complete event history.

Each call to `subscribe` starts at that instant and has an independent cursor,
so it receives no earlier events. Multiple subscribers can observe the same
subsequent events independently. A stream ends only after every sender
(`EventBroadcast` and all sink handles) is dropped and its buffered events are
drained. Having no subscribers, or dropping subscribers, never fails graph
execution.

`EventRetention` controls only successful `RunReport` storage; stream delivery
is controlled by the Sink and remains active with `EventRetention::None`.
Events from concurrently executing runs may interleave globally, while the
existing synchronous sink callback preserves emission order within each run.
Use `GraphEvent::run_id()` to filter or group them. The broadcast adapter is
not a durable or reliable-delivery system: it provides no event-history replay,
asynchronous backpressure, disk queue, or network transport. This is
independent of graph checkpoint Replay. SQLite durability and event streaming
are independent optional capabilities.

For a multi-node frontier, `SuperstepStarted` is emitted before its stable
`NodeStarted` sequence. `NodeCompleted` follows the real future-completion order
and is intentionally not deterministic. After the batch commits,
`StateUpdated` is emitted in stable node order. `SuperstepCompleted` is emitted
only after update commit and all successor routing succeed. To preserve Stage
1–4 sequential event compatibility, these two super-step boundary events are
only emitted for multi-node frontiers. In checkpoint-enabled runs, a required
save must also succeed before `SuperstepCompleted` is emitted.

Checkpoint-enabled runs emit `CheckpointSaved` only after storage confirms the
save. The event includes checkpoint/thread/run identifiers, boundary position,
and completed status without including the Snapshot. Snapshot or storage
failures emit the corresponding typed `RunFailed`; they do not emit
`CheckpointSaved`.

Resume emits `RunResumed` only after loading, latest/version/frontier
validation, and successful state restoration. It precedes every continued node
event. A resume frontier inside a child then emits `SubgraphStarted` for its
containing namespaces before restarting nodes.

Replay emits `ReplayStarted` only after exact historical loading, compatibility
validation, and successful restoration. It includes the new `RunId`, source
thread and checkpoint, and historical step/super-step. Replay never emits
`CheckpointSaved`; an interrupt during replay ends with one typed `RunFailed`
instead of `RunInterrupted`.

Subgraph entry and successful exit emit `SubgraphStarted` and
`SubgraphCompleted` with a structured `GraphPath`. They share the parent
`RunId`; nested children do not emit additional top-level run events.

Single-target conditional routing emits `RouteSelected`. A successful
conditional fan-out decision emits exactly one `RoutesSelected` after the
complete result has been validated; its `targets` are in stable compiled order.
Router failure, an empty result, a duplicate result, or an undeclared target
emits no route-selection event and ends with structured `RunFailed` metadata.

Successful suspension emits `NodeInterrupted -> CheckpointSaved ->
RunInterrupted` after `NodeStarted`. It emits neither `NodeCompleted`,
`StateUpdated`, nor `RunCompleted` for the interrupted attempt. Checkpoint save
or control failure replaces the final suspension event with one `RunFailed`.

`RunReport` remains a success-only result. On a node, state-apply, batch-apply,
router, undeclared-target, step-limit, snapshot, or checkpoint-storage failure,
the Runtime first delivers all events already reached and then a final
`GraphEvent::RunFailed` to the sink before returning `GraphRunError`. The
`RunFailed` payload contains a stable typed `RunFailure` classification and
execution context; the original source chain stays on `GraphRunError` and is
not stringified into the event. A failed run does not return a partial
`RunReport`.

### Event API migration from Stage 2 to Stage 3

Stage 3 added `RunId` to every `GraphEvent` variant and added `RunFailed`.
Constructing event variants and matching every named field are therefore
breaking changes from the Stage 2 API. Consumers should include `run_id` when
constructing an event and use `..` when a match does not need every field. The
enum remains `#[non_exhaustive]`.

### Execution namespace API migration in Stage 9

Stage 9 changed node-location fields from leaf-only `NodeId` values to
structured `NodePath` values. This affects node-related fields in
`GraphEvent`, execution context in `GraphRunError` and `RunFailure`,
`RunReport::visited_nodes`, `NodeUpdate` sources, checkpoint next frontiers,
and checkpoint/returned interrupt metadata. Code that constructs these values
or exhaustively matches their field types must migrate accordingly.

Use `NodePath::leaf()` or `NodePath::as_str()` when only the leaf is needed.
Compatibility accessors named `node_id()` remain available on
`NodeContext`, `NodeUpdate`, and checkpoint interrupt metadata; use their
`node_path()` accessors when the complete namespace is required. Display output
is diagnostic only and must not be parsed for Runtime navigation.

### Durable checkpoint API migration in Stage 10.1

- `InMemoryCheckpointer::new` now requires a `CheckpointCodec<Snapshot>`.
- Durable backends implement the non-generic `CheckpointStore` record port and
  use `RecordCheckpointer<T>` for Runtime integration.
- `CheckpointRecord`, `CheckpointRecordParts`, `EncodedValue`, and
  `Checkpoint::from_record` are the public persistence/reconstruction boundary.
- `CodecDescriptor::new` now requires independent schema, schema-version, and
  encoding identities; use `schema_version()` instead of the old `version()`
  accessor. `EncodedValue` and record idempotency compare all three.
- Durable Record step and super-step fields are now `u64`; typed
  `Checkpoint<T>` and Runtime counters remain `usize` with checked
  reconstruction.
- `CheckpointId`, `InterruptId`, and `RunId` changed from numeric process-local
  counters to UUID-backed values. Use `Display`/`FromStr`, `from_bytes`, or
  `from_uuid`; numeric `get()` construction/access no longer applies.
- `CheckpointWriteError` can now report `Encoding`, and Runtime exposes
  `CheckpointEncodeFailed` plus the corresponding `RunFailure`.

### Replay API additions in Stage 14

- `ReplayConfig::new(thread_id, checkpoint_id, checkpointer)` always requires an
  exact source checkpoint.
- `CompiledGraph::replay` returns `ReplayReport`, never `ExecutionOutcome`,
  because a replay interrupt is a structured failure rather than a saved
  suspension.
- `GraphEvent::ReplayStarted`,
  `GraphRunError::ReplayInterruptUnsupported`, and the matching `RunFailure`
  classification are new public variants. `GraphEvent` and `RunFailure` remain
  non-exhaustive.

## Execution control

`RunControl` composes with the existing `RunConfig` and `EventConfig`. It uses
Tokio timers and Tokio Util's `CancellationToken`; Group does not implement its
own executor, timer, polling thread, or cancellation primitive.

```rust
use std::time::Duration;

use group_agent_core::{EventConfig, RunConfig, RunControl};
use tokio_util::sync::CancellationToken;

let cancellation = CancellationToken::new();
let control = RunControl::new()
    .with_cancellation_token(cancellation.clone())
    .with_run_timeout(Duration::from_secs(30))
    .with_node_timeout(Duration::from_secs(10));

let report = compiled
    .invoke_with_control(
        initial_state,
        RunConfig::default(),
        EventConfig::default(),
        control,
    )
    .await?;
```

`RunControl::default()` supplies no external cancellation token and enables no
timeout. In that case node execution follows a direct-await fast path. With
control enabled:

- run timeout starts when `invoke_with_control` begins, before `RunStarted` is
  delivered;
- node timeout starts immediately before `NodeStarted` is delivered, so time in
  that synchronous sink callback counts toward the node deadline;
- the Runtime checks cancellation and the run deadline after `RunStarted`,
  before every node in a frontier, while each node future is pending, after
  observed node completion, and before advancing or completing the run;
- synchronous checks and asynchronous waiting both select the earlier absolute
  run or node deadline; equal deadlines select the run timeout. If Runtime
  polling resumes after both have expired, classification still follows that
  absolute ordering;
- cancellation precedes the selected timeout, and the selected timeout precedes
  a simultaneously ready node result. This preserves the equal-deadline
  priority cancellation, run timeout, node timeout, then node result. At node
  boundaries, cancellation and run timeout also take priority over `max_steps`.

The Runtime uses biased `tokio::select!` and does not spawn a task per node.
Cancellation or timeout drops all still-pending node futures in that
super-step. A failed super-step applies none of its collected updates. Dropping
a future does not roll back external side effects already performed by that
future. Synchronous `GraphState::apply`, `GraphState::apply_batch`, conditional
routers, and `EventSink` callbacks cannot be preempted; control is observed at
the next Runtime check after they return. Applied updates are never replayed.

Each parallel node has its own node deadline. Run timeout and cancellation cover
the complete invocation. If a parallel node error, cancellation, or timeout is
observed, the remaining futures are dropped and the first failure observed by
the Runtime wins. Absolute deadline ordering and the existing priority remain:
cancellation, run timeout, node timeout, then node result.

`NodeContext` exposes `cancellation_token()`, `is_cancelled()`,
`run_deadline()`, and `remaining_run_time()`. Cancellation state remains outside
`GraphState`.

Control failures return structured `GraphRunError::Cancelled`,
`GraphRunError::RunTimedOut`, or `GraphRunError::NodeTimedOut`. The sink first
receives all reached partial events and exactly one typed `RunFailed`; it does
not receive `RunCompleted`.

## Conditional routing

A conditional router is synchronous, read-only, and fallible:

```rust
graph.add_conditional_edges(
    "router",
    ["answer", "revise"],
    |state: &DraftState| {
        if state.ready {
            Ok(NodeId::from("answer"))
        } else {
            Ok(NodeId::from("revise"))
        }
    },
)?;
```

Conditional fan-out uses the same read-only, fallible boundary but returns one
or more targets:

```rust
graph.add_conditional_fan_out(
    "router",
    ["local", "web", "cache", END],
    |state: &AgentState| {
        let mut targets = vec![NodeId::from("local")];
        if state.needs_web {
            targets.push(NodeId::from("web"));
        }
        Ok(targets)
    },
)?;
```

Each executable node has exactly one fixed edge, static fan-out, single-target
conditional router, or conditional fan-out router. Both router forms run only
after the source update commits. A conditional fan-out result must be
non-empty, contain no duplicate `NodeId`, and remain within its whitelist.
Invalid results return structured `EmptyRouteTargets`,
`DuplicateRouteTarget`, or `InvalidRouteTarget` errors; duplicates are not
silently removed. `END` may appear beside ordinary targets and exits only the
source branch. One executable target forms a singleton frontier; multiple
targets form a parallel super-step. Targets are resolved to internal indices
and sorted into stable compiled order. Fan-in deduplication still ensures that
one downstream target executes once.

Conditional fan-out may currently select only ordinary nodes and `END`.
Declaring a subgraph mount in its whitelist is rejected at compile time. After
a parallel batch commits, routers for frontier nodes inspect the merged state
in stable node order. Async model, database, or tool work belongs in a node;
the node should write its result into state, and the router should only inspect
that updated state.

See
[`examples/conditional.rs`](crates/group-agent-core/examples/conditional.rs) for
an executable loop and
[`examples/conditional_fan_out.rs`](crates/group-agent-core/examples/conditional_fan_out.rs)
for dynamic multi-target selection.

## Runtime structure and performance policy

Public graph construction uses readable `NodeId` values backed by `Arc<str>`.
Compilation aggregates fixed successors, static fan-out targets, both
conditional router forms, source counts, and successor presence once, then
reuses that data for shape validation, outgoing-edge completeness, and
transition compilation.
Together with topology construction and reachability traversal, ordinary
compilation remains approximately O(V + E). Parents that combine subgraph
mounts with fan-out additionally run a composition-only reachable-frontier-pair
check so indirect mixed subgraph frontiers fail at compile time. It operates on
each produced frontier, removes `END` branches before co-activity checks, uses
structured identifiers rather than path strings, and does not affect graphs
without both features. Compilation resolves every target whitelist to internal
indices. One internal transition kernel handles fixed, single-target
conditional, static fan-out, conditional fan-out, and structural subgraph
enter/exit transitions after state commit. Fixed transitions remain O(1);
static targets are pre-sorted. Conditional fan-out processes only the router's
actual `T` targets and performs `O(T log T)` stable ordering without scanning
the graph.
Frontier sorting and deduplication operate only on produced successor indices,
not by scanning the complete graph. Internal `petgraph` types remain private.
Subgraph mounting flattens structural entry/exit items and precomputes
structured paths at compile time, so Runtime neither concatenates nor parses
path strings. Subgraph resume resolves only its saved frontier.

Each invocation owns its state, frontier, events, visited-node list, and step
counter. The Runtime does not clone complete states, take a global execution
lock, spawn each node, create mandatory channels, or repeat full graph
validation. `GraphState` does not require `Clone`; `RunReport<S>` is cloneable
only when `S: Clone`.

Compiled items distinguish normal nodes, interruptible nodes, and structural
subgraphs. Runtime matches the item kind and directly awaits the selected
public trait future. A normal `async-trait` node therefore keeps its one
required boxed trait future instead of passing through a second boxed adapter
future.

Checkpointing adds no storage call, snapshot creation, codec work, or lock
acquisition to a normal invocation. Enabled runs construct only next-frontier
metadata and never scan the complete graph. Snapshot and codec cost are defined
entirely by user implementations.

Criterion benchmarks provide regression baselines only; no comparative
performance claim is made.

```bash
cargo bench --workspace
```

The baseline covers compilation of 100-node and 1,000-node fixed graphs,
execution of fixed and conditional graphs, repeated invocation, and the Stage 4
control/observation cases. Stage 5 adds 2-, 8-, and 32-branch immediate
frontiers and an 8-branch short-wait frontier. The scheduler baselines are named
explicitly as a 32-total-node linear chain and a 32-branch/33-total-node
fan-out, so they are not presented as equivalent topologies. Stage 6 adds
checkpoint-disabled and in-memory checkpoint-enabled invocation baselines.
Stage 7 adds load-plus-restore-plus-one-immediate-node and completed-checkpoint
no-op resume baselines. Stage 8 adds singleton interrupt-save and
interrupt-resume-plus-final-save baselines. Stage 9 adds the normal-node
single-box path, a ten-node shared-state child, two-level nesting, child
checkpoint/resume, and child interrupt/resume. These are regression baselines
without performance thresholds or cross-framework claims. Stage 10.1 adds UUID
v4 generation, controlled default/retention/checkpoint invocation cases, Record
encode/decode, and fresh-adapter record reconstruction plus Resume. Stage 11
adds a single-target conditional baseline, conditional fan-out at 2, 8, and 32
targets, isomorphic static fan-out cases in the same harness, and
checkpoint-plus-resume of a multi-node frontier.
Stage 13 adds one shared harness for no Sink, broadcast with no subscriber, one
subscriber, four subscribers, and `EventRetention::None` with one subscriber.
Stage 14 adds read-only replay from a middle checkpoint through one immediate
node, completed-checkpoint no-op replay, and replay of a two-node frontier.
Stage 15 adds a historical fork plus one immediate node in the same harness.
Stage 15.1 adds a branch Resume baseline and an independent SQLite
restart-plus-branch-Resume benchmark. Stage 15.2 runs the branch Resume
baseline against a real `InMemoryCheckpointStore` and `RecordCheckpointer`,
rather than a benchmark-only branch implementation. Stage 16.2 measures
validation of a preconstructed 100-message request, aggregation of
preconstructed 100 and 1,000 text-delta streams, eight interleaved fragmented
tool calls, atomic ToolCall delta merge, extension merge/round-trip, atomic
extension conflict validation, merging one new Usage extension into 256
existing entries, and one validated-facade mock completion. Benchmark setup is
kept outside measured iterations. Stage 18 adds registry lookup at 1, 100, and
1,000 tools, simple and complex cached-schema validation through `ToolRuntime`,
immediate dispatch, an eight-call batch, and a stable result-order baseline
whose Tool futures intentionally complete in reverse-ready order. Registry,
schemas, and inputs are constructed outside measured iterations, and large
batch-report destruction is excluded from timed regions. Stage 19 adds offline
mapping baselines for 100 discovered MCP tools, stable namespace conversion,
text and structured results, and dispatch through a reusable in-process MCP
session. It intentionally excludes process startup, network access, and sleeps.
Criterion
uses explicit warm-up, measurement, sample-size, and noise-threshold settings.
Results are local regression baselines only; short-run variation is not a
reason to redesign the runtime.

## Workspace

```text
.
├── AGENTS.md
├── Cargo.toml
├── README.md
├── docs
│   └── adapters
│       └── genai.md
├── rust-toolchain.toml
├── rustfmt.toml
└── crates
    ├── group-agent-checkpoint-sqlite
    │   ├── Cargo.toml
    │   ├── migrations
    │   │   ├── 0001_checkpoint_store.sql
    │   │   ├── 0002_branch_heads.sql
    │   │   ├── 0003_branch_ownership.sql
    │   │   └── 0004_branch_read_consistency.sql
    │   ├── benches
    │   │   └── branch_restart.rs
    │   ├── src
    │   │   └── lib.rs
    │   └── tests
    │       ├── restart.rs
    │       └── store.rs
    ├── group-agent-observability-tokio
    │   ├── Cargo.toml
    │   ├── benches
    │   │   └── event_broadcast.rs
    │   ├── src
    │   │   └── lib.rs
    │   └── tests
    │       └── event_stream.rs
    ├── group-agent-genai
    │   ├── Cargo.toml
    │   ├── benches
    │   │   └── adapter.rs
    │   ├── examples
    │   │   ├── genai_model.rs
    │   │   └── genai_node.rs
    │   ├── src
    │   │   ├── adapter.rs
    │   │   ├── config.rs
    │   │   ├── error.rs
    │   │   ├── extensions.rs
    │   │   ├── lib.rs
    │   │   ├── request.rs
    │   │   ├── response.rs
    │   │   ├── stream.rs
    │   │   └── usage.rs
    │   └── tests
    │       ├── continuation.rs
    │       ├── error_mapping.rs
    │       ├── group_integration.rs
    │       ├── local_http.rs
    │       ├── request_mapping.rs
    │       ├── response_mapping.rs
    │       ├── stream_compatibility.rs
    │       ├── stream_mapping.rs
    │       └── support
    │           └── mod.rs
    ├── group-agent-model
    │   ├── Cargo.toml
    │   ├── benches
    │   │   └── model.rs
    │   ├── examples
    │   │   └── model_node.rs
    │   ├── src
    │   │   ├── content.rs
    │   │   ├── error.rs
    │   │   ├── extensions.rs
    │   │   ├── lib.rs
    │   │   ├── message.rs
    │   │   ├── metadata.rs
    │   │   ├── model.rs
    │   │   ├── request.rs
    │   │   ├── response.rs
    │   │   ├── stream.rs
    │   │   └── tool.rs
    │   └── tests
    │       ├── errors.rs
    │       ├── extensions.rs
    │       ├── messages.rs
    │       ├── model_integration.rs
    │       ├── requests.rs
    │       ├── redaction.rs
    │       ├── responses.rs
    │       ├── streaming.rs
    │       ├── support
    │       │   └── mod.rs
    │       └── tools.rs
    ├── group-agent-mcp
    │   ├── Cargo.toml
    │   ├── benches
    │   │   └── mapping.rs
    │   ├── examples
    │   │   ├── mcp_stdio.rs
    │   │   └── mcp_tool_node.rs
    │   ├── src
    │   │   ├── config.rs
    │   │   ├── discovery.rs
    │   │   ├── error.rs
    │   │   ├── lib.rs
    │   │   ├── mapping.rs
    │   │   ├── session.rs
    │   │   └── tool.rs
    │   └── tests
    │       ├── group_integration.rs
    │       └── mcp_adapter.rs
    ├── group-agent-tool
    │   ├── Cargo.toml
    │   ├── benches
    │   │   └── runtime.rs
    │   ├── examples
    │   │   ├── tool_node.rs
    │   │   └── tool_runtime.rs
    │   ├── src
    │   │   ├── error.rs
    │   │   ├── event.rs
    │   │   ├── lib.rs
    │   │   ├── registry.rs
    │   │   ├── runtime.rs
    │   │   └── tool.rs
    │   └── tests
    │       └── tool_runtime.rs
    └── group-agent-core
        ├── Cargo.toml
        ├── benches
        │   └── runtime.rs
        ├── examples
        │   ├── checkpoint.rs
        │   ├── conditional.rs
        │   ├── conditional_fan_out.rs
        │   ├── interrupt.rs
        │   ├── linear.rs
        │   ├── parallel.rs
        │   ├── fork.rs
        │   ├── replay.rs
        │   ├── resume.rs
        │   └── subgraph.rs
        ├── src
        │   ├── checkpoint.rs
        │   ├── checkpoint_codec.rs
        │   ├── checkpoint_record.rs
        │   ├── checkpoint_store.rs
        │   ├── context.rs
        │   ├── edge.rs
        │   ├── error.rs
        │   ├── event.rs
        │   ├── graph.rs
        │   ├── id.rs
        │   ├── lib.rs
        │   ├── node.rs
        │   ├── path.rs
        │   ├── runtime.rs
        │   ├── state.rs
        │   └── transition.rs
        └── tests
            ├── branch_store.rs
            ├── compile_validation.rs
            ├── checkpointing.rs
            ├── conditional_fan_out.rs
            ├── conditional_routing.rs
            ├── durable_checkpoint.rs
            ├── execution_control.rs
            ├── fork.rs
            ├── interrupt.rs
            ├── linear_execution.rs
            ├── observability.rs
            ├── parallel_execution.rs
            ├── replay.rs
            ├── resume.rs
            ├── subgraph.rs
            └── review_regressions.rs
```

## Run

```bash
cargo test --workspace
cargo run -p group-agent-core --example fork
cargo run -p group-agent-core --example replay
cargo run -p group-agent-core --example resume
cargo run -p group-agent-core --example interrupt
cargo run -p group-agent-model --example model_node
cargo run -p group-agent-genai --example genai_model
cargo check -p group-agent-genai --example genai_node
cargo run -p group-agent-tool --example tool_runtime
cargo run -p group-agent-tool --example tool_node
cargo test -p group-agent-tool --doc
cargo run -p group-agent-mcp --example mcp_stdio
cargo run -p group-agent-mcp --example mcp_tool_node
cargo test -p group-agent-mcp --doc
cargo bench --workspace --no-run
```

The stable toolchain always validates the full workspace. MSRV validation is
split between the Rust 1.85 foundation and the independently versioned
adapters:

```bash
cargo +1.85.0 check --workspace --exclude group-agent-genai --exclude group-agent-mcp --all-targets --all-features
cargo +1.85.0 test --workspace --exclude group-agent-genai --exclude group-agent-mcp
cargo +1.88.0 check -p group-agent-mcp --all-targets --all-features
cargo +1.88.0 test -p group-agent-mcp
cargo +1.88.0 check -p group-agent-genai --all-targets --all-features
cargo +1.88.0 test -p group-agent-genai
cargo +1.88.0 test -p group-agent-genai --doc
```

The complete acceptance matrix remains documented in
[`AGENTS.md`](AGENTS.md#required-validation).

## Current exclusions

This stage does not support State patches during Fork, branch merge, branch
deletion, parent/child State mapping, parent-frontier parallel subgraphs,
parallel interrupts, Replay writes or historical State modification, Time
Travel, PostgreSQL, built-in Serde codecs, arbitrary Node Command or Send APIs,
conditional fan-out into subgraph mounts, custom asynchronous backpressure,
disk event queues, OpenTelemetry exporters, metrics exporters, WebSocket or SSE
servers, network event proxies, standalone reducer registration, provider
fallback or retry, automatic Tool retry, exactly-once Tool execution, durable
Tool idempotency storage, Tool sandboxing, RAG, Tower middleware, Axum,
HTTP services, distributed workers, macro DSLs, or visualization. SQLite is the only
reference database backend; the bounded Tokio stream adapter is process-local
and intentionally lossy. `group-agent-model` remains provider-neutral.
`group-agent-genai` is the sole real chat provider adapter and is fixed to
genai 0.6.5; it excludes credential storage, `.env` loading, retries, rate
limiters, fallback, direct Tool Runtime integration, MCP, embeddings, RAG,
agent memory, ReAct, and prebuilt agents. `group-agent-tool` executes only
application-provided local Tool implementations and provides no provider,
credential, network, sandbox, or persistence layer. `group-agent-mcp` is the
sole MCP client adapter and supports production child-process stdio only. It
does not implement HTTP, OAuth, credential storage, automatic
`tools/list_changed` refresh, Resources, Prompts, Sampling, Roots, server
hosting, retry, remote rollback, or an Agent loop.

## Architecture review cadence

After Stage 10, 20, 30, and every later multiple of ten, perform a full
repository architecture review before continuing feature stages. Corrective
stages such as Stage 5.1 do not count toward this ten-stage cadence.
Stage 9.1 Review has passed. Stage 10.1 supplied the durable-checkpoint contract
correction required by the Stage 10 architecture review. Stages 11 through 15
preserve that reviewed Record/Codec/content-idempotency contract; Stage 15 adds
branch metadata as a Store capability without changing `CheckpointRecord`.
Stage 16 adds an independent application-layer model abstraction without
changing Core or its durable APIs. Stage 16.1 hardens that boundary with a
validated facade, partial usage, redacted Debug, and continuation Extensions.
Stage 16.2 makes raw validation non-bypassable and stream/Usage merging atomic.
Stage 17 adds the separate genai 0.6.5 adapter without changing Core or Model
dependency direction. Stage 18 adds the independent local Tool Runtime over
Model data, with Core remaining unchanged and used only for Tool crate
dev-integration coverage.
Stage 18.1 retains concrete JSON Schema sources, corrects fail-fast outcome
reporting, makes before/after observer semantics panic-safe, and adds
ToolCallId-paired message helpers without changing Core or Model domains.
Stage 19 adds the separate client-only MCP Tool backend with immutable discovery
snapshots, reusable stdio sessions, conservative remote behavior, fail-closed
content mapping, offline lifecycle coverage, and no Core, Model, or Tool reverse
dependency.
Stage 19.1 replaces unbounded upstream discovery aggregation with adapter-owned
cursor-cycle and traversal-limit enforcement, rejects duplicate behavior
overrides, classifies MCP error responses as Protocol, redacts Tool-set Debug,
and gives stdio explicit close/kill/wait plus a Tokio-runtime-independent
direct-child Drop fallback.
Stage 19.2 makes explicit shutdown a Session-owned shared completion, preserves
both outer and `QuitReason` JoinErrors as `ShutdownFailed`, keeps cleanup alive
when one waiter is cancelled, defines zero grace as immediate kill/wait after
one exit check, and documents rmcp 2.2.0's unobservable transport-close errors.
Stage 19.3 separates service and child cleanup tasks so a worker panic cannot
skip direct-child reap, publishes `CLOSED` before waking shared completion
waiters, and narrows the Drop reaper to a best-effort fallback when standard
thread creation fails.
