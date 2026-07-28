# Genai Adapter

> Status: experimental adapter over the compatibility-first Model API. See the
> [documentation index](../index.md) and [architecture](../../ARCHITECTURE.md).

`group-agent-genai` is an application-layer bridge between
`group-agent-model` and exactly `genai` 0.6.5. The design was originally
introduced in Stage 17, but this document describes the current adapter
contract rather than the stage history. The adapter does not change
`group-agent-core`, the checkpoint contract, or the provider-neutral Model
crate.

## Construction and ownership

The application owns authentication, endpoint resolution, model mapping, and
the `genai::Client`:

```rust
use genai::{Client, adapter::AdapterKind};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig,
    GenaiStreamingPolicy,
};
use group_agent_model::{
    ChatModel, ModelCapabilities, ModelId, ProviderId,
};

fn model() -> Result<ChatModel, Box<dyn std::error::Error>> {
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

The adapter does not read `.env`, cache credentials, build a Tokio Runtime,
recreate the Client, or retain hidden conversation state. The application may
configure genai `AuthResolver`, `ModelMapper`, or `ServiceTargetResolver`
before injection.

`GenaiChatModelAdapter::new(client, config)` supports ordinary text completion
with such an injected Client. Requests that may produce ToolCalls require the
trusted-target constructor:

```rust
# use genai::{ClientConfig, ServiceTarget, adapter::AdapterKind};
# use group_agent_genai::{GenaiAdapterConfig, GenaiChatModelAdapter};
# fn build(
#     target: ServiceTarget,
#     adapter_config: GenaiAdapterConfig,
# ) -> Result<GenaiChatModelAdapter, Box<dyn std::error::Error>> {
let client_config = ClientConfig::default()
    .with_adapter_kind(AdapterKind::OpenAIResp);
let adapter = GenaiChatModelAdapter::new_with_stable_target(
    client_config,
    target,
    adapter_config,
)?;
# Ok(adapter)
# }
```

This constructor rejects a `ClientConfig` with a `ServiceTargetResolver`, an
unbound adapter kind, or a target whose kind differs from that binding. The
same exact `ServiceTarget` is used for validation and genai dispatch, so raw
capture cannot be enabled based on one protocol and sent through another.
Dynamic or unknown resolution remains available for ordinary text completion,
but ToolCall generation and signature recovery fail before network dispatch.

## Mapping

| Group | genai 0.6.5 |
|---|---|
| ordered System/User/Assistant/Tool messages | ordered `ChatMessage` roles |
| ordered text parts | one no-separator text value per Group content list |
| ToolDefinition | `Tool` with unchanged name, description, and schema |
| Auto/None/Required/Named | exact `ToolChoice` variant |
| temperature/top-p/max tokens/stops | optional `ChatOptions` fields |
| ToolCall ID/name/arguments | `ToolCall` |
| ToolResult | Tool role `ToolResponse`; `is_error` remains local-only |
| response text and calls | AssistantMessage text and ToolCall values |
| genai StopReason | Stop/Length/ToolCalls/ContentFilter/Other |
| optional i32 Usage counters | independently optional checked u64 counters |

`parallel_tool_calls` is unsupported because genai 0.6.5 has no common request
field for it. Both a declared capability and an explicit request preference
fail rather than degrade. Binary, Custom, and ToolResponse content in an
assistant response are rejected.

Provider-reported model identity becomes the Group response model. Resolved
and provider model names, adapter kind, and raw stop reason are retained under
redacted Extensions. Missing stop reason becomes `Other("unspecified")`; it is
never fabricated as Stop.

## Extensions and continuation

Adapter-owned keys are defined in `group_agent_genai::extensions`:

- `group.genai.thought_signatures`: array of strings on ToolCall data;
- `group.genai.reasoning_content`: array of strings on assistant/response data;
- `group.genai.prompt_token_details` and
  `group.genai.completion_token_details`: serialized usage detail objects;
- `group.genai.previous_response_id`: non-empty request string;
- `group.genai.store`: request boolean;
- `group.genai.resolved_model`, `group.genai.provider_model`,
  `group.genai.adapter_kind`, and `group.genai.raw_stop_reason`: response
  metadata.

Unknown `group.genai.*` request keys fail. Other namespaces are ignored and are
not sent to genai. There is no header, Authorization, secret, or arbitrary JSON
body injection.

Continuation is explicit:

```rust
use group_agent_genai::extensions::{
    PREVIOUS_RESPONSE_ID, STORE,
};
use group_agent_model::{ChatRequest, Extensions, Message, ToolResult};
use serde_json::json;

# fn next_request(
#     prior: &group_agent_model::ChatResponse,
# ) -> Result<ChatRequest, Box<dyn std::error::Error>> {
let call = prior.message().tool_calls()[0].clone();
let mut extensions = Extensions::new();
extensions.insert(
    PREVIOUS_RESPONSE_ID,
    json!(prior.response_id().expect("provider response ID").as_str()),
)?;
extensions.insert(STORE, json!(true))?;
let request = ChatRequest::new(vec![
    Message::Assistant(prior.message().clone()),
    Message::tool(call.id().clone(), ToolResult::text("tool output")),
    Message::user("continue"),
])
.with_extensions(extensions);
# Ok(request)
# }
```

Thought signatures stay attached to ToolCall Extensions and are restored to
genai thought content exactly once. The adapter does not remember the prior
response ID; the caller must supply it.

For a stable-target non-streaming Client bound to
`AdapterKind::OpenAIResp`, a request that may produce a ToolCall internally
enables genai's public `capture_raw_body` and reasoning capture options. After
genai performs HTTP, authentication, endpoint handling, and ordinary response
normalization, the adapter applies a restricted parser to the captured
`serde_json::Value`. It accepts only the ordered Responses `output` items
needed to correlate `reasoning.encrypted_content` with the next
`function_call`, verifies response ID and normalized call ID/name/arguments,
attaches the signature to that ToolCall, and discards the captured value.

The configurable 8 MiB default is a post-capture parser admission limit.
genai has already read, parsed, and may have cloned the complete Provider value
before this check. The limit therefore cannot bound network reads, HTTP body
size, or peak allocation; it only prevents Group from continuing its
restricted parse. Measurement uses an early-terminating checked counting
writer and retains no serialized byte buffer. The raw value is never logged,
returned in Group `ChatResponse` or Extensions, retained by an adapter mapping
error, or stored in Adapter state. On successful mapping it is taken, parsed,
and released.

Consecutive reasoning items belong only to the immediately following
function call. Identical signatures within that call are deduplicated in first
occurrence order, while distinct signatures preserve Provider order. There is
no cross-call deduplication. Empty signatures, checked total-length overflow,
and configured per-call count or total-byte limit violations fail.
Intervening message or unknown items, missing calls, and normalized/raw
identity conflicts are Protocol errors; invalid JSON is Decode with its serde
source.

## Streaming and cancellation

Streaming is fail-closed by default. There is no public protocol profile or
unchecked override. An enabled `GenaiStreamingPolicy` requires the injected
Client itself to be bound to `AdapterKind::OpenAI`; unbound, Responses, and all
other AdapterKind values fail during adapter construction.

| Path | genai 0.6.5 |
| --- | --- |
| OpenAI Chat non-streaming text | Supported with dynamic or stable resolution |
| OpenAI Chat non-streaming ToolCall | Supported only with an exact stable target |
| OpenAI Chat text-only streaming | Supported with trusted binding |
| OpenAI Chat streaming with tools | Unsupported |
| OpenAI Responses non-streaming text | Supported with dynamic or stable resolution |
| OpenAI Responses non-streaming ToolCall | Supported only with stable-target post-capture verification |
| OpenAI Responses signature continuation | Supported only with a stable target; verified by a real two-turn HTTP fixture |
| OpenAI Responses streaming | Unsupported |
| Unknown/custom-resolver streaming | Unsupported unless the exact returned stream resolves to OpenAI Chat |
| Dynamic/unknown-resolver non-streaming ToolCall | Unsupported before HTTP |

OpenAI Chat 0.6.5 consumes only the first ToolCall delta when one SSE event
contains multiple calls. OpenAI Responses 0.6.5 can skip malformed events,
trace raw event data, and synthesize a successful End at transport EOF. These
losses occur before Group receives an event, so the adapter cannot repair them.
Requests that may produce a new ToolCall and all Responses streaming requests
therefore return `UnsupportedCapability(Streaming)` before HTTP dispatch.

genai resolves a `ServiceTarget` once while constructing
`ChatStreamResponse`; the resulting stream is lazy and exposes that exact
target as `model_iden`. Group validates this exact `model_iden` before polling
the same stream. A resolver that changes OpenAI Chat to Responses therefore
causes the unpolled stream to be dropped with zero server hits. Group does not
perform a check followed by a second resolver call, so there is no
check/dispatch TOCTOU window. A changing resolver is revalidated independently
for every returned stream. There is no public unsafe override and Extensions
cannot bypass the check.

On the audited text-only path the wrapper maps actual genai events online.
Usage is emitted before Finished, and ResponseStarted may be delayed until End
because genai exposes response ID and terminal metadata there. An unexpected
ToolCall event is a terminal Protocol error. `ThoughtSignatureChunk` is also
invalid on this path: empty and non-empty chunks both terminate with Protocol
without retaining the signature, guessing ownership, polling again, or
emitting a partial Group event.

The ToolCall normalizer enforces an append-only contract: unchanged cumulative
raw JSON emits nothing, a prefix extension emits only its suffix, and a
non-prefix change is Protocol. Terminal captured arguments are compared as
complete `serde_json::Value` values, including strings, numbers, booleans,
null, arrays, and objects. Invalid accumulated JSON is Decode with its serde
source; a different valid terminal value is Protocol.

Reasoning is never emitted as TextDelta. When retention is enabled it is
bounded and placed in response Extensions. A normal transport EOF without End
produces an explicit Protocol ModelError. The first item error is terminal.

The returned Group stream owns the genai stream directly. There is no channel
or forwarding task. Dropping a completion Future or stream drops the genai
owner; Group node timeout and cancellation therefore release the in-flight
HTTP request. This is an ownership guarantee, not a promise about when an
operating system closes a particular TCP connection.

## Errors and limits

HTTP 401, 403, 408, 429, and 5xx responses map to Authentication,
PermissionDenied, Timeout, RateLimited, and ProviderUnavailable. Transport,
decode, request, and protocol errors retain their concrete source. Request
mapping failures are InvalidRequest, malformed JSON is Decode, and provider
state or terminal metadata conflicts are Protocol.
Retry-after and HTTP status are preserved when genai exposes them. Debug and
Display do not include provider bodies, raw responses, prompts, reasoning,
tools, outputs, response IDs, headers, or credentials. `ResponseId` has
redacted Debug and Display; callers use `as_str()` for deliberate access.
When genai itself fails to parse a Provider response, its concrete
`genai::Error` remains reachable through `Error::source()`. Explicit source
chain traversal can therefore reach upstream error data even though Group's
default `ModelError`, Node, and Graph formatting is redacted. Applications
that log complete source chains must perform their own sensitive-data
filtering.
Group does not trace raw SSE data. The known unsafe genai Responses streaming
path may be constructed by genai during resolution but is never polled or
dispatched through this adapter; direct application calls to genai are outside
this guarantee. The non-streaming Responses captured value used for
continuation is admitted only after genai has fully captured it, restricted to
mapping, and discarded immediately. Its parser admission limit is not a
network or peak-memory limit.

The adapter has no retry, fallback, rate limiter, circuit breaker, credential
storage, `.env` loader, tool execution, MCP, embedding, RAG, memory, ReAct, or
prebuilt Agent. Upgrading to genai 0.7 is a separate migration.

## Compiler support policy

`group-agent-genai` requires Rust 1.88. The MCP adapter also requires Rust
1.88, so the full workspace requires Rust 1.88 or newer. Core, Model, Tool,
SQLite, and Observability retain Rust 1.85.

The published crates.io source for genai 0.6.5 uses let-chain syntax. Rust 1.85
reports that syntax as unstable, while it became stable in Rust 1.88. The
adapter's effective MSRV therefore follows the syntax required by the actual
upstream release. genai 0.6.5 does not itself declare `rust-version = "1.88"`;
this is a source-derived compatibility requirement, not a claim about its
manifest. Group's own adapter code did not cause the increase, and Group's
Runtime and provider-neutral domain model did not raise their MSRV.

Users who build only the Rust 1.85 foundation layer can omit both higher-MSRV
adapters. The authoritative complete matrix lives in
[Architecture: MSRV layering](../../ARCHITECTURE.md#msrv-layering),
[ADR-011](../adr/011-layered-msrv.md), and the executable
`./scripts/verify msrv` gate; this adapter document does not duplicate that
workspace matrix.

Group does not vendor or patch genai, use a Git or path override, set
`RUSTC_BOOTSTRAP`, enable nightly features, downgrade the verified SDK, or move
to the 0.7 beta. The crates.io dependency remains exactly genai 0.6.5.
