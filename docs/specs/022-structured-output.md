# Stage 22: Structured output and typed results

Status: Implemented and locally verified; independent implementation review PASS.
Execution: [Plan 034](../exec-plans/completed/034-structured-output.md).

## Objective and scope

Let a caller request a bounded JSON object contract, use the existing Tool loop,
and consume a locally validated final result as a Rust type. Carry the same
contract through streaming, durable approval, and recovery in a new process.
JSON syntax, schema conformity, Rust deserialization, and business truth are
separate checks. This feature establishes the first three, not factual accuracy.

This is one end-to-end capability with Model -> Genai -> Prebuilt implementation
slices, not three unrelated product modules. The user authorized the specification, public API, feature/dependency edges
and contract-bound durable identities on 2026-09-26. Stage 21 and Plans 028-033 remain
completed; Stage 22 is not completed by accepting this design.

Assumptions: retain Rust 1.88, genai 0.6.5, existing Tool semantics, offline tests,
and opt-in behavior. No new Core dependency or public Core API is needed.

## Baseline evidence

At HEAD `1b70e2356df4e4c90c8fa7ed69552f052926af6d`:

- `group-agent-model/src/model.rs`: ChatModel admits requests before one raw
  adapter call; response output validation is not currently a facade guarantee.
- `group-agent-model/src/stream.rs`: the collector poisons on protocol failure
  and rejects every event after Finished. Collection alone has no output schema.
- `group-agent-genai/src/openai_chat/request.rs`: native streaming rejects an
  upstream response_format default. Non-streaming calls currently use Genai.
- Locked Genai `openai/adapter_shared.rs` rewrites schemas through
  `schema_with_additional_properties_false`; this must not silently rewrite the
  new caller-owned contract. Its response abstraction is not proof of preserving
  every wire refusal field.
- `group-agent-prebuilt/src/lib.rs`: AgentConfig is Copy. Preserve that contract.
- `group-agent-prebuilt/src/snapshot.rs`: snapshots contain transcript, rounds,
  usage and stop reason, but no output schema or provider finish reason.
- `group-agent-prebuilt/src/agent.rs`: plain and approval graphs have distinct,
  fixed version-one identities. Completed checkpoint recovery may run no Node.

## Ownership and dependency direction

| Layer | Responsibility |
| --- | --- |
| Model | Immutable output contract, bounded local validation, capability admission, complete/stream output gate, typed extraction |
| Genai | Native wire mapping, refusal detection, fixed-target admission, HTTP/SSE limits |
| Prebuilt | Apply the contract to every model round; bind graph identity; expose validated final output across all invocation modes |
| Core / SQLite / Tool | Existing execution, storage, approval and tool policies; no production changes |
| Application | Supply schema and Rust type, choose provider/model, interpret results, own business validation and retry decisions |

Use an optional `structured-output` Cargo feature in Model, Genai and Prebuilt,
with defaults unchanged. Genai and Prebuilt forward it to Model. Model's feature
activates its existing `serde` feature plus optional `jsonschema` and `sha2`.
Reuse locked jsonschema 0.49.2 with default features disabled and locked sha2
0.10.9; no package version upgrades. Check the resulting transitive feature tree:
no network/file schema resolver or Tokio runtime dependency in Model. If that
cannot be satisfied, stop and revise the design rather than adding a hidden
runtime requirement. Prebuilt retains its existing Model serde activation.

## Public API

Names and semantics in this table are the implementation approval surface.
All additions are feature-gated; existing constructors, trait methods, defaults,
AgentConfig Copy, and plain-output behavior remain intact.

| Location | Addition | Contract |
| --- | --- | --- |
| Model | `StructuredOutput::new(name, schema: Value) -> Result<Self, OutputSchemaError>` | Validate the v1 profile, compile once, retain immutable schema and stable identity; cheap Clone |
| Model | `StructuredOutput::{name, schema, contract_id}` read-only accessors | Explicit access to caller data; Debug reports counts only |
| Model | `StructuredOutput::validate_response(&ChatResponse) -> Result<Option<ValidatedJsonOutput>, StructuredOutputError>` | ToolCalls round returns None; final Stop yields a validated object; other terminal states fail |
| Model | `ValidatedJsonOutput::{value, deserialize<T: DeserializeOwned>}` | Private validated constructor; deserialize returns `Result<T, OutputDecodeError>` without model calls |
| Model | `ChatRequest::with_structured_output(StructuredOutput)` and `structured_output()` | Default None; request retains immutable contract |
| Model | `ValidatedChatRequest::structured_output()` | Read-only forwarded contract; no new bypass constructor |
| Model | `ModelCapability::StructuredOutput`, capability setter/getter | Default false; presence of a contract requires this before raw dispatch |
| Model | `ModelErrorKind::OutputValidation` | Downcastable `StructuredOutputError` source; never hidden retry |
| Prebuilt | `ToolCallingAgent::new_with_output(model, tools, config, output)` | Additive constructor returning existing AgentBuildError; does not change AgentConfig |
| Prebuilt | `AgentBuildError::OutputConfiguration(...)` | Typed source for unsupported constructor-time contract/capability combination |
| Prebuilt | `AgentOutcome::structured_output() -> Option<&ValidatedJsonOutput>` | Some only for a structured FinalAnswer, None for plain results and MaxRounds |

No generic Agent type, derive macro, schema generation, unchecked validated-output
constructor, or new stream event is introduced. Rust T is chosen when extracting,
not embedded in persistent state. Changing T cannot change what was generated or
persisted. Type mismatch is an explicit extraction error, not a failed graph run.

API sketch (see the runnable offline Prebuilt `structured_output` example):

```rust,ignore
let output = StructuredOutput::new("answer", serde_json::json!({
    "type": "object",
    "properties": {"summary": {"type": "string"}},
    "required": ["summary"],
    "additionalProperties": false
}))?;
let agent = ToolCallingAgent::new_with_output(model, tools, config, output)?;
let outcome = agent.invoke(messages).await?;
let value = outcome.structured_output().ok_or(AppError::NoFinalAnswer)?;
let answer: Answer = value.deserialize()?;
```

Use ordinary Rust 2024 ownership, private fields, typed errors and custom
payload-safe Debug/Display, following the existing Model facade conventions.
Use `Arc` for immutable compiled contracts, not shared mutable State. Preserve
existing Clone/PartialEq contracts on requests and outcomes; compare immutable
contract content/identity and validated values, not compiled-validator pointers.

## Schema profile: `group-json-output/1`

This deliberately small profile is a Group contract, not a claim to implement
all JSON Schema or all provider subsets. JSON Schema 2020-12 validation semantics
apply to admitted keywords. Reject unknown keywords rather than stripping them.

- Root is an object schema with `type: "object"`.
- Types: object, array, string, integer, number, boolean. Nested nullable values
  may use `[type, "null"]` (either order); root cannot be nullable.
- Object nodes require explicit properties, required containing exactly every
  property once, and additionalProperties=false. Empty objects are allowed.
- Arrays require one schema under items; tuple schemas are excluded.
- String enum is supported; nonempty distinct values, consistent with type. A
  nullable string enum must include null if null is intended to validate.
- Optional description is a string annotation. It is retained, not rewritten.
- Exclude references, definitions, recursion, boolean schemas, arbitrary unions,
  anyOf/oneOf/allOf/not, pattern/format, numeric/length constraints, defaults,
  examples and external resources. Reject type-specific keywords on other types.
- Name: 1-64 ASCII letters/digits/underscore/hyphen.
- Fixed v1 admission bounds: 64 KiB serialized schema, 8 nested schema nodes on
  any path (root counts as 1), 256 total properties, 256 total enum entries,
  16 KiB combined UTF-8 bytes in property names, descriptions and enum strings.
  These are conservative Group limits, not provider maximum claims.
- Fixed text bound for every structured-request round: 1 MiB UTF-8 across all
  assistant text parts, including commentary accompanying ToolCalls. Complete
  checks this before deciding whether the round is intermediate; stream applies
  the same bound incrementally. Final JSON nesting depth is limited to 64. Reject
  duplicate object keys, trailing data, non-finite/out-of-range numbers and
  markdown fences. Whitespace surrounding one JSON value is allowed.

Reject unsupported or oversized schemas before compilation/dispatch; check
traversal depth before recursive serialization. Compile offline with no resource
retriever. Validate once per final response boundary, never once per text delta.

## Model response and streaming semantics

The request contract is enforced by ChatModel even for a third-party adapter
that advertises support but returns invalid data. Raw adapter methods remain the
existing trusted extension boundary, not an application bypass guarantee.

Complete:

1. Existing request checks, required output capability and other capability
   checks, then exactly one raw dispatch.
2. Enforce the common text byte bound before classifying the response. With
   nonempty ToolCalls, require FinishReason::ToolCalls and structurally valid
   call data; preserve accompanying text as intermediate commentary, not JSON.
3. Without ToolCalls, require FinishReason::Stop, parse the entire text within
   limits, and validate against the contract. Empty final text is invalid.
4. A mismatch returns OutputValidation; it must not reach a ModelCompleted Update.
   Non-Stop finish reasons fail even if their partial text happens to be valid.

Streaming (only requests with a contract take this new path):

- Wrap the raw stream without a detached task. Check protocol and byte limits
  before forwarding each provisional text/tool delta. Buffer at most the bounded
  output and existing bounded tool data; do not repeatedly parse prefixes.
- Hold back Finished. Consume raw termination, rejecting subsequent events,
  missing termination, transport errors and duplicate terminals. Only then perform
  final validation and yield the one successful Finished event, followed by EOF.
  Raw termination means EOF of ChatEventStream. In the existing native Chat
  adapter, `[DONE]` closes that logical stream; this is not a requirement to wait
  for the HTTP connection to close or validate bytes beyond its logical terminator.
- On the first failure, drop the underlying stream, yield one typed error, then
  stay at EOF. Never yield Finished or Agent Completed after a validation error.
- A raw stream that never ends remains caller-owned pending work; cancellation,
  deadline and drop must still release it. No new implicit timeout/retry.
- Existing `collect_chat_stream`/collector behavior stays unchanged. After
  collecting a response, callers can use `output.validate_response(&response)`
  and `deserialize()` for typed access. Do not imply the bare collector carries
  a schema. This explicit extraction may repeat one bounded validation.

Failure categories are separately testable: schema admission, unsupported model
capability, provider refusal, incomplete finish, malformed/duplicate-key JSON,
schema mismatch, resource limit, and Rust deserialization mismatch. Existing
Protocol/Decode/transport errors retain their categories. Output errors retain
concrete sources where present but default formatting excludes schema, instance,
refusal text, property paths and source messages. Retryability is Never for
contract/admission/output failures. Raw refusal text need not be retained.

## First adapter target

The initial Genai implementation supports only an explicitly configured stable
OpenAI Chat target with `GenaiStreamingPolicy::OpenAiChat` and declared structured
output capability. Unsupported policy/target declarations fail before HTTP.
This is an experimental adapter path, not endpoint/model certification.

- Send response_format with type=json_schema, caller name, strict=true and the
  exact admitted schema. Do not convert schemas via Genai's schema-rewriting
  helper. Existing upstream response_format/extra_body defaults remain rejected
  for this policy; do not invent implicit precedence.
- Reuse the existing configured native HTTP client, auth, endpoint, generation
  controls, headers, no-retry and no-redirect behavior.
- Add a private native non-stream completion path only for requests carrying this
  contract, so response refusal and finish semantics are read from the wire.
  Ordinary completions remain on their existing Genai path. Share native request
  mapping with streaming; stream=false omits stream_options.
- Bound successful native completion bodies to 4 MiB while reading, including
  bodies without Content-Length; require JSON media type and one choice. Do not
  read provider error bodies just to format errors. Existing SSE/frame/tool
  bounds remain in effect as well as the final output bound.
- Recognize a non-null refusal field (including an empty refusal string) in JSON
  or SSE as a typed refusal failure; malformed field types are protocol errors.
  Refusal wins over apparently valid accompanying JSON. Never publish its text.
- Preserve multi-tool identities, interleaved deltas, usage, finish reasons and
  existing request parity. Do not automatically enable strict Tool arguments or
  rewrite Tool schemas; output format and Tool input validation are independent.

## Prebuilt and persistence

Pass the contract on every Model request. Tool rounds continue normally; the
final response must pass validation before State commit. MaxRounds after a Tool
batch remains an ordinary outcome with no structured value and no extra model
request. Approval, rejection, observer composition and round budgets remain
unchanged. No new node/topology is needed.

Bind the graph identity to the immutable contract:

```text
group-agent-prebuilt/tool-calling-agent/structured/1/<digest>
group-agent-prebuilt/tool-calling-agent/approval/structured/1/<digest>
```

Digest is lowercase SHA-256 of compact UTF-8 JSON encoding of
`["group-json-output/1", name, schema]`, with recursively sorted object keys,
array order preserved, no insignificant whitespace and Serde JSON string
escaping. This is a versioned Group encoding, not an RFC 8785 claim. Pin golden
vectors. Whitespace/object-key order do not change identity; arrays, annotations,
name and schema changes do. Resource/validation semantics belong to profile v1;
changing them requires a new profile identity. Rust type T and provider/model
selection are not in this structural contract identity. Hashes are not signatures
and do not protect against a malicious store.

The existing plain/approval graph IDs, AgentSnapshot fields, snapshot codec
identity/version, interrupt payload, Core Record format and SQLite migrations
remain byte-compatible. No migration or adoption of old plain checkpoints into
a structured graph is provided. A fresh process must reconstruct the same
contract from application configuration. Core graph-version checks reject a
mismatch before model/tool execution or writable fork creation; cover Resume,
Replay and Fork explicitly. No caller-supplied graph-ID override bypasses this.

A completed snapshot has no saved FinishReason and can execute zero nodes. At
Prebuilt outcome conversion, revalidate its final transcript under the configured
contract before returning a structured value or publishing Completed. Apply this
to plain invoke, all streaming/sink variants, resume, replay and fork. New live
responses already passed the finish-reason gate before their commit; restored
snapshots are trusted only within the existing store/codec integrity boundary.
Malformed restored content produces AgentError with a typed output source, never
a successful structured result. Preserve immediate GraphRunError sources for
actual graph failures; conversion failures are explicitly separate and may occur
after Core has finished/persisted work. Do not promise rollback or no writes for
post-execution conversion errors. Graph identity mismatch is the pre-execution
no-side-effect boundary. Validated values are transient, never duplicated in the
checkpoint format; retain the canonical transcript.

## Acceptance and test strategy

Public tests, not only helper tests, must cover:

1. Profile limits/unsupported keywords/remote references reject before adapter
   dispatch; schema clones reuse compiled work; feature-off consumers compile.
2. Third-party fake adapter invalid complete/stream results fail through ChatModel;
   tool rounds bypass only final JSON validation; unknown finishes fail closed.
3. Fragment splits, Unicode, duplicate keys, exact limits/limit+1, trailing junk,
   refusal, valid JSON with wrong schema, and valid schema with wrong Rust T.
   Test exactly 1 MiB and limit+1 of commentary in a ToolCalls round through both
   complete and stream; both must agree even though commentary is not JSON.
4. After terminal/errors: no later success or events; drop/cancel/deadline releases
   raw futures, including the period after raw Finished but before EOF.
5. Local HTTP verifies exact strict schema mapping for complete and stream,
   refusal+content precedence, bounded bodies, auth/URL/options parity and zero
   hidden retries. No real provider quota.
6. Tool -> structured final answer; MaxRounds; approval/rejection; invalid final
   result leaves previous durable head unchanged, and no Completed event appears.
7. Same contract recovery works in a new process. Different name/schema/profile
   or plain/structured mode fails before calls, Tool effects or lineage mutation.
   Completed snapshot recovery rebuilds the typed result without a provider call.
8. Replay/Fork preserve their existing read/write rules; malformed restored final
   content never becomes a validated result; completed conversion error sources
   and any prior Store writes are asserted honestly.
9. Debug/Display sentinel tests; concrete error-source reachability; existing
   plain text, approval, streaming and process-recovery regressions remain green.

## Files, commands and boundaries

New Model code belongs in `crates/group-agent-model/src/structured_output/`,
facade integration in model.rs, public tests in each crate's `tests/structured_output.rs`.
Native adapter changes stay in `group-agent-genai/src/openai_chat/`; Prebuilt
contract/outcome helpers should use a small `src/structured_output.rs` module to
avoid expanding the existing large agent.rs unnecessarily. Documentation is
synchronized with implemented behavior; execution evidence is in Plan 034.

Implementation gates (not executed during specification authoring):

```bash
cargo test --locked -p group-agent-model --features structured-output --test structured_output
cargo test --locked -p group-agent-genai --features structured-output --test structured_output
cargo test --locked -p group-agent-prebuilt --features structured-output --test structured_output
cargo test --locked --workspace --all-features
cargo check --locked -p group-agent-model --no-default-features
cargo check --locked -p group-agent-genai --no-default-features
cargo check --locked -p group-agent-prebuilt --no-default-features
./scripts/verify all
git diff --check
```

Always: public-boundary tests, bounded validation, no hidden retries, scoped
changes, independent review, current documentation and MSRV verification.
Ask first: public changes beyond the approval table, changed old graph IDs,
checkpoint formats, migrations, new high-risk dependency or broader provider set.
Never: log payloads by default, silently weaken schemas, call live providers
without explicit opt-in, infer commit/push authority, claim semantic truth or
exactly-once execution.

## Sources and authorized decisions

Protocol reference checked 2026-09-26:
[OpenAI structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs).
It documents a limited schema dialect, required fields, refusal and incomplete
responses. The smaller Group profile, local validation, resource bounds and
identity scheme above are Group engineering decisions, not provider promises.
Local evidence: locked Genai 0.6.5 `openai/adapter_shared.rs`, Group's Model facade,
native request/decoder, Prebuilt graph/outcome/snapshot and existing Plan 033.

Implementation approval covers: the public additions table, optional feature
and dependency edges, bounded v1 profile, native complete/stream target, and new
contract-bound graph IDs with old snapshots/IDs unchanged. No production edits
are authorized solely by this document's existence.
