# ADR-005: The Model facade validates before adapter execution

## Status

Accepted

## Context

A facade convention is insufficient if ordinary callers can invoke a raw
adapter with an unchecked `ChatRequest`. Provider adapters must share one
validation and capability boundary.

## Decision

Applications call `ChatModel`. It validates request structure and capabilities
before constructing the non-bypassable `ValidatedChatRequest` accepted by raw
adapter methods. Stream collection validates each event before atomic commit
and remains failed after its first error.

## Alternatives

- Require each adapter to reimplement validation.
- Keep raw methods public over `ChatRequest`.
- Recover a collector after protocol conflict.

## Consequences

Adapters receive one trusted request state and cannot accidentally bypass
common checks. External adapter implementers use accessors rather than a public
unchecked constructor.

## Related documents

- [Model and Tools](../design/model-and-tools.md)
- [Genai Adapter](../adapters/genai.md)

