# ADR-009: MCP shutdown completion belongs to the Session

## Status

Accepted

## Context

If the first caller Future owns cleanup, cancelling that waiter can cancel
service close and child reap. Service and OS child cleanup are independent
failure domains.

## Decision

The Session owns one shutdown supervisor and shared completion. Service close
and direct-child cleanup run independently and always converge. The result is
stored, CLOSED is published, and only then are waiters notified. Drop remains
a separate best-effort fallback.

## Alternatives

- Let every caller run cleanup.
- Return on the first service error and reap later.
- Treat Drop as equivalent to explicit shutdown.

## Consequences

Concurrent callers observe one result, and cancelling a waiter does not cancel
cleanup. Explicit shutdown may wait for the slower path. The guarantee covers
the direct child, not a complete process tree.

## Related documents

- [MCP Adapter](../adapters/mcp.md)
- [Error, Cancellation, and Observability](../design/error-cancellation-observability.md)

