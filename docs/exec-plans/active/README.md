# Active Execution Plans

This directory contains complex work that is currently Proposed, In Progress,
or under independent review.

Before editing an area, check this directory for overlapping ownership. Update
the relevant Plan during implementation rather than creating a competing plan
or recording decisions only in chat.

Active Plans must not claim completion. Once acceptance and review are
finished, mark the Plan Completed and move it to `../completed/`.

## Plans

No active Execution Plans.

On 2026-09-15 the User cancelled publication preparation and requested deletion
of Plan 025. It was removed, not completed. Historical preflight records remain
under `docs/release/`; they do not authorize resuming publication. Code-review
corrections were approved and completed on 2026-09-15 in
[Plan 026](../completed/026-review-corrections.md).

[Plan 027](../completed/027-unify-msrv.md) completed the unified Rust 1.88 MSRV
on 2026-09-15.

[Plan 028](../completed/028-prebuilt-durability.md) completed opt-in durable
execution (checkpointed invoke, Resume, Replay, Fork) for the experimental
Prebuilt ToolCallingAgent on 2026-09-18, including the feature-gated Model
serde surface and the review corrections for both Minor findings.

[Plan 029](../completed/029-prebuilt-tool-approval.md) completed opt-in
durable human tool approval (InterruptibleNode suspension before Tool side
effects, approve/reject Resume decisions, split approval GraphVersion) plus
the additive Core `run_config()` config getters on 2026-09-18. Independent
review returned PASS with no findings; the one accepted suggestion
(cross-configuration resume tests) was implemented before closure.
