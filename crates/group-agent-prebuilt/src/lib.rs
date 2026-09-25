//! Experimental provider-neutral prebuilt Agent composition for Group.
//!
//! This crate is experimental. It provides a streaming and non-streaming
//! Agent backed by a private Group Core graph. It alternates model turns and
//! optional ToolRuntime-backed bounded Tool batches until a final answer or
//! the configured model round limit. Business Tool errors can continue as
//! ToolMessages; Tool infrastructure errors stop the invocation and retain the
//! complete current batch report when available.
//! Invocation may use Core's default controls or caller-supplied Core
//! cancellation, timeout, and event configuration. Future drop releases local
//! work ownership but does not prove remote cancellation or side-effect
//! rollback, and the crate performs no automatic retry.
//!
//! Durable execution is opt-in. `invoke_with_checkpoint`, `resume`, `replay`,
//! and `fork` accept Core checkpoint configuration typed over the public
//! opaque [`AgentSnapshot`], and [`AgentSnapshotCodec`] encodes snapshots as
//! canonical JSON. Plain `invoke` stays non-durable.
//!
//! Durable human approval is experimental and opt-in through
//! [`AgentConfig::with_tool_approval`]. On the durable methods, a
//! checkpoint-enabled invocation suspends before executing any Tool side
//! effect with an [`AgentApprovalRequest`] payload listing the pending calls,
//! and `resume` consumes a single-attempt [`AgentApprovalDecision`]: approve
//! executes the pending batch, while reject commits business-error
//! ToolMessages and the loop continues. The non-durable `invoke` paths fail
//! closed instead of silently skipping approval.
//!
//! Streaming execution is experimental and opt-in through [`ToolCallingAgent::stream`]
//! and [`ToolCallingAgent::invoke_with_stream_sink`]. Streaming invocations emit
//! [`AgentStreamEvent`] items—including incremental text tokens, tool call fragments,
//! tool lifecycle events, and the final completion outcome—as an asynchronous
//! [`AgentEventStream`] or to a lightweight synchronous [`AgentEventSink`]. Dropping
//! the stream or invocation drops locally owned model and tool futures without
//! leaving detached background tasks.
//!
//! [`ToolCallingAgent::stream_with_checkpoint`] and
//! [`ToolCallingAgent::invoke_with_checkpoint_stream_sink`] combine streaming
//! with durability. `ApprovalRequired` is provisional; only a terminal
//! [`AgentStreamEvent::Interrupted`] confirms a saved approval checkpoint.
//! Continue with [`ToolCallingAgent::resume_stream`] or
//! [`ToolCallingAgent::resume_with_stream_sink`] and a one-attempt approval
//! decision. The new invocation attaches its own non-persistent sink after
//! validated restore. Earlier token events are not replayed. A completed
//! checkpoint emits only Completed, without model/tool calls or a new save.
//! Streaming Replay and Fork are not provided.
//!
//! Provider adapters, MCP lifecycle, persistence, observability adapters, and
//! product policy stay outside this crate. Provider construction, MCP lifecycle
//! ownership, retry/fallback, rollback, exactly-once, structured output,
//! Memory/RAG/PDF/OCR, Multi-Agent, and middleware are not implemented. Local
//! and MCP-backed Tools use the same injected ToolRuntime boundary.
//!
//! Core, Model, and Tool retain their stable boundaries. This crate's public
//! API remains experimental, and its private State, Update, Nodes, router,
//! topology, and `CompiledGraph` are not public extension points.
//!
//! ```
//! use group_agent_prebuilt::{AgentConfig, AgentConfigError};
//!
//! let config = AgentConfig::new(4)?;
//! assert_eq!(config.max_rounds(), 4);
//! # Ok::<(), AgentConfigError>(())
//! ```
//!
//! Zero rounds are rejected at the public construction boundary:
//!
//! ```
//! use group_agent_prebuilt::{AgentConfig, AgentConfigError};
//!
//! assert_eq!(
//!     AgentConfig::new(0),
//!     Err(AgentConfigError::ZeroMaxRounds),
//! );
//! ```
//!
//! Tool execution policy is deliberately absent from version-one
//! configuration:
//!
//! ```compile_fail
//! use group_agent_prebuilt::AgentConfig;
//!
//! let config = AgentConfig::new(4).unwrap();
//! config.tool_concurrency();
//! ```
//!
//! Agent graph internals and an aggregate Agent Tool batch error are not part
//! of the Slice 1 public surface:
//!
//! ```compile_fail
//! use group_agent_prebuilt::{AgentState, AgentToolBatchError, CompiledGraph};
//! ```

mod agent;
mod agent_nodes;
mod approval;
mod codec;
mod durable_stream;
mod error;
mod outcome;
mod snapshot;
mod state;
mod stream;
mod structured_output;

pub use agent::{AgentOutcome, AgentStopReason, ToolCallingAgent};
pub use approval::{AgentApprovalDecision, AgentApprovalRequest};
pub use codec::AgentSnapshotCodec;
pub use error::{AgentBuildError, AgentError};
pub use outcome::{AgentForkReport, AgentInterrupted, AgentReplayReport, AgentRunOutcome};
pub use snapshot::AgentSnapshot;
pub use stream::{AgentEventSink, AgentEventStream, AgentStreamEvent};

/// Experimental version-one configuration for a prebuilt Tool-calling Agent.
///
/// This type is not yet a stable compatibility commitment. Version one limits
/// the configuration surface to one validated `max_rounds` value and the
/// experimental Tool-approval switch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AgentConfig {
    max_rounds: usize,
    tool_approval: bool,
}

impl AgentConfig {
    /// Creates experimental Agent configuration with a valid round cap.
    ///
    /// The round cap must be positive and small enough that a later internal
    /// Core step bound of `2 * max_rounds` can be represented by `usize`.
    /// That derived bound is validated but is not stored or exposed.
    ///
    /// # Errors
    ///
    /// Returns [`AgentConfigError::ZeroMaxRounds`] for zero, or
    /// [`AgentConfigError::MaxStepsOverflow`] when doubling the value would
    /// overflow `usize`.
    pub const fn new(max_rounds: usize) -> Result<Self, AgentConfigError> {
        if max_rounds == 0 {
            return Err(AgentConfigError::ZeroMaxRounds);
        }
        if max_rounds.checked_mul(2).is_none() {
            return Err(AgentConfigError::MaxStepsOverflow);
        }
        Ok(Self {
            max_rounds,
            tool_approval: false,
        })
    }

    /// Enables or disables experimental durable human approval before Tool
    /// execution.
    ///
    /// When enabled, a checkpoint-enabled invocation durably suspends before
    /// executing any Tool side effect and resumes with an
    /// [`crate::AgentApprovalDecision`]. The non-durable `invoke` paths fail
    /// closed instead of silently skipping approval.
    #[must_use]
    pub const fn with_tool_approval(mut self, tool_approval: bool) -> Self {
        self.tool_approval = tool_approval;
        self
    }

    /// Returns the maximum number of successfully committed model rounds.
    #[must_use]
    pub const fn max_rounds(self) -> usize {
        self.max_rounds
    }

    /// Returns whether Tool execution requires durable human approval.
    #[must_use]
    pub const fn tool_approval(&self) -> bool {
        self.tool_approval
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            max_rounds: 8,
            tool_approval: false,
        }
    }
}

/// Experimental construction error for [`AgentConfig`].
///
/// The variants expose only stable configuration classifications. Default
/// formatting contains no caller payload or internal implementation detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentConfigError {
    /// The requested model-round limit was zero.
    ZeroMaxRounds,
    /// Doubling the round limit for a private Core step bound would overflow.
    MaxStepsOverflow,
}

impl std::fmt::Display for AgentConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroMaxRounds => formatter.write_str("max_rounds must be greater than zero"),
            Self::MaxStepsOverflow => {
                formatter.write_str("max_rounds is too large for the internal step bound")
            }
        }
    }
}

impl std::error::Error for AgentConfigError {}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::{AgentConfig, AgentConfigError};

    #[test]
    fn zero_rounds_are_rejected() {
        assert_eq!(AgentConfig::new(0), Err(AgentConfigError::ZeroMaxRounds));
    }

    #[test]
    fn one_round_is_valid() {
        let config = AgentConfig::new(1).expect("one round is valid");

        assert_eq!(config.max_rounds(), 1);
    }

    #[test]
    fn ordinary_round_limit_is_preserved() {
        let config = AgentConfig::new(17).expect("ordinary round limit is valid");

        assert_eq!(config.max_rounds(), 17);
    }

    #[test]
    fn default_round_limit_is_valid() {
        assert_eq!(AgentConfig::default().max_rounds(), 8);
    }

    #[test]
    fn overflowing_internal_step_bound_is_rejected() {
        let first_overflowing = (usize::MAX / 2) + 1;

        assert_eq!(
            AgentConfig::new(first_overflowing),
            Err(AgentConfigError::MaxStepsOverflow)
        );
        assert_eq!(
            AgentConfig::new(usize::MAX),
            Err(AgentConfigError::MaxStepsOverflow)
        );
    }

    #[test]
    fn tool_approval_defaults_to_off() {
        let config = AgentConfig::new(4).expect("round limit is valid");

        assert!(!config.tool_approval());
        assert!(!AgentConfig::default().tool_approval());
    }

    #[test]
    fn tool_approval_builder_and_getter_round_trip() {
        let enabled = AgentConfig::new(4)
            .expect("round limit is valid")
            .with_tool_approval(true);
        assert!(enabled.tool_approval());
        assert_eq!(enabled.max_rounds(), 4);

        let disabled = enabled.with_tool_approval(false);
        assert!(!disabled.tool_approval());
        assert_ne!(
            AgentConfig::new(4).expect("round limit is valid"),
            enabled,
            "the approval flag participates in equality"
        );
    }

    #[test]
    fn config_error_formatting_is_classified_and_source_free() {
        let zero = AgentConfigError::ZeroMaxRounds;
        let overflow = AgentConfigError::MaxStepsOverflow;

        assert_eq!(zero.to_string(), "max_rounds must be greater than zero");
        assert_eq!(
            overflow.to_string(),
            "max_rounds is too large for the internal step bound"
        );
        assert!(zero.source().is_none());
        assert!(overflow.source().is_none());
    }
}

#[cfg(test)]
mod agent_tests;

#[cfg(feature = "agent-sequence")]
mod sequence;
#[cfg(feature = "agent-sequence")]
pub use sequence::*;
