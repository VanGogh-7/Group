//! Experimental provider-neutral prebuilt Agent composition for Group.
//!
//! This crate is experimental. It currently provides a minimal non-streaming
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
//! Provider adapters, MCP lifecycle, persistence, observability adapters, and
//! product policy stay outside this crate. Streaming orchestration, built-in
//! durability codecs or resume/replay/fork, provider construction, MCP
//! lifecycle ownership, retry/fallback, rollback, exactly-once, approval,
//! structured output, Memory/RAG/PDF/OCR, Multi-Agent, and middleware are not
//! implemented. Local and MCP-backed Tools use the same injected ToolRuntime
//! boundary.
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
mod error;
mod state;

pub use agent::{AgentOutcome, AgentStopReason, ToolCallingAgent};
pub use error::{AgentBuildError, AgentError};

/// Experimental version-one configuration for a prebuilt Tool-calling Agent.
///
/// This type is not yet a stable compatibility commitment. Version one limits
/// the configuration surface to one validated `max_rounds` value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AgentConfig {
    max_rounds: usize,
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
        Ok(Self { max_rounds })
    }

    /// Returns the maximum number of successfully committed model rounds.
    #[must_use]
    pub const fn max_rounds(self) -> usize {
        self.max_rounds
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self { max_rounds: 8 }
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
