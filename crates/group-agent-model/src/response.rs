use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::{AssistantMessage, ExtensionMergeError, Extensions, IdentifierError, ModelId};

/// A stable provider response identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResponseId(Arc<str>);

impl ResponseId {
    /// Creates a validated response identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(IdentifierError::EmptyResponseId);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Returns the identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResponseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ResponseId {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Why a model stopped producing output.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum FinishReason {
    /// Natural stop or stop sequence.
    Stop,
    /// Output length limit.
    Length,
    /// The assistant requested one or more tools.
    ToolCalls,
    /// Provider content filtering.
    ContentFilter,
    /// Provider reported a logical response error.
    Error,
    /// A provider-defined reason not covered by common variants.
    Other(String),
}

impl fmt::Debug for FinishReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stop => formatter.write_str("Stop"),
            Self::Length => formatter.write_str("Length"),
            Self::ToolCalls => formatter.write_str("ToolCalls"),
            Self::ContentFilter => formatter.write_str("ContentFilter"),
            Self::Error => formatter.write_str("Error"),
            Self::Other(value) => formatter
                .debug_struct("Other")
                .field("bytes", &value.len())
                .finish(),
        }
    }
}

/// Partial or complete token accounting for one response.
///
/// Each common counter is independently optional. Explicit totals may exceed
/// input plus output because providers can include other token categories, but
/// cannot be smaller than a known component or the checked sum of both known
/// components.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct TokenUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    extensions: Extensions,
}

impl TokenUsage {
    /// Creates usage with all counters unknown.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            extensions: Extensions::new(),
        }
    }

    /// Creates and validates independently optional counters.
    pub fn from_parts(
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        total_tokens: Option<u64>,
    ) -> Result<Self, TokenUsageError> {
        let usage = Self {
            input_tokens,
            output_tokens,
            total_tokens,
            extensions: Extensions::new(),
        };
        usage.validate()?;
        Ok(usage)
    }

    /// Adds provider-specific usage categories.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Returns input tokens when reported.
    #[must_use]
    pub const fn input_tokens(&self) -> Option<u64> {
        self.input_tokens
    }

    /// Returns output tokens when reported.
    #[must_use]
    pub const fn output_tokens(&self) -> Option<u64> {
        self.output_tokens
    }

    /// Returns an explicit provider total when reported.
    #[must_use]
    pub const fn total_tokens(&self) -> Option<u64> {
        self.total_tokens
    }

    /// Returns provider-specific usage categories.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Computes input plus output only when both are known.
    pub fn checked_computed_total(&self) -> Result<Option<u64>, TokenUsageError> {
        match (self.input_tokens, self.output_tokens) {
            (Some(input_tokens), Some(output_tokens)) => input_tokens
                .checked_add(output_tokens)
                .map(Some)
                .ok_or(TokenUsageError::TotalOverflow {
                    input_tokens,
                    output_tokens,
                }),
            _ => Ok(None),
        }
    }

    /// Returns the explicit total, or the checked computed total when possible.
    pub fn effective_total(&self) -> Result<Option<u64>, TokenUsageError> {
        match self.total_tokens {
            Some(total) => Ok(Some(total)),
            None => self.checked_computed_total(),
        }
    }

    /// Atomically merges a cumulative snapshot into this usage value.
    ///
    /// `Some` counters must not decrease, `None` retains the existing value,
    /// and totals must remain consistent. All counters and extension conflicts
    /// are validated before mutation. Existing extension values are not
    /// cloned; only new values are moved into this collection.
    pub fn merge_snapshot(&mut self, snapshot: Self) -> Result<(), TokenUsageError> {
        let input_tokens = merge_counter("input_tokens", self.input_tokens, snapshot.input_tokens)?;
        let output_tokens =
            merge_counter("output_tokens", self.output_tokens, snapshot.output_tokens)?;
        let total_tokens = merge_counter("total_tokens", self.total_tokens, snapshot.total_tokens)?;

        Self {
            input_tokens,
            output_tokens,
            total_tokens,
            extensions: Extensions::new(),
        }
        .validate()?;
        self.extensions
            .validate_idempotent_merge(&snapshot.extensions)
            .map_err(TokenUsageError::ExtensionConflict)?;

        self.input_tokens = input_tokens;
        self.output_tokens = output_tokens;
        self.total_tokens = total_tokens;
        self.extensions.commit_idempotent_merge(snapshot.extensions);
        Ok(())
    }

    fn validate(&self) -> Result<(), TokenUsageError> {
        let computed = self.checked_computed_total()?;
        if let Some(total_tokens) = self.total_tokens {
            let lower_bound = computed
                .or(self.input_tokens)
                .or(self.output_tokens)
                .unwrap_or(0);
            if total_tokens < lower_bound {
                return Err(TokenUsageError::InconsistentTotal {
                    input_tokens: self.input_tokens,
                    output_tokens: self.output_tokens,
                    total_tokens,
                });
            }
            if self.input_tokens.is_some_and(|input| total_tokens < input)
                || self
                    .output_tokens
                    .is_some_and(|output| total_tokens < output)
            {
                return Err(TokenUsageError::InconsistentTotal {
                    input_tokens: self.input_tokens,
                    output_tokens: self.output_tokens,
                    total_tokens,
                });
            }
        }
        Ok(())
    }
}

impl fmt::Debug for TokenUsage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenUsage")
            .field("input_tokens", &self.input_tokens)
            .field("output_tokens", &self.output_tokens)
            .field("total_tokens", &self.total_tokens)
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// Invalid token accounting or cumulative usage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TokenUsageError {
    /// Input plus output overflowed `u64`.
    #[error("token total overflowed: input={input_tokens}, output={output_tokens}")]
    TotalOverflow {
        input_tokens: u64,
        output_tokens: u64,
    },
    /// An explicit total was smaller than known accounting.
    #[error(
        "token total is inconsistent: input={input_tokens:?}, output={output_tokens:?}, \
         total={total_tokens}"
    )]
    InconsistentTotal {
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        total_tokens: u64,
    },
    /// A cumulative usage snapshot decreased a known counter.
    #[error("cumulative usage field `{field}` decreased from {previous} to {current}")]
    CounterDecreased {
        field: &'static str,
        previous: u64,
        current: u64,
    },
    /// Provider-specific usage metadata conflicted between snapshots.
    #[error("usage extension conflict: {0}")]
    ExtensionConflict(#[source] ExtensionMergeError),
}

/// A complete provider-neutral chat response.
#[derive(Clone, PartialEq)]
pub struct ChatResponse {
    message: AssistantMessage,
    finish_reason: FinishReason,
    usage: Option<TokenUsage>,
    response_id: Option<ResponseId>,
    model: Option<ModelId>,
    extensions: Extensions,
}

impl ChatResponse {
    /// Creates a response with optional identity and usage left unknown.
    #[must_use]
    pub fn new(message: AssistantMessage, finish_reason: FinishReason) -> Self {
        Self {
            message,
            finish_reason,
            usage: None,
            response_id: None,
            model: None,
            extensions: Extensions::new(),
        }
    }

    /// Adds reported usage, including partial usage.
    #[must_use]
    pub fn with_usage(mut self, usage: TokenUsage) -> Self {
        self.usage = Some(usage);
        self
    }

    /// Adds a provider response identifier.
    #[must_use]
    pub fn with_response_id(mut self, response_id: ResponseId) -> Self {
        self.response_id = Some(response_id);
        self
    }

    /// Adds the actual model identifier reported for the response.
    #[must_use]
    pub fn with_model(mut self, model: ModelId) -> Self {
        self.model = Some(model);
        self
    }

    /// Adds provider-neutral response metadata.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Returns the assistant message.
    #[must_use]
    pub const fn message(&self) -> &AssistantMessage {
        &self.message
    }

    /// Returns the finish reason.
    #[must_use]
    pub const fn finish_reason(&self) -> &FinishReason {
        &self.finish_reason
    }

    /// Returns reported usage, or `None` when no usage event was supplied.
    #[must_use]
    pub const fn usage(&self) -> Option<&TokenUsage> {
        self.usage.as_ref()
    }

    /// Returns the provider response identifier when available.
    #[must_use]
    pub const fn response_id(&self) -> Option<&ResponseId> {
        self.response_id.as_ref()
    }

    /// Returns the actual model identifier when reported.
    #[must_use]
    pub const fn model(&self) -> Option<&ModelId> {
        self.model.as_ref()
    }

    /// Returns provider-neutral response metadata.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl fmt::Debug for ChatResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatResponse")
            .field("message", &self.message)
            .field("finish_reason", &self.finish_reason)
            .field("usage", &self.usage)
            .field("response_id", &self.response_id)
            .field("model", &self.model)
            .field("extensions", &self.extensions)
            .finish()
    }
}

fn merge_counter(
    field: &'static str,
    previous: Option<u64>,
    current: Option<u64>,
) -> Result<Option<u64>, TokenUsageError> {
    match (previous, current) {
        (Some(previous), Some(current)) if current < previous => {
            Err(TokenUsageError::CounterDecreased {
                field,
                previous,
                current,
            })
        }
        (_, Some(current)) => Ok(Some(current)),
        (previous, None) => Ok(previous),
    }
}
