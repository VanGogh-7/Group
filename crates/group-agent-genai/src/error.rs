use std::time::Duration;

use group_agent_model::{
    ExtensionError, IdentifierError, MetadataValidationError, ModelCapability, ModelError,
    ModelErrorKind, ModelId, ProviderId, RequestValidationError, Retryability, TokenUsageError,
};
use thiserror::Error;

/// Invalid immutable adapter configuration.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GenaiAdapterConfigError {
    /// The requested genai model was empty.
    #[error("requested genai model must not be empty")]
    EmptyRequestedModel,
    /// genai 0.6.5 has no provider-neutral parallel-tool-call request control.
    #[error("genai 0.6.5 does not support parallel_tool_calls request control")]
    ParallelToolCallsUnsupported,
    /// Group metadata was internally contradictory.
    #[error("invalid model metadata")]
    InvalidMetadata(#[source] MetadataValidationError),
    /// Streaming was enabled on a client without a bound adapter kind.
    #[error("enabled streaming requires a client bound to an audited adapter")]
    StreamingClientUnbound,
    /// Streaming was enabled for an adapter kind not audited by Group.
    #[error("enabled streaming is unsupported for bound adapter `{adapter}`")]
    StreamingAdapterUnsupported {
        /// Non-sensitive genai adapter identifier.
        adapter: &'static str,
    },
    /// Adapter streaming policy contradicts declared model metadata.
    #[error("enabled streaming policy requires the streaming capability")]
    StreamingCapabilityMissing,
    /// A stable target configuration included a dynamic target resolver.
    #[error("stable service-target binding must not include a ServiceTargetResolver")]
    StableTargetResolverUnsupported,
    /// A stable target configuration did not bind the ClientConfig.
    #[error("stable service-target binding requires a bound ClientConfig")]
    StableTargetClientUnbound,
    /// A stable target disagreed with the ClientConfig adapter binding.
    #[error("stable service-target adapter does not match bound ClientConfig adapter")]
    StableTargetAdapterMismatch,
}

/// A request, response, or streaming conversion failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GenaiMappingError {
    /// A request passed to the standalone mapper was not facade-valid.
    #[error("group chat request validation failed")]
    InvalidGroupRequest(#[source] RequestValidationError),
    /// An adapter-owned extension had the wrong JSON shape.
    #[error("extension `{key}` must contain {expected}")]
    InvalidExtensionType {
        /// Stable adapter-owned key.
        key: &'static str,
        /// Public expected type description.
        expected: &'static str,
    },
    /// An unknown adapter-owned request extension was supplied.
    #[error("unknown group.genai request extension `{key}`")]
    UnknownRequestExtension {
        /// Unknown key. Values are never retained.
        key: String,
    },
    /// Response-ID continuation was disabled by adapter policy.
    #[error("response ID continuation is disabled")]
    ResponseIdContinuationDisabled,
    /// genai 0.6.5 cannot express the requested parallel-tool-call control.
    #[error("parallel_tool_calls cannot be mapped by genai 0.6.5")]
    ParallelToolCallsUnsupported,
    /// A request part is outside the current genai mapping.
    #[error("unsupported Group request content kind `{kind}`")]
    UnsupportedRequestContent {
        /// Non-sensitive variant name.
        kind: &'static str,
    },
    /// A response part is outside the current Group content model.
    #[error("unsupported genai response content kind `{kind}`")]
    UnsupportedResponseContent {
        /// Non-sensitive variant name.
        kind: &'static str,
    },
    /// Streaming is disabled for this adapter instance.
    #[error("streaming is disabled by adapter policy")]
    StreamingDisabled,
    /// The exact stream resolved to an adapter not audited by Group.
    #[error("resolved genai stream adapter `{adapter}` is unsupported")]
    ResolvedStreamingAdapterUnsupported {
        /// Non-sensitive genai adapter identifier.
        adapter: &'static str,
    },
    /// genai 0.6.5 cannot safely stream requests that may produce tool calls.
    #[error("tool streaming is unsupported by the selected genai 0.6.5 profile")]
    ToolStreamingUnsupported,
    /// Non-streaming tool generation requires an immutable service target.
    #[error("tool generation requires a stable service-target binding")]
    UntrustedToolCallBinding,
    /// A text-only stream unexpectedly emitted tool data.
    #[error("text-only genai stream unexpectedly emitted tool-call data")]
    UnexpectedToolCallInTextOnlyStream,
    /// A text-only stream unexpectedly emitted a thought signature.
    #[error("text-only genai stream unexpectedly emitted thought-signature data")]
    UnexpectedThoughtSignatureInTextOnlyStream,
    /// A non-terminal tool chunk did not contain cumulative raw JSON text.
    #[error("genai stream tool-call chunk did not contain cumulative raw JSON text")]
    UnexpectedStreamToolArgumentsKind,
    /// A provider identifier violated a Group invariant.
    #[error("invalid provider response identifier in `{field}`")]
    InvalidIdentifier {
        /// Non-sensitive field name.
        field: &'static str,
        /// Concrete validation source.
        #[source]
        source: IdentifierError,
    },
    /// A provider returned a negative token counter.
    #[error("genai usage field `{field}` contained a negative value")]
    NegativeTokenCount {
        /// Non-sensitive counter name.
        field: &'static str,
    },
    /// Provider usage violated Group accounting invariants.
    #[error("genai usage violates token accounting invariants")]
    InvalidTokenUsage(#[source] TokenUsageError),
    /// A genai usage detail object could not be represented as JSON.
    #[error("genai usage detail `{key}` could not be serialized")]
    UsageDetailSerialization {
        /// Stable adapter-owned key.
        key: &'static str,
        /// Concrete serde source.
        #[source]
        source: serde_json::Error,
    },
    /// Adapter extension construction failed.
    #[error("failed to construct adapter extension `{key}`")]
    ExtensionConstruction {
        /// Stable adapter-owned key.
        key: &'static str,
        /// Concrete extension source.
        #[source]
        source: ExtensionError,
    },
    /// Stream start occurred more than once.
    #[error("genai stream emitted Start more than once")]
    DuplicateStreamStart,
    /// A genai stream ended without its logical End event.
    #[error("genai stream reached EOF without End")]
    MissingStreamEnd,
    /// A stream tool-call identifier was absent.
    #[error("genai stream tool call has an empty call id")]
    EmptyStreamToolCallId,
    /// A stream tool call exceeded configured cardinality.
    #[error("genai stream tool-call count exceeds configured maximum {maximum}")]
    StreamToolCallLimit {
        /// Configured maximum.
        maximum: u32,
    },
    /// Cumulative genai argument data conflicted with earlier fragments.
    #[error("genai stream tool-call arguments conflict with prior chunks")]
    ConflictingStreamToolArguments,
    /// Captured terminal tool metadata conflicted with emitted chunks.
    #[error("genai terminal tool-call metadata conflicts with streamed chunks")]
    ConflictingTerminalToolCall,
    /// Terminal thought signatures conflicted with streamed signatures.
    #[error("genai terminal thought signatures conflict with streamed signatures")]
    ConflictingThoughtSignatures,
    /// Terminal reasoning conflicted with streamed reasoning.
    #[error("genai terminal reasoning conflicts with streamed reasoning")]
    ConflictingReasoningContent,
    /// Retained reasoning exceeded the configured byte limit.
    #[error("genai stream reasoning exceeds configured byte maximum {maximum}")]
    ReasoningLimitExceeded {
        /// Configured byte maximum.
        maximum: usize,
    },
    /// Retained signatures exceeded the configured byte limit.
    #[error("genai stream thought signatures exceed configured byte maximum {maximum}")]
    ThoughtSignatureLimitExceeded {
        /// Configured byte maximum.
        maximum: usize,
    },
    /// A complete genai tool argument value could not be serialized.
    #[error("genai tool arguments could not be serialized")]
    ToolArgumentsSerialization(#[source] serde_json::Error),
    /// Accumulated raw tool arguments were not valid JSON at terminal capture.
    #[error("accumulated genai tool arguments are not valid JSON")]
    InvalidAccumulatedToolArguments(#[source] serde_json::Error),
    /// A captured Responses value exceeded the post-capture parser admission limit.
    #[error("captured OpenAI Responses value exceeds parser admission byte maximum {maximum}")]
    ResponsesParserAdmissionLimitExceeded {
        /// Configured parser admission maximum.
        maximum: usize,
        /// Safe serialization-counting source.
        #[source]
        source: serde_json::Error,
    },
    /// Serialized-length accounting overflowed while measuring admission.
    #[error("captured OpenAI Responses parser admission length overflowed")]
    ResponsesParserAdmissionLengthOverflow(#[source] serde_json::Error),
    /// A captured Responses value could not be counted as JSON.
    #[error("captured OpenAI Responses value could not be measured for parser admission")]
    ResponsesParserAdmissionMeasurement(#[source] serde_json::Error),
    /// A required field in the restricted Responses schema was absent or invalid.
    #[error("captured OpenAI Responses field `{field}` is malformed")]
    InvalidResponsesRawField {
        /// Non-sensitive schema field.
        field: &'static str,
    },
    /// A raw Responses function-call argument string was invalid JSON.
    #[error("captured OpenAI Responses tool arguments are invalid JSON")]
    InvalidResponsesToolArguments(#[source] serde_json::Error),
    /// A Responses tool call was returned without the internally requested body.
    #[error("OpenAI Responses tool continuation body is unavailable")]
    MissingResponsesRawBody,
    /// Captured and normalized Responses identities disagree.
    #[error("captured OpenAI Responses identity conflicts with normalized response")]
    ConflictingResponsesIdentity,
    /// Captured and normalized Responses tool calls disagree.
    #[error("captured OpenAI Responses tool call conflicts with normalized response")]
    ConflictingResponsesToolCall,
    /// A Responses reasoning signature could not be assigned deterministically.
    #[error("captured OpenAI Responses reasoning signature has ambiguous ownership")]
    AmbiguousResponsesThoughtSignature,
    /// Signature length accounting overflowed.
    #[error("captured OpenAI Responses thought-signature length overflowed")]
    ThoughtSignatureLengthOverflow,
    /// One ToolCall exceeded the configured distinct-signature count.
    #[error("captured OpenAI Responses tool call exceeds distinct-signature maximum {maximum}")]
    ThoughtSignatureCountExceeded {
        /// Configured per-call maximum.
        maximum: usize,
    },
}

impl GenaiMappingError {
    pub(crate) fn into_model_error(self, provider: &ProviderId, model: &ModelId) -> ModelError {
        let kind = self.model_error_kind();
        let message = match kind {
            ModelErrorKind::InvalidRequest => "genai request mapping failed",
            ModelErrorKind::UnsupportedCapability(_) => {
                "genai capability combination is unsupported"
            }
            ModelErrorKind::Decode => "genai response decoding failed",
            ModelErrorKind::Protocol => "genai response protocol violation",
            _ => "genai mapping failed",
        };
        ModelError::with_source(kind, message, self)
            .with_model_context(provider.clone(), model.clone())
            .with_retryability(Retryability::Never)
    }

    fn model_error_kind(&self) -> ModelErrorKind {
        match self {
            Self::InvalidGroupRequest(_)
            | Self::InvalidExtensionType { .. }
            | Self::UnknownRequestExtension { .. }
            | Self::ResponseIdContinuationDisabled
            | Self::ParallelToolCallsUnsupported
            | Self::UnsupportedRequestContent { .. } => ModelErrorKind::InvalidRequest,
            Self::StreamingDisabled
            | Self::ResolvedStreamingAdapterUnsupported { .. }
            | Self::ToolStreamingUnsupported => {
                ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
            }
            Self::UntrustedToolCallBinding => {
                ModelErrorKind::UnsupportedCapability(ModelCapability::ToolCalling)
            }
            Self::NegativeTokenCount { .. }
            | Self::InvalidTokenUsage(_)
            | Self::UsageDetailSerialization { .. }
            | Self::ToolArgumentsSerialization(_)
            | Self::InvalidAccumulatedToolArguments(_)
            | Self::ResponsesParserAdmissionLimitExceeded { .. }
            | Self::ResponsesParserAdmissionLengthOverflow(_)
            | Self::ResponsesParserAdmissionMeasurement(_)
            | Self::InvalidResponsesRawField { .. }
            | Self::InvalidResponsesToolArguments(_) => ModelErrorKind::Decode,
            Self::UnsupportedResponseContent { .. }
            | Self::InvalidIdentifier { .. }
            | Self::ExtensionConstruction { .. }
            | Self::DuplicateStreamStart
            | Self::MissingStreamEnd
            | Self::EmptyStreamToolCallId
            | Self::StreamToolCallLimit { .. }
            | Self::ConflictingStreamToolArguments
            | Self::ConflictingTerminalToolCall
            | Self::ConflictingThoughtSignatures
            | Self::ConflictingReasoningContent
            | Self::ReasoningLimitExceeded { .. }
            | Self::ThoughtSignatureLimitExceeded { .. }
            | Self::UnexpectedToolCallInTextOnlyStream
            | Self::UnexpectedThoughtSignatureInTextOnlyStream
            | Self::UnexpectedStreamToolArgumentsKind
            | Self::MissingResponsesRawBody
            | Self::ConflictingResponsesIdentity
            | Self::ConflictingResponsesToolCall
            | Self::AmbiguousResponsesThoughtSignature
            | Self::ThoughtSignatureLengthOverflow
            | Self::ThoughtSignatureCountExceeded { .. } => ModelErrorKind::Protocol,
        }
    }
}

#[derive(Clone)]
struct ErrorFacts {
    kind: ModelErrorKind,
    status: Option<u16>,
    retry_after: Option<Duration>,
}

impl ErrorFacts {
    const fn new(kind: ModelErrorKind) -> Self {
        Self {
            kind,
            status: None,
            retry_after: None,
        }
    }

    const fn with_status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }

    const fn with_retry_after(mut self, retry_after: Option<Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }
}

pub(crate) fn map_genai_error(
    error: genai::Error,
    provider: &ProviderId,
    model: &ModelId,
) -> ModelError {
    let facts = classify_genai_error(&error);
    let mut mapped = ModelError::with_source(facts.kind, "genai provider call failed", error)
        .with_model_context(provider.clone(), model.clone());
    if let Some(status) = facts.status {
        mapped = mapped.with_http_status(status);
    }
    if let Some(retry_after) = facts.retry_after {
        mapped = mapped.with_retry_after(retry_after);
    }
    mapped
}

fn classify_genai_error(error: &genai::Error) -> ErrorFacts {
    use genai::Error;

    match error {
        Error::ChatReqHasNoMessages { .. }
        | Error::LastChatMessageIsNotUser { .. }
        | Error::MessageRoleNotSupported { .. }
        | Error::MessageContentTypeNotSupported { .. }
        | Error::JsonModeWithoutInstruction
        | Error::VerbosityParsing { .. }
        | Error::ReasoningParsingError { .. }
        | Error::ServiceTierParsing { .. }
        | Error::PromptCacheRetentionParsing { .. }
        | Error::ModelMapperFailed { .. }
        | Error::AdapterKindMismatch { .. } => ErrorFacts::new(ModelErrorKind::InvalidRequest),
        Error::RequiresApiKey { .. } | Error::NoAuthResolver { .. } | Error::NoAuthData { .. } => {
            ErrorFacts::new(ModelErrorKind::Authentication)
        }
        Error::Resolver { resolver_error, .. } => match resolver_error {
            genai::resolver::Error::ApiKeyEnvNotFound { .. }
            | genai::resolver::Error::ResolverAuthDataNotSingleValue => {
                ErrorFacts::new(ModelErrorKind::Authentication)
            }
            genai::resolver::Error::Custom(_) => ErrorFacts::new(ModelErrorKind::Other),
        },
        Error::NoChatResponse { .. }
        | Error::InvalidJsonResponseElement { .. }
        | Error::ChatResponseGeneration { .. }
        | Error::StreamParse { .. }
        | Error::SerdeJson(_) => ErrorFacts::new(ModelErrorKind::Decode),
        Error::ChatResponse { .. } | Error::JsonValueExt(_) => {
            ErrorFacts::new(ModelErrorKind::Protocol)
        }
        Error::WebAdapterCall { webc_error, .. } | Error::WebModelCall { webc_error, .. } => {
            classify_web_error(webc_error)
        }
        Error::WebStream { error, .. } => {
            if let Some(error) = error.downcast_ref::<genai::Error>() {
                classify_genai_error(error)
            } else if let Some(error) = error.downcast_ref::<genai::webc::Error>() {
                classify_web_error(error)
            } else {
                ErrorFacts::new(ModelErrorKind::ProviderUnavailable)
            }
        }
        Error::HttpError { status, .. } => classify_status(status.as_u16(), None),
        Error::AdapterNotSupported { .. } => ErrorFacts::new(ModelErrorKind::InvalidRequest),
        Error::Internal(_) => ErrorFacts::new(ModelErrorKind::Other),
    }
}

fn classify_web_error(error: &genai::webc::Error) -> ErrorFacts {
    match error {
        genai::webc::Error::ResponseFailedStatus {
            status, headers, ..
        } => {
            let retry_after = headers
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            classify_status(status.as_u16(), retry_after)
        }
        genai::webc::Error::ResponseFailedNotJson { .. }
        | genai::webc::Error::ResponseFailedInvalidJson { .. } => {
            ErrorFacts::new(ModelErrorKind::Decode)
        }
        genai::webc::Error::Reqwest(error) => {
            if error.is_timeout() {
                ErrorFacts::new(ModelErrorKind::Timeout)
            } else if let Some(status) = error.status() {
                classify_status(status.as_u16(), None)
            } else {
                ErrorFacts::new(ModelErrorKind::ProviderUnavailable)
            }
        }
        genai::webc::Error::JsonValueExt(_) => ErrorFacts::new(ModelErrorKind::Protocol),
    }
}

fn classify_status(status: u16, retry_after: Option<Duration>) -> ErrorFacts {
    let kind = match status {
        401 => ModelErrorKind::Authentication,
        403 => ModelErrorKind::PermissionDenied,
        408 => ModelErrorKind::Timeout,
        429 => ModelErrorKind::RateLimited,
        500..=599 => ModelErrorKind::ProviderUnavailable,
        400..=499 => ModelErrorKind::InvalidRequest,
        _ => ModelErrorKind::Other,
    };
    ErrorFacts::new(kind)
        .with_status(status)
        .with_retry_after(retry_after)
}

#[cfg(test)]
mod tests {
    use group_agent_model::{ModelCapability, ModelErrorKind};

    use super::GenaiMappingError;

    #[test]
    fn mapping_error_classification_is_central_and_state_aware() {
        let cases = [
            (
                GenaiMappingError::UnknownRequestExtension {
                    key: "group.genai.typo".to_owned(),
                },
                ModelErrorKind::InvalidRequest,
            ),
            (
                GenaiMappingError::ToolStreamingUnsupported,
                ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming),
            ),
            (
                GenaiMappingError::ResolvedStreamingAdapterUnsupported {
                    adapter: "openai_resp",
                },
                ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming),
            ),
            (
                GenaiMappingError::UntrustedToolCallBinding,
                ModelErrorKind::UnsupportedCapability(ModelCapability::ToolCalling),
            ),
            (
                GenaiMappingError::UnsupportedResponseContent {
                    kind: "ToolResponse",
                },
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::ConflictingStreamToolArguments,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::ConflictingTerminalToolCall,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::ConflictingThoughtSignatures,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::ConflictingReasoningContent,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::MissingStreamEnd,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::MissingResponsesRawBody,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::ConflictingResponsesIdentity,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::AmbiguousResponsesThoughtSignature,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::ThoughtSignatureLengthOverflow,
                ModelErrorKind::Protocol,
            ),
            (
                GenaiMappingError::InvalidResponsesToolArguments(
                    serde_json::from_str::<serde_json::Value>("{").expect_err("invalid JSON"),
                ),
                ModelErrorKind::Decode,
            ),
            (
                GenaiMappingError::NegativeTokenCount {
                    field: "prompt_tokens",
                },
                ModelErrorKind::Decode,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.model_error_kind(), expected);
        }
    }
}
