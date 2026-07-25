use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

use crate::{ModelCapability, ModelId, ModelMetadata, ProviderId, RequestValidationError};

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// High-level model failure classification.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelErrorKind {
    /// Provider-neutral request validation failed.
    InvalidRequest,
    /// The request or operation requires an undeclared capability.
    UnsupportedCapability(ModelCapability),
    /// Credentials were absent or rejected.
    Authentication,
    /// Credentials were valid but lacked permission.
    PermissionDenied,
    /// A provider rate limit was reached.
    RateLimited,
    /// A provider was temporarily unavailable.
    ProviderUnavailable,
    /// A provider or adapter deadline elapsed.
    Timeout,
    /// Streaming or wire protocol invariants were violated.
    Protocol,
    /// Provider data could not be decoded.
    Decode,
    /// The provider layer explicitly aborted work.
    Cancelled,
    /// A classified failure outside the common categories.
    Other,
}

/// Whether retry may be appropriate after a model failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Retryability {
    /// Retrying the same operation is not expected to help.
    Never,
    /// A later retry may succeed.
    Retryable,
    /// The adapter cannot classify retry safety.
    Unknown,
}

/// A source-preserving provider-neutral model error.
pub struct ModelError {
    kind: ModelErrorKind,
    message: String,
    provider: Option<ProviderId>,
    model: Option<ModelId>,
    http_status: Option<u16>,
    retry_after: Option<Duration>,
    retryability: Retryability,
    source: Option<BoxedError>,
}

impl ModelError {
    /// Creates a classified message-only error.
    #[must_use]
    pub fn new(kind: ModelErrorKind, message: impl Into<String>) -> Self {
        let retryability = default_retryability(&kind);
        Self {
            kind,
            message: message.into(),
            provider: None,
            model: None,
            http_status: None,
            retry_after: None,
            retryability,
            source: None,
        }
    }

    /// Creates a classified error while preserving its concrete source.
    #[must_use]
    pub fn with_source<E>(kind: ModelErrorKind, message: impl Into<String>, source: E) -> Self
    where
        E: Into<BoxedError>,
    {
        let mut error = Self::new(kind, message);
        error.source = Some(source.into());
        error
    }

    /// Converts provider-neutral request validation into a model invocation error.
    #[must_use]
    pub fn invalid_request(source: RequestValidationError) -> Self {
        Self::with_source(
            ModelErrorKind::InvalidRequest,
            "chat request validation failed",
            source,
        )
    }

    /// Creates an unsupported-capability error with model context.
    #[must_use]
    pub fn unsupported(capability: ModelCapability, metadata: &ModelMetadata) -> Self {
        Self::new(
            ModelErrorKind::UnsupportedCapability(capability),
            format!(
                "model `{}` does not support {capability:?}",
                metadata.model()
            ),
        )
        .with_model_context(metadata.provider().clone(), metadata.model().clone())
        .with_retryability(Retryability::Never)
    }

    /// Adds provider and model context.
    #[must_use]
    pub fn with_model_context(mut self, provider: ProviderId, model: ModelId) -> Self {
        self.provider = Some(provider);
        self.model = Some(model);
        self
    }

    /// Adds an HTTP status captured by an adapter.
    #[must_use]
    pub const fn with_http_status(mut self, status: u16) -> Self {
        self.http_status = Some(status);
        self
    }

    /// Adds a provider retry delay hint when the error can reasonably be
    /// retried.
    ///
    /// Hints on rate limits, provider unavailability, and timeouts are kept.
    /// A hint supplied for another classification is discarded.
    #[must_use]
    pub const fn with_retry_after(mut self, retry_after: Duration) -> Self {
        if matches!(
            self.kind,
            ModelErrorKind::RateLimited
                | ModelErrorKind::ProviderUnavailable
                | ModelErrorKind::Timeout
        ) {
            self.retry_after = Some(retry_after);
        }
        self
    }

    /// Overrides retry classification without implementing retry policy.
    #[must_use]
    pub const fn with_retryability(mut self, retryability: Retryability) -> Self {
        self.retryability = retryability;
        self
    }

    /// Returns the failure classification.
    #[must_use]
    pub const fn kind(&self) -> &ModelErrorKind {
        &self.kind
    }

    /// Returns the framework-level message.
    #[must_use]
    pub fn as_message(&self) -> &str {
        &self.message
    }

    /// Returns provider context when known.
    #[must_use]
    pub const fn provider(&self) -> Option<&ProviderId> {
        self.provider.as_ref()
    }

    /// Returns model context when known.
    #[must_use]
    pub const fn model(&self) -> Option<&ModelId> {
        self.model.as_ref()
    }

    /// Returns an HTTP status when an adapter observed one.
    #[must_use]
    pub const fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    /// Returns the provider retry delay hint.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Returns retry classification.
    #[must_use]
    pub const fn retryability(&self) -> Retryability {
        self.retryability
    }

    /// Returns whether the adapter classified this error as retryable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self.retryability, Retryability::Retryable)
    }
}

const fn default_retryability(kind: &ModelErrorKind) -> Retryability {
    match kind {
        ModelErrorKind::RateLimited
        | ModelErrorKind::ProviderUnavailable
        | ModelErrorKind::Timeout => Retryability::Retryable,
        ModelErrorKind::Other => Retryability::Unknown,
        ModelErrorKind::InvalidRequest
        | ModelErrorKind::UnsupportedCapability(_)
        | ModelErrorKind::Authentication
        | ModelErrorKind::PermissionDenied
        | ModelErrorKind::Protocol
        | ModelErrorKind::Decode
        | ModelErrorKind::Cancelled => Retryability::Never,
    }
}

impl fmt::Display for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "model error ({:?})", self.kind)?;
        if let Some(provider) = &self.provider {
            write!(formatter, " from provider `{provider}`")?;
        }
        if let Some(model) = &self.model {
            write!(formatter, " for model `{model}`")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelError")
            .field("kind", &self.kind)
            .field("message_redacted", &true)
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("http_status", &self.http_status)
            .field("retry_after", &self.retry_after)
            .field("retryability", &self.retryability)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl StdError for ModelError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}
