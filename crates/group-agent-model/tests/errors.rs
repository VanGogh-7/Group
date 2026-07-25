use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

use group_agent_model::{
    ModelCapability, ModelError, ModelErrorKind, ModelId, ProviderId, Retryability,
};

#[test]
fn every_error_kind_has_a_documented_default_retryability() {
    let cases = [
        (ModelErrorKind::InvalidRequest, Retryability::Never),
        (
            ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming),
            Retryability::Never,
        ),
        (ModelErrorKind::Authentication, Retryability::Never),
        (ModelErrorKind::PermissionDenied, Retryability::Never),
        (ModelErrorKind::RateLimited, Retryability::Retryable),
        (ModelErrorKind::ProviderUnavailable, Retryability::Retryable),
        (ModelErrorKind::Timeout, Retryability::Retryable),
        (ModelErrorKind::Protocol, Retryability::Never),
        (ModelErrorKind::Decode, Retryability::Never),
        (ModelErrorKind::Cancelled, Retryability::Never),
        (ModelErrorKind::Other, Retryability::Unknown),
    ];

    for (kind, expected) in cases {
        let error = ModelError::new(kind, "details");
        assert_eq!(error.retryability(), expected);
        assert_eq!(error.is_retryable(), expected == Retryability::Retryable);
    }
}

#[test]
fn context_status_and_reasonable_retry_hint_round_trip() {
    let provider = ProviderId::new("provider").expect("valid provider");
    let model = ModelId::new("model").expect("valid model");
    let error = ModelError::new(ModelErrorKind::RateLimited, "details")
        .with_model_context(provider.clone(), model.clone())
        .with_http_status(429)
        .with_retry_after(Duration::from_secs(3));

    assert_eq!(error.provider(), Some(&provider));
    assert_eq!(error.model(), Some(&model));
    assert_eq!(error.http_status(), Some(429));
    assert_eq!(error.retry_after(), Some(Duration::from_secs(3)));

    let invalid = ModelError::new(ModelErrorKind::InvalidRequest, "details")
        .with_retry_after(Duration::from_secs(3));
    assert_eq!(invalid.retry_after(), None);
}

#[derive(Debug)]
struct Root;

impl fmt::Display for Root {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("root")
    }
}

impl StdError for Root {}

#[test]
fn source_chain_reaches_the_concrete_root() {
    let error = ModelError::with_source(ModelErrorKind::Other, "details", Root);

    assert!(error.source().is_some_and(|source| source.is::<Root>()));
}
