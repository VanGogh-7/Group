mod support;

use group_agent_genai::extensions::PREVIOUS_RESPONSE_ID;
use group_agent_genai::{
    GenaiAdapterConfig, GenaiAdapterConfigError, GenaiChatModelAdapter, GenaiModelConfig,
};
use group_agent_model::{Extensions, ModelCapabilities, ModelId, ProviderId};
use serde_json::json;
use support::openai_client;

#[test]
fn unsupported_capability_fails_during_configuration() {
    let error = GenaiModelConfig::new(
        "model",
        ProviderId::new("provider").expect("provider"),
        ModelId::new("model").expect("model"),
        ModelCapabilities::new()
            .with_tool_calling(true)
            .with_parallel_tool_calls(true),
    )
    .expect_err("parallel request control is absent in genai 0.6.5");
    assert!(matches!(
        error,
        GenaiAdapterConfigError::ParallelToolCallsUnsupported
    ));
}

#[test]
fn public_debug_never_reveals_injected_client_auth() {
    let client_secret = "client-auth-secret-sentinel";
    let client = {
        use genai::resolver::{AuthData, AuthResolver};
        let secret = client_secret.to_owned();
        genai::Client::builder()
            .with_auth_resolver(AuthResolver::from_resolver_fn(move |_| {
                Ok(Some(AuthData::from_single(secret.clone())))
            }))
            .build()
    };
    let model = GenaiModelConfig::new(
        "requested-model",
        ProviderId::new("provider").expect("provider"),
        ModelId::new("model").expect("model"),
        ModelCapabilities::new(),
    )
    .expect("config");
    let adapter =
        GenaiChatModelAdapter::new(client, GenaiAdapterConfig::new(model)).expect("adapter");
    let debug = format!("{adapter:?}");
    assert!(debug.contains("requested-model"));
    assert!(!debug.contains(client_secret));

    let fallback_client = openai_client("http://127.0.0.1:1/v1/");
    let model = GenaiModelConfig::new(
        "model",
        ProviderId::new("provider").expect("provider"),
        ModelId::new("model").expect("model"),
        ModelCapabilities::new(),
    )
    .expect("config");
    let debug = format!(
        "{:?}",
        GenaiChatModelAdapter::new(fallback_client, GenaiAdapterConfig::new(model))
            .expect("adapter")
    );
    assert!(!debug.contains("local-test-only"));
}

#[test]
fn continuation_extension_debug_redacts_the_response_id_value() {
    let sentinel = "previous-response-id-secret-sentinel";
    let extensions = Extensions::new()
        .with(PREVIOUS_RESPONSE_ID, json!(sentinel))
        .expect("extension");
    let debug = format!("{extensions:?}");
    assert!(debug.contains(PREVIOUS_RESPONSE_ID));
    assert!(!debug.contains(sentinel));
}
