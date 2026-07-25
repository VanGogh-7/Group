mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::StreamExt;
use genai::adapter::AdapterKind;
use genai::chat::{
    ChatOptions as GenaiOptions, ChatRequest as GenaiRequest, ChatStreamEvent as GenaiStreamEvent,
    Tool as GenaiTool,
};
use genai::resolver::{AuthData, Endpoint, ServiceTargetResolver};
use genai::{ModelIden, ServiceTarget};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiAdapterConfigError, GenaiChatModelAdapter, GenaiModelConfig,
    GenaiStreamingPolicy,
};
use group_agent_model::{
    ChatModel, ChatRequest, Extensions, Message, ModelCapabilities, ModelCapability,
    ModelErrorKind, ModelId, ProviderId, ToolChoice, ToolDefinition, ToolName,
};
use serde_json::json;
use support::{MockResponse, MockServer, openai_client, responses_model, stable_openai_model};

fn capabilities() -> ModelCapabilities {
    ModelCapabilities::new()
        .with_streaming(true)
        .with_tool_calling(true)
}

fn group_tool() -> ToolDefinition {
    ToolDefinition::new(
        ToolName::new("lookup").expect("tool name"),
        "Lookup data",
        json!({"type":"object","properties":{}}),
    )
}

fn tool_request(choice: ToolChoice) -> ChatRequest {
    ChatRequest::new(vec![Message::user("hello")])
        .with_tools(vec![group_tool()])
        .with_tool_choice(choice)
}

fn configured_model(
    client: genai::Client,
    policy: GenaiStreamingPolicy,
) -> Result<ChatModel, Box<dyn std::error::Error>> {
    let model = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("local-openai")?,
        ModelId::new("configured-model")?,
        capabilities(),
    )?;
    let adapter = GenaiChatModelAdapter::new(
        client,
        GenaiAdapterConfig::new(model).with_streaming_policy(policy),
    )?;
    Ok(ChatModel::from_adapter(adapter)?)
}

fn redirected_client(
    base_url: impl Into<String>,
    bound: AdapterKind,
    actual: AdapterKind,
) -> genai::Client {
    let base_url = base_url.into();
    let resolver = ServiceTargetResolver::from_resolver_fn(
        move |target: ServiceTarget| -> Result<ServiceTarget, genai::resolver::Error> {
            Ok(ServiceTarget {
                endpoint: Endpoint::from_owned(base_url.clone()),
                auth: AuthData::from_single("local-test-only"),
                model: ModelIden::new(actual, target.model.model_name),
            })
        },
    );
    genai::Client::builder()
        .with_adapter_kind(bound)
        .with_service_target_resolver(resolver)
        .build()
}

#[tokio::test]
async fn genai_065_chat_stream_drops_the_second_tool_delta_in_one_sse_event() {
    let server = MockServer::start(MockResponse::sse(concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[",
        "{\"index\":0,\"id\":\"call-a\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}},",
        "{\"index\":1,\"id\":\"call-b\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}",
        "]},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n"
    )))
    .await
    .expect("server");
    let client = openai_client(server.base_url());
    let request = GenaiRequest::from_user("hello").with_tools([
        GenaiTool::new("lookup").with_schema(json!({"type":"object","properties":{}}))
    ]);
    let response = client
        .exec_chat_stream(
            "gpt-4o-mini",
            request,
            Some(&GenaiOptions::default().with_capture_tool_calls(true)),
        )
        .await
        .expect("stream initialization");
    let mut stream = response.stream;
    let mut call_ids = Vec::new();
    while let Some(event) = stream.next().await {
        if let GenaiStreamEvent::ToolCallChunk(chunk) = event.expect("genai event") {
            call_ids.push(chunk.tool_call.call_id);
        }
    }
    assert!(call_ids.iter().any(|id| id == "call-a"));
    assert!(
        !call_ids.iter().any(|id| id == "call-b"),
        "genai 0.6.5 only reads the first tool delta from one SSE event"
    );
}

#[tokio::test]
async fn group_rejects_chat_tools_before_network_but_complete_still_works() {
    let stream_server = MockServer::start(MockResponse::sse("data: [DONE]\n\n"))
        .await
        .expect("server");
    let stream_model = configured_model(
        openai_client(stream_server.base_url()),
        GenaiStreamingPolicy::TextOnly,
    )
    .expect("model");
    let request = ChatRequest::new(vec![Message::user("hello")]).with_tools(vec![group_tool()]);
    let error = match stream_model.stream(request).await {
        Ok(_) => panic!("tool streaming must fail closed"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
    );
    assert_eq!(stream_server.hit_count(), 0);

    let complete_server = MockServer::start(MockResponse::json(
        r#"{
          "id":"chatcmpl-tool",
          "model":"provider-model",
          "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]
        }"#,
    ))
    .await
    .expect("server");
    let complete_model =
        stable_openai_model(complete_server.base_url(), capabilities()).expect("model");
    let response = complete_model
        .complete(ChatRequest::new(vec![Message::user("hello")]).with_tools(vec![group_tool()]))
        .await
        .expect("non-streaming tool request remains supported");
    assert_eq!(response.message().text_content(), "ok");
    assert_eq!(complete_server.hit_count(), 1);
}

#[tokio::test]
async fn every_tool_choice_that_can_call_tools_is_rejected_before_network() {
    let choices = [
        ToolChoice::Auto,
        ToolChoice::Required,
        ToolChoice::Named(ToolName::new("lookup").expect("tool name")),
    ];
    for choice in choices {
        let stream_server = MockServer::start(MockResponse::sse("data: [DONE]\n\n"))
            .await
            .expect("stream server");
        let stream_model = configured_model(
            openai_client(stream_server.base_url()),
            GenaiStreamingPolicy::TextOnly,
        )
        .expect("stream model");
        let error = match stream_model.stream(tool_request(choice.clone())).await {
            Ok(_) => panic!("tool streaming must fail closed"),
            Err(error) => error,
        };
        assert_eq!(
            error.kind(),
            &ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
        );
        assert_eq!(stream_server.hit_count(), 0);

        let complete_server = MockServer::start(MockResponse::json(
            r#"{
              "id":"chatcmpl-tool",
              "model":"provider-model",
              "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]
            }"#,
        ))
        .await
        .expect("complete server");
        let complete_model = stable_openai_model(complete_server.base_url(), capabilities())
            .expect("complete model");
        complete_model
            .complete(tool_request(choice))
            .await
            .expect("non-streaming request remains supported");
        assert_eq!(complete_server.hit_count(), 1);
    }
}

#[tokio::test]
async fn text_only_policy_allows_plain_openai_chat_streaming() {
    let server = MockServer::start(MockResponse::sse(concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"safe\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n"
    )))
    .await
    .expect("server");
    let model = configured_model(
        openai_client(server.base_url()),
        GenaiStreamingPolicy::TextOnly,
    )
    .expect("model");
    let mut stream = model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("plain text stream");
    let mut text = String::new();
    while let Some(event) = stream.next().await {
        if let group_agent_model::ChatStreamEvent::TextDelta(chunk) = event.expect("event") {
            text.push_str(&chunk);
        }
    }
    assert_eq!(text, "safe");
    assert_eq!(server.hit_count(), 1);
}

#[tokio::test]
async fn disabled_policy_rejects_text_streaming_before_network() {
    let server = MockServer::start(MockResponse::sse("data: [DONE]\n\n"))
        .await
        .expect("server");
    let model_config = GenaiModelConfig::new(
        "gpt-4o-mini",
        ProviderId::new("local-openai").expect("provider"),
        ModelId::new("configured-model").expect("model"),
        capabilities(),
    )
    .expect("model config");
    let adapter = GenaiChatModelAdapter::new(
        openai_client(server.base_url()),
        GenaiAdapterConfig::new(model_config),
    )
    .expect("adapter");
    let model = ChatModel::from_adapter(adapter).expect("facade");
    let error = match model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
    {
        Ok(_) => panic!("disabled streaming must fail closed"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
    );
    assert_eq!(server.accepted_connection_count(), 0);
    assert_eq!(server.hit_count(), 0);
}

#[tokio::test]
async fn resolved_responses_adapter_is_rejected_before_the_lazy_stream_is_polled() {
    let server = MockServer::start(MockResponse::sse(
        "data: {\"type\":\"response.completed\"}\n\n",
    ))
    .await
    .expect("server");
    let endpoint = server.base_url().to_owned();
    let resolver = ServiceTargetResolver::from_resolver_fn(
        move |target: ServiceTarget| -> Result<ServiceTarget, genai::resolver::Error> {
            Ok(ServiceTarget {
                endpoint: Endpoint::from_owned(endpoint.clone()),
                auth: AuthData::from_single("local-test-only"),
                model: ModelIden::new(AdapterKind::OpenAIResp, target.model.model_name),
            })
        },
    );
    let client = genai::Client::builder()
        .with_adapter_kind(AdapterKind::OpenAI)
        .with_service_target_resolver(resolver)
        .build();
    let model = configured_model(client, GenaiStreamingPolicy::TextOnly).expect("model");

    let error = match model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
    {
        Ok(_) => panic!("resolved Responses stream must fail closed"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
    );
    assert_eq!(server.accepted_connection_count(), 0);
    assert_eq!(server.hit_count(), 0);
}

#[tokio::test]
async fn changing_resolver_is_checked_against_each_exact_returned_stream() {
    let safe_server = MockServer::start(MockResponse::sse(concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"safe\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n"
    )))
    .await
    .expect("safe server");
    let unsafe_server = MockServer::start(MockResponse::sse(
        "data: {\"type\":\"response.completed\"}\n\n",
    ))
    .await
    .expect("unsafe server");
    let safe_endpoint = safe_server.base_url().to_owned();
    let unsafe_endpoint = unsafe_server.base_url().to_owned();
    let resolutions = Arc::new(AtomicUsize::new(0));
    let resolver_count = Arc::clone(&resolutions);
    let resolver = ServiceTargetResolver::from_resolver_fn(
        move |target: ServiceTarget| -> Result<ServiceTarget, genai::resolver::Error> {
            let first = resolver_count.fetch_add(1, Ordering::SeqCst) == 0;
            Ok(ServiceTarget {
                endpoint: Endpoint::from_owned(if first {
                    safe_endpoint.clone()
                } else {
                    unsafe_endpoint.clone()
                }),
                auth: AuthData::from_single("local-test-only"),
                model: ModelIden::new(
                    if first {
                        AdapterKind::OpenAI
                    } else {
                        AdapterKind::OpenAIResp
                    },
                    target.model.model_name,
                ),
            })
        },
    );
    let client = genai::Client::builder()
        .with_adapter_kind(AdapterKind::OpenAI)
        .with_service_target_resolver(resolver)
        .build();
    let model = configured_model(client, GenaiStreamingPolicy::TextOnly).expect("model");

    let first = model
        .stream(ChatRequest::new(vec![Message::user("first")]))
        .await
        .expect("first exact stream is audited");
    let events = first.collect::<Vec<_>>().await;
    assert!(events.iter().all(Result::is_ok));
    assert_eq!(safe_server.hit_count(), 1);

    let error = match model
        .stream(ChatRequest::new(vec![Message::user("second")]))
        .await
    {
        Ok(_) => panic!("second exact stream resolves to Responses"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
    );
    assert_eq!(resolutions.load(Ordering::SeqCst), 2);
    assert_eq!(unsafe_server.accepted_connection_count(), 0);
    assert_eq!(unsafe_server.hit_count(), 0);
}

#[tokio::test]
async fn extensions_cannot_override_the_bound_protocol() {
    let server = MockServer::start(MockResponse::sse("data: [DONE]\n\n"))
        .await
        .expect("server");
    let model = configured_model(
        openai_client(server.base_url()),
        GenaiStreamingPolicy::TextOnly,
    )
    .expect("model");
    let mut extensions = Extensions::new();
    extensions
        .insert("group.genai.protocol_profile", json!("openai"))
        .expect("syntactically valid extension");
    let error = match model
        .stream(ChatRequest::new(vec![Message::user("hello")]).with_extensions(extensions))
        .await
    {
        Ok(_) => panic!("adapter-owned unknown key must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), &ModelErrorKind::InvalidRequest);
    assert_eq!(server.hit_count(), 0);
}

#[tokio::test]
async fn dynamic_non_streaming_resolver_is_text_only_and_tool_calls_fail_before_network() {
    let text_server = MockServer::start(MockResponse::json(
        r#"{
          "id":"chat-text",
          "model":"provider-model",
          "choices":[{"message":{"role":"assistant","content":"chat text"},"finish_reason":"stop"}]
        }"#,
    ))
    .await
    .expect("text server");
    let text_model = responses_model(
        redirected_client(
            text_server.base_url(),
            AdapterKind::OpenAIResp,
            AdapterKind::OpenAI,
        ),
        capabilities(),
    )
    .expect("dynamic text model");
    let response = text_model
        .complete(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("ordinary text complete");
    assert_eq!(response.message().text_content(), "chat text");
    assert_eq!(text_server.accepted_connection_count(), 1);
    assert_eq!(text_server.hit_count(), 1);

    for choice in [
        ToolChoice::Auto,
        ToolChoice::Required,
        ToolChoice::Named(ToolName::new("lookup").expect("tool name")),
    ] {
        for (bound, actual) in [
            (AdapterKind::OpenAIResp, AdapterKind::OpenAI),
            (AdapterKind::OpenAI, AdapterKind::OpenAIResp),
        ] {
            let server = MockServer::start(MockResponse::json(
                r#"{"id":"must-not-run","model":"provider-model","output":[]}"#,
            ))
            .await
            .expect("server");
            let model = if matches!(bound, AdapterKind::OpenAIResp) {
                responses_model(
                    redirected_client(server.base_url(), bound, actual),
                    capabilities(),
                )
                .expect("responses model")
            } else {
                configured_model(
                    redirected_client(server.base_url(), bound, actual),
                    GenaiStreamingPolicy::TextOnly,
                )
                .expect("chat model")
            };
            let error = model
                .complete(tool_request(choice.clone()))
                .await
                .expect_err("dynamic tool generation must fail closed");
            assert_eq!(
                error.kind(),
                &ModelErrorKind::UnsupportedCapability(ModelCapability::ToolCalling)
            );
            assert_eq!(server.accepted_connection_count(), 0);
            assert_eq!(server.hit_count(), 0);
        }
    }
}

#[tokio::test]
async fn extensions_cannot_enable_signature_recovery_on_a_dynamic_client() {
    let server = MockServer::start(MockResponse::json(
        r#"{"id":"must-not-run","model":"provider-model","output":[]}"#,
    ))
    .await
    .expect("server");
    let model = responses_model(
        redirected_client(
            server.base_url(),
            AdapterKind::OpenAIResp,
            AdapterKind::OpenAIResp,
        ),
        capabilities(),
    )
    .expect("model");
    let mut extensions = Extensions::new();
    extensions
        .insert("group.genai.stable_target", json!(true))
        .expect("syntactically valid extension");
    let error = model
        .complete(tool_request(ToolChoice::Auto).with_extensions(extensions))
        .await
        .expect_err("extension cannot establish trust");
    assert_eq!(error.kind(), &ModelErrorKind::InvalidRequest);
    assert_eq!(server.accepted_connection_count(), 0);
    assert_eq!(server.hit_count(), 0);
}

#[test]
fn contradictory_streaming_configuration_is_rejected() {
    fn model_config(capabilities: ModelCapabilities) -> GenaiModelConfig {
        GenaiModelConfig::new(
            "model",
            ProviderId::new("provider").expect("provider"),
            ModelId::new("model").expect("model"),
            capabilities,
        )
        .expect("model config")
    }

    let unbound_client = GenaiChatModelAdapter::new(
        genai::Client::default(),
        GenaiAdapterConfig::new(model_config(capabilities()))
            .with_streaming_policy(GenaiStreamingPolicy::TextOnly),
    )
    .expect_err("enabled policy needs a bound client");
    assert!(matches!(
        unbound_client,
        GenaiAdapterConfigError::StreamingClientUnbound
    ));

    for policy in [
        GenaiStreamingPolicy::TextOnly,
        GenaiStreamingPolicy::AuditedTextOnly,
    ] {
        let responses_client = GenaiChatModelAdapter::new(
            genai::Client::builder()
                .with_adapter_kind(AdapterKind::OpenAIResp)
                .build(),
            GenaiAdapterConfig::new(model_config(capabilities())).with_streaming_policy(policy),
        )
        .expect_err("Responses streaming is disabled at construction");
        assert!(matches!(
            responses_client,
            GenaiAdapterConfigError::StreamingAdapterUnsupported {
                adapter: "openai_resp"
            }
        ));
    }

    let unaudited_client = GenaiChatModelAdapter::new(
        genai::Client::builder()
            .with_adapter_kind(AdapterKind::Anthropic)
            .build(),
        GenaiAdapterConfig::new(model_config(capabilities()))
            .with_streaming_policy(GenaiStreamingPolicy::TextOnly),
    )
    .expect_err("unaudited adapters fail closed");
    assert!(matches!(
        unaudited_client,
        GenaiAdapterConfigError::StreamingAdapterUnsupported {
            adapter: "anthropic"
        }
    ));

    let missing_capability = GenaiChatModelAdapter::new(
        genai::Client::builder()
            .with_adapter_kind(AdapterKind::OpenAI)
            .build(),
        GenaiAdapterConfig::new(model_config(ModelCapabilities::new()))
            .with_streaming_policy(GenaiStreamingPolicy::TextOnly),
    )
    .expect_err("metadata must declare streaming");
    assert!(matches!(
        missing_capability,
        GenaiAdapterConfigError::StreamingCapabilityMissing
    ));

    let target = || ServiceTarget {
        endpoint: Endpoint::from_static("http://127.0.0.1:1/v1/"),
        auth: AuthData::from_single("redacted"),
        model: ModelIden::new(AdapterKind::OpenAIResp, "model"),
    };
    let resolver_config = genai::ClientConfig::default()
        .with_adapter_kind(AdapterKind::OpenAIResp)
        .with_service_target_resolver(ServiceTargetResolver::from_resolver_fn(
            |target: ServiceTarget| Ok(target),
        ));
    let error = GenaiChatModelAdapter::new_with_stable_target(
        resolver_config,
        target(),
        GenaiAdapterConfig::new(model_config(capabilities())),
    )
    .expect_err("stable binding excludes dynamic target resolvers");
    assert!(matches!(
        error,
        GenaiAdapterConfigError::StableTargetResolverUnsupported
    ));

    let error = GenaiChatModelAdapter::new_with_stable_target(
        genai::ClientConfig::default(),
        target(),
        GenaiAdapterConfig::new(model_config(capabilities())),
    )
    .expect_err("stable binding requires an explicit adapter");
    assert!(matches!(
        error,
        GenaiAdapterConfigError::StableTargetClientUnbound
    ));

    let error = GenaiChatModelAdapter::new_with_stable_target(
        genai::ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
        target(),
        GenaiAdapterConfig::new(model_config(capabilities())),
    )
    .expect_err("stable target and client adapter must match");
    assert!(matches!(
        error,
        GenaiAdapterConfigError::StableTargetAdapterMismatch
    ));
}
