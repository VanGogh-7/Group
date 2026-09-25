#![cfg(feature = "structured-output")]
mod support;
use genai::{
    ClientConfig, ModelIden, ServiceTarget,
    adapter::AdapterKind,
    resolver::{AuthData, Endpoint},
};
use group_agent_genai::{
    GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig, GenaiStreamingPolicy,
};
use group_agent_model::{
    ChatModel, ChatRequest, Message, ModelCapabilities, ModelErrorKind, ModelId, ProviderId,
    StructuredOutput, collect_chat_stream,
};
use serde_json::{Value, json};
use support::{MockResponse, MockServer};
fn output() -> StructuredOutput {
    StructuredOutput::new(
        "answer",
        json!({"type":"object","properties":{"answer":{"type":"string"}},
        "required":["answer"],"additionalProperties":false}),
    )
    .unwrap()
}
fn request() -> ChatRequest {
    ChatRequest::new(vec![Message::user("question")]).with_structured_output(output())
}
fn config() -> GenaiAdapterConfig {
    GenaiAdapterConfig::new(
        GenaiModelConfig::new(
            "gpt-4o-mini",
            ProviderId::new("openai").unwrap(),
            ModelId::new("test").unwrap(),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true)
                .with_structured_output(true),
        )
        .unwrap(),
    )
    .with_streaming_policy(GenaiStreamingPolicy::OpenAiChat)
}
fn model(url: &str) -> ChatModel {
    configured_model(url, config())
}
fn configured_model(url: &str, cfg: GenaiAdapterConfig) -> ChatModel {
    ChatModel::from_adapter(
        GenaiChatModelAdapter::new_with_stable_target(
            ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
            ServiceTarget {
                endpoint: Endpoint::from_owned(url.to_owned()),
                auth: AuthData::from_single("test-only"),
                model: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            },
            cfg,
        )
        .unwrap(),
    )
    .unwrap()
}
fn response(streaming: bool, content: &str, refusal: Value, finish: &str) -> MockResponse {
    let message = json!({"role":"assistant","content":content,"refusal":refusal});
    let value = json!({"object":if streaming{"chat.completion.chunk"}else{"chat.completion"},
        "id":"id","model":"gpt-4o-mini","choices":[{"index":0,"finish_reason":finish,
            if streaming {"delta"}else{"message"}:message}],
        "usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}});
    if streaming {
        MockResponse::sse(format!("data: {value}\n\ndata: [DONE]\n\n"))
    } else {
        MockResponse::json(value.to_string())
    }
}
#[tokio::test]
async fn complete_and_stream_send_exact_contract_and_return_valid_json() {
    for streaming in [false, true] {
        let mut server = MockServer::start(response(
            streaming,
            r#"{"answer":"ok"}"#,
            Value::Null,
            "stop",
        ))
        .await
        .unwrap();
        let model = model(server.base_url());
        let result = if streaming {
            collect_chat_stream(model.stream(request()).await.unwrap()).await
        } else {
            model.complete(request()).await
        };
        let result = result.unwrap();
        assert_eq!(
            output()
                .validate_response(&result)
                .unwrap()
                .unwrap()
                .value(),
            &json!({"answer":"ok"})
        );
        let wire = server.request_json().await;
        assert_eq!(
            wire["response_format"],
            json!({"type":"json_schema","json_schema":{"name":"answer","strict":true,"schema":output().schema()}})
        );
        assert_eq!(wire["stream"], streaming);
        assert_eq!(wire.get("stream_options").is_some(), streaming);
        assert_eq!(server.hit_count(), 1);
    }
}
#[tokio::test]
async fn refusal_truncation_and_schema_errors_fail_without_retry() {
    for streaming in [false, true] {
        for (content, refusal, finish) in [
            (r#"{"answer":"ok"}"#, json!("SECRET_REFUSAL"), "stop"),
            (r#"{"answer":"ok"}"#, json!(""), "stop"),
            (r#"{"answer":"ok"}"#, Value::Null, "length"),
            ("{}", Value::Null, "stop"),
            (r#"{"answer":"a","answer":"b"}"#, Value::Null, "stop"),
        ] {
            let server = MockServer::start(response(streaming, content, refusal, finish))
                .await
                .unwrap();
            let model = model(server.base_url());
            let result = if streaming {
                collect_chat_stream(model.stream(request()).await.unwrap()).await
            } else {
                model.complete(request()).await
            };
            let error = result.unwrap_err();
            assert_eq!(error.kind(), &ModelErrorKind::OutputValidation);
            assert!(!format!("{error:?} {error}").contains("SECRET_REFUSAL"));
            assert_eq!(server.hit_count(), 1);
        }
    }
}
#[tokio::test]
async fn bounded_completion_body_and_malformed_refusal_are_rejected() {
    for response in [
        MockResponse::json(" ".repeat(4 * 1024 * 1024 + 1)),
        response(false, r#"{"answer":"ok"}"#, json!(123), "stop"),
    ] {
        let server = MockServer::start(response).await.unwrap();
        let error = model(server.base_url())
            .complete(request())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), &ModelErrorKind::Protocol);
        assert_eq!(server.hit_count(), 1);
    }
}
#[test]
fn capability_declaration_requires_a_stable_native_target() {
    let cfg = config().with_streaming_policy(GenaiStreamingPolicy::Disabled);
    assert!(GenaiChatModelAdapter::new(genai::Client::default(), cfg).is_err());
}

#[tokio::test]
async fn transport_failures_preserve_context_and_concrete_source() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1/", listener.local_addr().unwrap());
    drop(listener);
    for streaming in [false, true] {
        let model = model(&url);
        let error = if streaming {
            collect_chat_stream(model.stream(request()).await.unwrap())
                .await
                .unwrap_err()
        } else {
            model.complete(request()).await.unwrap_err()
        };
        assert_eq!(error.kind(), &ModelErrorKind::ProviderUnavailable);
        assert_eq!(error.provider().unwrap().as_str(), "openai");
        assert_eq!(error.model().unwrap().as_str(), "test");
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<reqwest::Error>()
        );
    }
}
#[tokio::test]
async fn complete_collector_failure_preserves_protocol_source() {
    let calls: Vec<_> = (0..1026).map(|i| json!({"id":format!("c{i}"),"type":"function","function":{"name":"tool","arguments":"{}"}})).collect();
    let server = MockServer::start(MockResponse::json(json!({"object":"chat.completion","id":"id","model":"test","choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","tool_calls":calls}}]}).to_string())).await.unwrap();
    let model = configured_model(
        server.base_url(),
        config().with_streaming_limits(
            group_agent_genai::GenaiStreamingLimits::default().with_max_tool_calls(2048),
        ),
    );
    let error = model.complete(request()).await.unwrap_err();
    assert_eq!(error.kind(), &ModelErrorKind::Protocol);
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .is::<group_agent_model::StreamProtocolError>()
    );
    assert_eq!(server.hit_count(), 1);
}
#[tokio::test]
async fn dropping_complete_or_stream_setup_releases_http_request() {
    use std::time::Duration;
    use support::HangingRequestServer;
    for streaming in [false, true] {
        let mut server = HangingRequestServer::start().await.unwrap();
        let model = model(server.base_url());
        let mut pending = Box::pin(async {
            if streaming {
                let _ = collect_chat_stream(model.stream(request()).await.unwrap()).await;
            } else {
                let _ = model.complete(request()).await;
            }
        });
        tokio::select! {
            () = server.wait_received() => {},
            () = &mut pending => panic!("request unexpectedly finished"),
            () = tokio::time::sleep(Duration::from_secs(5)) => panic!("request not received"),
        }
        drop(pending);
        tokio::time::timeout(Duration::from_secs(5), server.wait_closed())
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn structured_stream_waiting_for_done_releases_connection_on_drop() {
    use futures_util::StreamExt;
    let mut server = support::HangingSseServer::start("data: {\"object\":\"chat.completion.chunk\",\"id\":\"id\",\"model\":\"test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"{\\\"answer\\\":\\\"ok\\\"}\"},\"finish_reason\":\"stop\"}]}\n\n").await.unwrap();
    let mut stream = model(server.base_url()).stream(request()).await.unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Ok(group_agent_model::ChatStreamEvent::TextDelta(_)))
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), stream.next())
            .await
            .is_err()
    );
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(5), server.wait_closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn native_complete_and_stream_preserve_successful_multiple_tool_calls() {
    use group_agent_model::FinishReason;
    for streaming in [false, true] {
        let tools: Vec<_> = (0..2).map(|i| {
            let mut call = json!({"id":format!("call_{i}"),"type":"function","function":{"name":"lookup","arguments":format!("{{\"index\":{i}}}")}});
            if streaming { call["index"] = json!(i); }
            call
        }).collect();
        let message =
            json!({"role":"assistant","content":"Looking up both items.","tool_calls":tools});
        let body = json!({"object":if streaming {"chat.completion.chunk"} else {"chat.completion"},"id":"id","model":"gpt-4o-mini","choices":[{"index":0,"finish_reason":"tool_calls",if streaming {"delta"} else {"message"}:message}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}});
        let wire = if streaming {
            MockResponse::sse(format!("data: {body}\n\ndata: [DONE]\n\n"))
        } else {
            MockResponse::json(body.to_string())
        };
        let server = MockServer::start(wire).await.unwrap();
        let model = model(server.base_url());
        let result = if streaming {
            collect_chat_stream(model.stream(request()).await.unwrap())
                .await
                .unwrap()
        } else {
            model.complete(request()).await.unwrap()
        };
        assert_eq!(result.finish_reason(), &FinishReason::ToolCalls);
        assert_eq!(result.message().text_content(), "Looking up both items.");
        assert!(output().validate_response(&result).unwrap().is_none());
        assert_eq!(result.message().tool_calls().len(), 2);
        for (i, call) in result.message().tool_calls().iter().enumerate() {
            assert_eq!(call.id().as_str(), format!("call_{i}"));
            assert_eq!(call.name().as_str(), "lookup");
            assert_eq!(call.arguments(), &json!({"index":i}));
        }
        assert_eq!(result.usage().unwrap().total_tokens(), Some(7));
        assert_eq!(server.hit_count(), 1);
    }
}
