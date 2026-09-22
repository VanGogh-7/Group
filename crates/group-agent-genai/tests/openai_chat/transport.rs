use super::*;
use group_agent_model::ModelErrorKind;
use std::error::Error;
use std::time::Duration;
use support::wire::{WireResponse, WireServer};
use support::{HangingRequestServer, HangingSseServer};

#[tokio::test]
async fn endpoint_resolution_matches_nonstream_queries_and_trailing_slashes() {
    for streaming in [false, true] {
        for trailing_slash in [false, true] {
            let body = if streaming {
                chunk(json!({"content":"done"}), json!("stop")) + "data: [DONE]\n\n"
            } else {
                json!({"id":"id","model":"gpt-4o-mini","choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}).to_string()
            };
            let response = if streaming {
                WireResponse::sse(body)
            } else {
                WireResponse::json(body)
            };
            let mut server = WireServer::start(vec![response]).await;
            let base = if trailing_slash {
                server.base_url()
            } else {
                server.base_url().trim_end_matches('/')
            };
            let model = model(&format!("{base}?api-version=fixture"));
            let request = ChatRequest::new(vec![Message::user("hello")]);
            if streaming {
                collect_chat_stream(model.stream(request).await.unwrap())
                    .await
                    .unwrap();
            } else {
                model.complete(request).await.unwrap();
            }
            let (headers, _) = server.request().await;
            let path = if trailing_slash {
                "/v1/chat/completions"
            } else {
                "/chat/completions"
            };
            assert!(headers.starts_with(&format!("POST {path}?api-version=fixture HTTP/1.1\r\n")));
        }
    }
}

#[tokio::test]
async fn unauthenticated_streaming_preserves_genai_nonstream_auth_rejection() {
    let body = chunk(json!({"content":"done"}), json!("stop")) + "data: [DONE]\n\n";
    let mut server = WireServer::start(vec![WireResponse::sse(body)]).await;
    let mut target = target(server.base_url());
    target.auth = AuthData::None;
    let model = ChatModel::from_adapter(
        GenaiChatModelAdapter::new_with_stable_target(
            ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
            target,
            config(),
        )
        .unwrap(),
    )
    .unwrap();
    let error = model
        .complete(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .unwrap_err();
    assert!(matches!(
        error.source().unwrap().downcast_ref::<genai::Error>(),
        Some(genai::Error::Resolver {
            resolver_error: genai::resolver::Error::ResolverAuthDataNotSingleValue,
            ..
        })
    ));
    assert_eq!(server.hits(), 0);
    collect_chat_stream(
        model
            .stream(ChatRequest::new(vec![Message::user("hello")]))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let (headers, _) = server.request().await;
    assert!(!headers.contains("authorization:"));
    assert_eq!(server.hits(), 1);
}

#[test]
fn invalid_endpoints_have_typed_payload_safe_configuration_errors() {
    use group_agent_genai::GenaiAdapterConfigError;
    let error = GenaiChatModelAdapter::new_with_stable_target(
        ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
        target("SECRET_INVALID_URL"),
        config(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        GenaiAdapterConfigError::OpenAiChatEndpoint(_)
    ));
    assert!(error.source().unwrap().source().is_some());
    assert!(!format!("{error:?} {error}").contains("SECRET_INVALID_URL"));
    let error = GenaiChatModelAdapter::new_with_stable_target(
        ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
        target("file:///SECRET_PATH/"),
        config(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        GenaiAdapterConfigError::UnsupportedOpenAiChatSetting {
            field: "endpoint scheme"
        }
    ));
    assert!(!format!("{error:?} {error}").contains("SECRET_PATH"));
}

#[tokio::test]
async fn utf8_bom_line_endings_and_multiline_data_survive_every_http_byte_boundary() {
    for newline in ["\n", "\r\n", "\r"] {
        let frame = json!({"id":"id","model":"model","choices":[{"index":0,"delta":{"content":"你好 café"},"finish_reason":"stop"}]}).to_string();
        let multiline = frame.replacen(",\"model\"", &format!(",{newline}data: \"model\""), 1);
        let body = format!(
            "\u{feff}: comment{newline}{newline}event: message{newline}data: {multiline}{newline}{newline}data: [DONE]{newline}{newline}"
        );
        let server = WireServer::start(vec![WireResponse::bytewise(body.as_bytes())]).await;
        let response = collect_chat_stream(
            model(server.base_url())
                .stream(ChatRequest::new(vec![Message::user("hello")]))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.message().text_content(), "你好 café");
    }
}

#[tokio::test]
async fn invalid_utf8_keeps_a_concrete_source_and_never_finishes() {
    let server = WireServer::start(vec![WireResponse::bytewise(b"data: \xff\n\n")]).await;
    let mut stream = model(server.base_url()).stream(request()).await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), &ModelErrorKind::Decode);
    assert!(
        error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .is::<std::str::Utf8Error>()
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn http_failures_redirects_and_wrong_content_type_are_terminal_without_retry() {
    for (response, expected) in [
        (
            MockResponse::status(401, "SECRET_BODY"),
            ModelErrorKind::Authentication,
        ),
        (
            MockResponse::status(429, "SECRET_BODY").with_header("retry-after", "2"),
            ModelErrorKind::RateLimited,
        ),
        (
            MockResponse::status(503, "SECRET_BODY"),
            ModelErrorKind::ProviderUnavailable,
        ),
        (
            MockResponse::status(307, "SECRET_BODY")
                .with_header("location", "http://127.0.0.1:1/SECRET_URL"),
            ModelErrorKind::Other,
        ),
        (MockResponse::json("{}"), ModelErrorKind::Protocol),
    ] {
        let server = MockServer::start(response).await.unwrap();
        let mut stream = model(server.base_url()).stream(request()).await.unwrap();
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), &expected);
        if expected == ModelErrorKind::RateLimited {
            assert_eq!(error.retry_after(), Some(Duration::from_secs(2)));
        }
        assert!(!format!("{error:?} {error}").contains("SECRET_"));
        assert!(error.source().is_some());
        assert!(stream.next().await.is_none());
        assert_eq!(server.hit_count(), 1);
    }
}

#[tokio::test]
async fn web_config_headers_and_explicit_request_override_are_used() {
    for override_auth in [false, true] {
        let body = chunk(json!({"content":"done"}), json!("stop")) + "data: [DONE]\n\n";
        let mut server = WireServer::start(vec![WireResponse::sse(body)]).await;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-app", "configured".parse().unwrap());
        let client_config = ClientConfig::default()
            .with_adapter_kind(AdapterKind::OpenAI)
            .with_web_config(genai::WebConfig::default().with_default_headers(headers));
        let mut target = target(server.base_url());
        if override_auth {
            target.endpoint = Endpoint::from_static("http://127.0.0.1:1/unreachable/");
            target.auth = AuthData::RequestOverride {
                url: format!("{}custom", server.base_url()),
                headers: [("x-auth".to_owned(), "explicit".to_owned())].into(),
            };
        }
        let model = ChatModel::from_adapter(
            GenaiChatModelAdapter::new_with_stable_target(client_config, target, config()).unwrap(),
        )
        .unwrap();
        collect_chat_stream(
            model
                .stream(ChatRequest::new(vec![Message::user("hello")]))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let (headers, _) = server.request().await;
        assert!(headers.contains("x-app: configured"));
        assert!(headers.contains(if override_auth {
            "x-auth: explicit"
        } else {
            "authorization: Bearer local-test-only"
        }));
    }
}

#[tokio::test]
async fn dropping_an_unpolled_stream_dispatches_nothing_and_dropping_active_stream_closes_it() {
    let server = MockServer::start(MockResponse::sse("data: [DONE]\n\n"))
        .await
        .unwrap();
    let stream = model(server.base_url()).stream(request()).await.unwrap();
    drop(stream);
    assert_eq!(server.hit_count(), 0);
    let mut server = HangingSseServer::start("data: {\"id\":\"id\",\"model\":\"model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"},\"finish_reason\":null}]}\n\n").await.unwrap();
    let mut stream = model(server.base_url()).stream(request()).await.unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Ok(ChatStreamEvent::TextDelta(_)))
    ));
    drop(stream);
    tokio::time::timeout(Duration::from_secs(5), server.wait_closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn configured_http_timeout_releases_the_inflight_request() {
    let mut server = HangingRequestServer::start().await.unwrap();
    let client_config = ClientConfig::default()
        .with_adapter_kind(AdapterKind::OpenAI)
        .with_web_config(genai::WebConfig::default().with_timeout(Duration::from_millis(100)));
    let model = ChatModel::from_adapter(
        GenaiChatModelAdapter::new_with_stable_target(
            client_config,
            target(server.base_url()),
            config(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut stream = model.stream(request()).await.unwrap();
    let (error, ()) = tokio::join!(
        async { stream.next().await.unwrap().unwrap_err() },
        server.wait_received()
    );
    assert_eq!(error.kind(), &ModelErrorKind::Timeout);
    assert!(error.source().unwrap().is::<reqwest::Error>());
    assert!(stream.next().await.is_none());
    tokio::time::timeout(Duration::from_secs(5), server.wait_closed())
        .await
        .unwrap();
}
