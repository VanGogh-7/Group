mod support;

use std::error::Error as _;
use std::time::Duration;

use group_agent_model::{ChatRequest, Message, ModelCapabilities, ModelErrorKind};
use support::{MockResponse, MockServer, model, openai_client};

fn capabilities() -> ModelCapabilities {
    ModelCapabilities::new()
        .with_streaming(true)
        .with_tool_calling(true)
        .with_usage_reporting(true)
}

#[tokio::test]
async fn non_streaming_response_crosses_real_genai_client() {
    let mut server = MockServer::start(MockResponse::json(
        r#"{
          "id":"ignored-by-openai-chat-completions",
          "model":"provider-model-2026",
          "choices":[{
            "message":{"role":"assistant","content":"answer"},
            "finish_reason":"stop"
          }],
          "usage":{"prompt_tokens":7,"total_tokens":9}
        }"#,
    ))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");

    let response = model
        .complete(ChatRequest::new(vec![Message::user("request-sentinel")]))
        .await
        .expect("completion");
    assert_eq!(response.message().text_content(), "answer");
    assert_eq!(
        response.model().expect("provider model").as_str(),
        "provider-model-2026"
    );
    let usage = response.usage().expect("partial usage");
    assert_eq!(usage.input_tokens(), Some(7));
    assert_eq!(usage.output_tokens(), None);
    assert_eq!(usage.total_tokens(), Some(9));

    let request = server.request_json().await;
    assert_eq!(request["model"], "gpt-4o-mini");
    assert_eq!(request["messages"][0]["content"], "request-sentinel");
}

#[tokio::test]
async fn malformed_json_is_decode_with_genai_source() {
    let server = MockServer::start(MockResponse::json("{not-json"))
        .await
        .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let error = model
        .complete(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect_err("malformed provider JSON must fail");

    assert_eq!(error.kind(), &ModelErrorKind::Decode);
    assert!(error.source().is_some());
    assert!(!format!("{error:?}").contains("{not-json"));
    assert!(!error.to_string().contains("{not-json"));
}

#[tokio::test]
async fn http_statuses_are_classified_and_retry_after_is_preserved() {
    let cases = [
        (401, ModelErrorKind::Authentication, false),
        (403, ModelErrorKind::PermissionDenied, false),
        (408, ModelErrorKind::Timeout, true),
        (429, ModelErrorKind::RateLimited, true),
        (500, ModelErrorKind::ProviderUnavailable, true),
    ];

    for (status, expected_kind, retryable) in cases {
        let response = MockResponse::status(status, r#"{"error":"provider-secret"}"#)
            .with_header("Retry-After", "3");
        let server = MockServer::start(response).await.expect("server");
        let model = model(openai_client(server.base_url()), capabilities()).expect("model");
        let error = model
            .complete(ChatRequest::new(vec![Message::user("hello")]))
            .await
            .expect_err("status must fail");

        assert_eq!(error.kind(), &expected_kind);
        assert_eq!(error.http_status(), Some(status));
        assert_eq!(error.is_retryable(), retryable);
        if retryable {
            assert_eq!(error.retry_after(), Some(Duration::from_secs(3)));
        }
        assert!(error.source().is_some());
        assert!(!format!("{error:?}").contains("provider-secret"));
        assert!(!error.to_string().contains("provider-secret"));
    }
}
