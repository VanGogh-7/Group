mod support;

use group_agent_genai::extensions::{
    COMPLETION_TOKEN_DETAILS, PROMPT_TOKEN_DETAILS, PROVIDER_MODEL, REASONING_CONTENT,
    RESOLVED_MODEL,
};
use group_agent_model::{ChatRequest, FinishReason, Message, ModelCapabilities, ModelErrorKind};
use support::{MockResponse, MockServer, model, openai_client};

fn capabilities() -> ModelCapabilities {
    ModelCapabilities::new()
        .with_tool_calling(true)
        .with_usage_reporting(true)
}

#[tokio::test]
async fn text_tools_reasoning_identity_and_detailed_usage_map_once() {
    let server = MockServer::start(MockResponse::json(
        r#"{
          "model":"provider-model",
          "choices":[{
            "message":{
              "role":"assistant",
              "content":"visible",
              "reasoning_content":"reasoning-sentinel",
              "tool_calls":[
                {"id":"call-a","type":"function","function":{"name":"alpha","arguments":"{\"a\":1}"}},
                {"id":"call-b","type":"function","function":{"name":"beta","arguments":{"b":2}}}
              ]
            },
            "finish_reason":"tool_calls"
          }],
          "usage":{
            "prompt_tokens":10,
            "completion_tokens":4,
            "total_tokens":14,
            "prompt_tokens_details":{"cached_tokens":3},
            "completion_tokens_details":{"reasoning_tokens":2}
          }
        }"#,
    ))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let response = model
        .complete(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("response");

    assert_eq!(response.message().text_content(), "visible");
    assert!(
        !response
            .message()
            .text_content()
            .contains("reasoning-sentinel")
    );
    assert_eq!(response.finish_reason(), &FinishReason::ToolCalls);
    assert_eq!(response.message().tool_calls().len(), 2);
    assert_eq!(response.message().tool_calls()[0].id().as_str(), "call-a");
    assert_eq!(response.message().tool_calls()[0].name().as_str(), "alpha");
    assert_eq!(
        response.message().tool_calls()[0].arguments(),
        &serde_json::json!({"a":1})
    );
    assert_eq!(response.message().tool_calls()[1].id().as_str(), "call-b");
    assert_eq!(
        response
            .message()
            .extensions()
            .get(REASONING_CONTENT)
            .expect("reasoning extension"),
        &serde_json::json!(["reasoning-sentinel"])
    );
    assert_eq!(
        response.extensions().get(RESOLVED_MODEL),
        Some(&serde_json::json!("gpt-4o-mini"))
    );
    assert_eq!(
        response.extensions().get(PROVIDER_MODEL),
        Some(&serde_json::json!("provider-model"))
    );
    let usage = response.usage().expect("usage");
    assert_eq!(usage.input_tokens(), Some(10));
    assert_eq!(usage.output_tokens(), Some(4));
    assert_eq!(usage.total_tokens(), Some(14));
    assert_eq!(
        usage
            .extensions()
            .get(PROMPT_TOKEN_DETAILS)
            .expect("prompt details")["cached_tokens"],
        3
    );
    assert_eq!(
        usage
            .extensions()
            .get(COMPLETION_TOKEN_DETAILS)
            .expect("completion details")["reasoning_tokens"],
        2
    );
    let debug = format!("{response:?}");
    assert!(!debug.contains("reasoning-sentinel"));
    assert!(!debug.contains(r#"{"a":1}"#));
}

#[tokio::test]
async fn all_known_unknown_and_missing_stop_reasons_are_explicit() {
    let cases = [
        ("stop", FinishReason::Stop),
        ("length", FinishReason::Length),
        ("tool_calls", FinishReason::ToolCalls),
        ("content_filter", FinishReason::ContentFilter),
        (
            "future-provider-reason",
            FinishReason::Other("future-provider-reason".to_owned()),
        ),
    ];
    for (raw, expected) in cases {
        let body = format!(
            r#"{{
              "model":"provider-model",
              "choices":[{{"message":{{"role":"assistant","content":"x"}},"finish_reason":"{raw}"}}]
            }}"#
        );
        let server = MockServer::start(MockResponse::json(body))
            .await
            .expect("server");
        let model = model(openai_client(server.base_url()), capabilities()).expect("model");
        let response = model
            .complete(ChatRequest::new(vec![Message::user("hello")]))
            .await
            .expect("response");
        assert_eq!(response.finish_reason(), &expected);
    }

    let server = MockServer::start(MockResponse::json(
        r#"{"model":"provider-model","choices":[{"message":{"role":"assistant","content":"x"}}]}"#,
    ))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let response = model
        .complete(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("response");
    assert_eq!(
        response.finish_reason(),
        &FinishReason::Other("unspecified".to_owned())
    );
}

#[tokio::test]
async fn all_partial_usage_shapes_survive_and_negative_counts_fail() {
    let cases = [
        (r#""prompt_tokens":5"#, (Some(5), None, None)),
        (r#""completion_tokens":6"#, (None, Some(6), None)),
        (r#""total_tokens":7"#, (None, None, Some(7))),
    ];
    for (usage_fields, expected) in cases {
        let body = format!(
            r#"{{
              "model":"provider-model",
              "choices":[{{"message":{{"role":"assistant","content":"x"}},"finish_reason":"stop"}}],
              "usage":{{{usage_fields}}}
            }}"#
        );
        let server = MockServer::start(MockResponse::json(body))
            .await
            .expect("server");
        let model = model(openai_client(server.base_url()), capabilities()).expect("model");
        let response = model
            .complete(ChatRequest::new(vec![Message::user("hello")]))
            .await
            .expect("response");
        let usage = response.usage().expect("partial usage");
        assert_eq!(
            (
                usage.input_tokens(),
                usage.output_tokens(),
                usage.total_tokens()
            ),
            expected
        );
    }

    let server = MockServer::start(MockResponse::json(
        r#"{
          "model":"provider-model",
          "choices":[{"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],
          "usage":{"prompt_tokens":-1}
        }"#,
    ))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let error = model
        .complete(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect_err("negative usage must fail");
    assert_eq!(error.kind(), &ModelErrorKind::Decode);
    assert!(std::error::Error::source(&error).is_some());
}
