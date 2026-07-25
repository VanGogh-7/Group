mod support;

use futures_util::StreamExt;
use group_agent_genai::extensions::{RAW_STOP_REASON, RESOLVED_MODEL};
use group_agent_model::{
    ChatRequest, ChatStreamEvent, FinishReason, Message, ModelCapabilities, ModelErrorKind,
    collect_chat_stream,
};
use support::{
    HangingSseServer, MockResponse, MockServer, model, openai_client, openai_responses_client,
    responses_model,
};

fn capabilities() -> ModelCapabilities {
    ModelCapabilities::new()
        .with_streaming(true)
        .with_tool_calling(true)
        .with_usage_reporting(true)
}

#[tokio::test]
async fn text_usage_and_end_are_normalized_online() {
    let server = MockServer::start(MockResponse::sse(concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":8,\"total_tokens\":12}}\n\n",
        "data: [DONE]\n\n"
    )))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let mut stream = model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("stream");

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("stream item"));
    }
    let response_started = events
        .iter()
        .position(|event| matches!(event, ChatStreamEvent::ResponseStarted { .. }))
        .expect("response started");
    let usage = events
        .iter()
        .position(|event| matches!(event, ChatStreamEvent::Usage(_)))
        .expect("usage");
    let finished = events
        .iter()
        .position(|event| matches!(event, ChatStreamEvent::Finished(_)))
        .expect("finished");
    assert!(
        response_started > 0,
        "genai exposes stream identity only at End"
    );
    assert!(usage < finished);
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                ChatStreamEvent::TextDelta(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        ["Hel", "lo"]
    );
    match &events[response_started] {
        ChatStreamEvent::ResponseStarted {
            response_id,
            model,
            extensions,
        } => {
            assert!(response_id.is_none());
            assert_eq!(model.as_ref().expect("model").as_str(), "gpt-4o-mini");
            assert_eq!(
                extensions.get(RESOLVED_MODEL),
                Some(&serde_json::json!("gpt-4o-mini"))
            );
            assert_eq!(
                extensions.get(RAW_STOP_REASON),
                Some(&serde_json::json!("stop"))
            );
        }
        _ => unreachable!("position was response-started"),
    }

    let response = collect_chat_stream(futures_util::stream::iter(events.into_iter().map(Ok)))
        .await
        .expect("collector");
    assert_eq!(response.message().text_content(), "Hello");
    assert_eq!(response.finish_reason(), &FinishReason::Stop);
    assert!(response.message().tool_calls().is_empty());
    let usage = response.usage().expect("partial usage");
    assert_eq!(usage.input_tokens(), Some(8));
    assert_eq!(usage.output_tokens(), None);
    assert_eq!(usage.total_tokens(), Some(12));
}

#[tokio::test]
async fn eof_without_end_is_an_adapter_protocol_error() {
    let server = MockServer::start(MockResponse::sse(
        "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
    ))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let mut stream = model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("stream");
    assert!(matches!(
        stream.next().await,
        Some(Ok(ChatStreamEvent::TextDelta(_)))
    ));
    let error = stream
        .next()
        .await
        .expect("explicit error")
        .expect_err("missing End");
    assert_eq!(error.kind(), &ModelErrorKind::Protocol);
    assert!(std::error::Error::source(&error).is_some());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn malformed_or_provider_error_item_terminates_stream_with_source() {
    for (body, expected) in [
        ("data: {not-json\n\n", ModelErrorKind::Decode),
        (
            "data: {\"error\":{\"message\":\"provider-stream-secret\",\"type\":\"server_error\"}}\n\n",
            ModelErrorKind::Protocol,
        ),
    ] {
        let server = MockServer::start(MockResponse::sse(body))
            .await
            .expect("server");
        let model = model(openai_client(server.base_url()), capabilities()).expect("model");
        let mut stream = model
            .stream(ChatRequest::new(vec![Message::user("hello")]))
            .await
            .expect("stream");
        let error = stream
            .next()
            .await
            .expect("error item")
            .expect_err("must fail");
        assert_eq!(error.kind(), &expected);
        assert!(std::error::Error::source(&error).is_some());
        assert!(!format!("{error:?}").contains("provider-stream-secret"));
        assert!(!error.to_string().contains("provider-stream-secret"));
        assert!(stream.next().await.is_none(), "error is terminal");
    }
}

#[tokio::test]
async fn unexpected_tool_event_is_terminal_and_emits_no_damaged_delta() {
    let server = MockServer::start(MockResponse::sse(concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-a\",",
        "\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"must-not-be-polled\"},\"finish_reason\":null}]}\n\n",
        "data: [DONE]\n\n"
    )))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let mut stream = model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("text-only request");
    let error = stream
        .next()
        .await
        .expect("terminal error")
        .expect_err("unexpected tool data must fail");
    assert_eq!(error.kind(), &ModelErrorKind::Protocol);
    assert!(
        stream.next().await.is_none(),
        "error must stop further polling"
    );
}

#[tokio::test]
async fn stream_initialization_http_error_is_classified() {
    let server = MockServer::start(MockResponse::status(
        500,
        r#"{"error":"stream-init-secret"}"#,
    ))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let mut stream = model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("genai defers HTTP initialization into its stream");
    let error = stream
        .next()
        .await
        .expect("stream error")
        .expect_err("HTTP status must fail");
    assert_eq!(error.kind(), &ModelErrorKind::ProviderUnavailable);
    assert_eq!(error.http_status(), Some(500));
    assert!(!format!("{error:?}").contains("stream-init-secret"));
}

#[tokio::test]
async fn responses_stream_is_rejected_before_raw_events_are_requested() {
    let raw_sentinel = "RAW_RESPONSES_EVENT_SENTINEL_17_1";
    let server = MockServer::start(MockResponse::sse(format!(
        "data: {{malformed:{raw_sentinel}\n\ndata: {{\"type\":\"response.created\""
    )))
    .await
    .expect("server");
    let model =
        responses_model(openai_responses_client(server.base_url()), capabilities()).expect("model");
    let error = match model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
    {
        Ok(_) => panic!("Responses streaming must fail closed"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(group_agent_model::ModelCapability::Streaming)
    );
    assert_eq!(server.hit_count(), 0);
    for rendered in [format!("{error:?}"), error.to_string()] {
        assert!(!rendered.contains(raw_sentinel));
    }
}

#[tokio::test]
async fn dropping_group_stream_drops_the_underlying_http_stream() {
    let mut server = HangingSseServer::start(
        "data: {\"choices\":[{\"delta\":{\"content\":\"first\"},\"finish_reason\":null}]}\n\n",
    )
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let mut stream = model
        .stream(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect("stream");
    assert!(matches!(
        stream.next().await,
        Some(Ok(ChatStreamEvent::TextDelta(text))) if text == "first"
    ));
    drop(stream);

    tokio::time::timeout(std::time::Duration::from_secs(2), server.wait_closed())
        .await
        .expect("dropping wrapper should release the genai response stream");
}
