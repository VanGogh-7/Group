use super::*;
use group_agent_genai::{GenaiAdapterConfigError, GenaiStreamingLimits};
use group_agent_model::ModelErrorKind;

fn final_text() -> String {
    chunk(json!({"content":"done"}), json!("stop")) + "data: [DONE]\n\n"
}

async fn stream_error(body: String, config: GenaiAdapterConfig, expected: ModelErrorKind) {
    let server = MockServer::start(MockResponse::sse(body)).await.unwrap();
    let model = ChatModel::from_adapter(
        GenaiChatModelAdapter::new_with_stable_target(
            ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
            target(server.base_url()),
            config,
        )
        .unwrap(),
    )
    .unwrap();
    let mut stream = model.stream(request()).await.unwrap();
    let error = loop {
        match stream.next().await.expect("an explicit error is required") {
            Ok(ChatStreamEvent::Finished(_)) => panic!("invalid response must not finish"),
            Ok(_) => {}
            Err(error) => break error,
        }
    };
    assert_eq!(error.kind(), &expected);
    assert!(std::error::Error::source(&error).is_some());
    assert!(!format!("{error:?} {error}").contains("SECRET_"));
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(server.hit_count(), 1);
}

#[tokio::test]
async fn malformed_incomplete_and_conflicting_streams_fail_permanently() {
    let call = || json!({"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"lookup","arguments":"{}"}}]});
    let start = chunk(call(), Value::Null);
    let cases = vec![
        ("data: {SECRET_PAYLOAD\n\n".into(), ModelErrorKind::Decode),
        (
            "data: {\"error\":{\"message\":\"SECRET_PROVIDER\"}}\n\n".into(),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({"content":"partial"}), Value::Null),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({"content":"partial"}), Value::Null) + "data: [DONE]\n\n",
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({}), json!("stop")) + "data: [DONE]\n",
            ModelErrorKind::Protocol,
        ),
        (
            start.clone() + &chunk(json!({}), json!("length")) + "data: [DONE]\n\n",
            ModelErrorKind::Protocol,
        ),
        (
            start.clone() + &chunk(json!({}), json!("stop")) + "data: [DONE]\n\n",
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({}), json!("tool_calls")) + "data: [DONE]\n\n",
            ModelErrorKind::Protocol,
        ),
        (
            start.clone()
                + &chunk(
                    json!({"tool_calls":[{"index":0,"id":"changed","function":{"arguments":" "}}]}),
                    Value::Null,
                ),
            ModelErrorKind::Protocol,
        ),
        (
            start.clone()
                + &chunk(
                    json!({"tool_calls":[{"index":0,"function":{"name":"changed","arguments":" "}}]}),
                    Value::Null,
                ),
            ModelErrorKind::Protocol,
        ),
        (
            start
                + &chunk(
                    json!({"tool_calls":[{"index":1,"id":"a","function":{"name":"lookup","arguments":"{}"}}]}),
                    Value::Null,
                ),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(
                json!({"tool_calls":[{"index":0,"function":{"name":"lookup","arguments":"{}"}}]}),
                Value::Null,
            ),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(
                json!({"tool_calls":[{"index":u32::MAX,"id":"a","function":{"name":"lookup","arguments":"{}"}}]}),
                Value::Null,
            ),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(
                json!({"tool_calls":[{"index":0,"id":"a","type":"custom","function":{"name":"lookup","arguments":"{}"}}]}),
                Value::Null,
            ),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(
                json!({"tool_calls":[{"index":0,"id":"a","function":{"name":"lookup","arguments":"{SECRET_INCOMPLETE"}}]}),
                json!("tool_calls"),
            ) + "data: [DONE]\n\n",
            ModelErrorKind::Decode,
        ),
        (
            chunk(json!({"content":17}), Value::Null),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({"refusal":"SECRET_REFUSAL"}), json!("stop")),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({"reasoning_content":"SECRET_REASONING"}), Value::Null),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({}), json!("unknown_reason")),
            ModelErrorKind::Protocol,
        ),
        (
            chunk(json!({}), json!("stop")) + &chunk(json!({"content":"late"}), Value::Null),
            ModelErrorKind::Protocol,
        ),
        (
            "event: error\ndata: {}\n\n".into(),
            ModelErrorKind::Protocol,
        ),
        (
            "data: {\"choices\":[{},{}]}\n\n".into(),
            ModelErrorKind::Protocol,
        ),
    ];
    for (body, kind) in cases {
        stream_error(body, config(), kind).await;
    }
}

#[tokio::test]
async fn normalized_frame_and_aggregate_argument_limits_are_enforced() {
    let limited =
        config().with_streaming_limits(GenaiStreamingLimits::new().with_max_sse_event_bytes(32));
    stream_error(
        ":".to_owned() + &"x".repeat(64) + "\n\n" + &final_text(),
        limited,
        ModelErrorKind::Protocol,
    )
    .await;
    let limited =
        config().with_streaming_limits(GenaiStreamingLimits::new().with_max_tool_argument_bytes(3));
    stream_error(
        chunk(
            json!({"tool_calls":[
                {"index":0,"id":"a","function":{"name":"lookup","arguments":"{}"}},
                {"index":1,"id":"b","function":{"name":"lookup","arguments":"{}"}}
            ]}),
            json!("tool_calls"),
        ) + "data: [DONE]\n\n",
        limited,
        ModelErrorKind::Protocol,
    )
    .await;
}

#[tokio::test]
async fn client_defaults_and_namespaced_model_preserve_the_nonstream_wire_contract() {
    for streaming in [false, true] {
        let response = if streaming {
            MockResponse::sse(final_text())
        } else {
            MockResponse::json(json!({"id":"id","model":"gpt-4o-mini","choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}).to_string())
        };
        let mut server = MockServer::start(response).await.unwrap();
        let mut target = target(server.base_url());
        target.model = ModelIden::new(AdapterKind::OpenAI, "openai::gpt-4o-mini");
        let options = genai::chat::ChatOptions::default()
            .with_temperature(0.4)
            .with_max_tokens(42)
            .with_stop_sequences(vec!["do-not-inherit".into()]);
        let model = ChatModel::from_adapter(
            GenaiChatModelAdapter::new_with_stable_target(
                ClientConfig::default()
                    .with_adapter_kind(AdapterKind::OpenAI)
                    .with_chat_options(options),
                target,
                config(),
            )
            .unwrap(),
        )
        .unwrap();
        let request = ChatRequest::new(vec![Message::user("hello")]);
        if streaming {
            collect_chat_stream(model.stream(request).await.unwrap())
                .await
                .unwrap();
        } else {
            model.complete(request).await.unwrap();
        }
        let wire = server.request_json().await;
        assert_eq!(wire["model"], "gpt-4o-mini");
        assert_eq!(wire["temperature"], 0.4);
        assert_eq!(wire["max_tokens"], 42);
        assert!(wire.get("max_completion_tokens").is_none());
        assert!(wire.get("stop").is_none());
    }
}

#[tokio::test]
async fn strict_wire_preserves_tool_choices_and_explicit_generation_controls() {
    use group_agent_model::{GenerationConfig, ToolChoice};
    for (choice, expected) in [
        (ToolChoice::Auto, json!("auto")),
        (ToolChoice::None, json!("none")),
        (ToolChoice::Required, json!("required")),
        (
            ToolChoice::Named(ToolName::new("lookup").unwrap()),
            json!({"type":"function","function":{"name":"lookup"}}),
        ),
    ] {
        let mut server = MockServer::start(MockResponse::sse(final_text()))
            .await
            .unwrap();
        let request = request().with_tool_choice(choice).with_generation(
            GenerationConfig::new()
                .with_temperature(0.2)
                .with_top_p(0.7)
                .with_max_output_tokens(23)
                .with_stop_sequences(["END"]),
        );
        collect_chat_stream(model(server.base_url()).stream(request).await.unwrap())
            .await
            .unwrap();
        let wire = server.request_json().await;
        assert_eq!(wire["tool_choice"], expected);
        assert_eq!(wire["temperature"], 0.2);
        assert_eq!(wire["top_p"], 0.7);
        assert_eq!(wire["max_tokens"], 23);
        assert_eq!(wire["stop"], json!(["END"]));
        assert_eq!(
            wire["tools"][0]["function"]["parameters"],
            super::request().tools()[0].input_schema().clone()
        );
        assert_eq!(wire["stream_options"], json!({"include_usage":true}));
    }
}

#[test]
fn strict_mode_requires_a_stable_transport_and_explicit_supported_configuration() {
    let client = genai::Client::builder()
        .with_adapter_kind(AdapterKind::OpenAI)
        .build();
    assert!(matches!(
        GenaiChatModelAdapter::new(client, config()),
        Err(GenaiAdapterConfigError::OpenAiChatRequiresStableTarget)
    ));
    for options in [
        genai::chat::ChatOptions {
            extra_body: Some(json!({"stream":false})),
            ..Default::default()
        },
        genai::chat::ChatOptions {
            extra_headers: Some(genai::Headers::default()),
            ..Default::default()
        },
    ] {
        let error = GenaiChatModelAdapter::new_with_stable_target(
            ClientConfig::default()
                .with_adapter_kind(AdapterKind::OpenAI)
                .with_chat_options(options),
            target("http://127.0.0.1:1/v1/"),
            config(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            GenaiAdapterConfigError::UnsupportedOpenAiChatSetting { .. }
        ));
    }
    let mut target = target("http://127.0.0.1:1/v1/");
    target.auth = AuthData::from_env("SECRET_ENV_NAME");
    let error = GenaiChatModelAdapter::new_with_stable_target(
        ClientConfig::default().with_adapter_kind(AdapterKind::OpenAI),
        target,
        config(),
    )
    .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("SECRET_ENV_NAME"));
}
