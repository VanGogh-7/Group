#![cfg(feature = "structured-output")]

use group_agent_model::{AssistantMessage, ChatResponse, FinishReason, StructuredOutput};
use serde_json::json;

fn schema() -> serde_json::Value {
    json!({"type":"object","properties":{"answer":{"type":"string"}},
        "required":["answer"],"additionalProperties":false})
}

#[test]
fn validates_exact_result_and_rejects_duplicate_keys() {
    let output = StructuredOutput::new("answer", schema()).unwrap();
    for text in [r#"{"answer":"ok"}"#, " {\"answer\":\"ok\"} \n"] {
        let response = ChatResponse::new(AssistantMessage::text(text), FinishReason::Stop);
        let value = output.validate_response(&response).unwrap().unwrap();
        assert_eq!(value.value(), &json!({"answer":"ok"}));
    }
    for text in [
        r#"{"answer":"a","answer":"b"}"#,
        r#"{"answer":1}"#,
        r#"{"answer":"a","extra":true}"#,
        "{}",
        "```json\n{}\n```",
        "{} {}",
    ] {
        let response = ChatResponse::new(AssistantMessage::text(text), FinishReason::Stop);
        assert!(output.validate_response(&response).is_err());
    }
}

#[test]
fn schema_admission_and_canonical_identity() {
    let first = StructuredOutput::new("answer", schema()).unwrap();
    let second = StructuredOutput::new("answer", serde_json::from_str(
        r#"{"additionalProperties":false,"required":["answer"],"properties":{"answer":{"type":"string"}},"type":"object"}"#
    ).unwrap()).unwrap();
    assert_eq!(
        first.contract_id(),
        "2000bca08b38b22c4158a699f242f7735cf42135efa07416d6d84c3c13d298bc"
    );
    assert_eq!(first, second);
    assert_eq!(first.contract_id(), second.contract_id());
    assert_ne!(
        first.contract_id(),
        StructuredOutput::new("other", schema())
            .unwrap()
            .contract_id()
    );
    for bad in [
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"type":"string"}),
        json!({"type":"object","properties":{},"required":[],"additionalProperties":true}),
    ] {
        assert!(StructuredOutput::new("answer", bad).is_err());
    }
}

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use group_agent_model::{
    ChatEventStream, ChatModel, ChatModelAdapter, ChatRequest, ChatStreamEvent, Message,
    ModelCapabilities, ModelError, ModelErrorKind, ModelId, ModelMetadata, ProviderId,
    Retryability, StructuredOutputError, ToolCall, ToolCallId, ToolName, ValidatedChatRequest,
    collect_chat_stream,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Fake {
    metadata: ModelMetadata,
    response: ChatResponse,
    events: Vec<ChatStreamEvent>,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl ChatModelAdapter for Fake {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }
    async fn complete_raw(&self, _: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.response.clone())
    }
    async fn stream_raw(&self, _: ValidatedChatRequest) -> Result<ChatEventStream, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::iter(
            self.events.clone().into_iter().map(Ok),
        )))
    }
}
fn model(
    text: &str,
    finish: FinishReason,
    supported: bool,
    events: Vec<ChatStreamEvent>,
) -> (ChatModel, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let model = ChatModel::from_adapter(Fake {
        metadata: ModelMetadata::new(
            ProviderId::new("fake").unwrap(),
            ModelId::new("fake").unwrap(),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_structured_output(supported),
        ),
        response: ChatResponse::new(AssistantMessage::text(text), finish),
        events,
        calls: calls.clone(),
    })
    .unwrap();
    (model, calls)
}
fn request() -> ChatRequest {
    ChatRequest::new(vec![Message::user("question")])
        .with_structured_output(StructuredOutput::new("answer", schema()).unwrap())
}
#[tokio::test]
async fn facade_rejects_before_dispatch_and_validates_untrusted_responses() {
    let (model, calls) = model("{}", FinishReason::Stop, false, vec![]);
    assert!(matches!(
        model.complete(request()).await.unwrap_err().kind(),
        ModelErrorKind::UnsupportedCapability(_)
    ));
    assert!(model.stream(request()).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for text in ["not json", "{}", r#"{"answer":false}"#] {
        let (model, calls) = self::model(text, FinishReason::Stop, true, vec![]);
        let error = model.complete(request()).await.unwrap_err();
        assert_eq!(error.kind(), &ModelErrorKind::OutputValidation);
        assert_eq!(error.retryability(), Retryability::Never);
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<StructuredOutputError>()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn streamed_json_fragments_validate_only_at_clean_eof() {
    let text = r#"{"answer":"你好"}"#;
    let mut events: Vec<_> = text
        .chars()
        .map(|c| ChatStreamEvent::TextDelta(c.to_string()))
        .collect();
    events.push(ChatStreamEvent::Finished(FinishReason::Stop));
    let (model, _) = model(text, FinishReason::Stop, true, events);
    let response = collect_chat_stream(model.stream(request()).await.unwrap())
        .await
        .unwrap();
    let output = StructuredOutput::new("answer", schema()).unwrap();
    let validated = output.validate_response(&response).unwrap().unwrap();
    #[derive(serde::Deserialize)]
    struct Answer {
        answer: String,
    }
    assert_eq!(validated.deserialize::<Answer>().unwrap().answer, "你好");
    assert!(validated.deserialize::<Vec<String>>().is_err());
    assert!(!format!("{validated:?}").contains("你好"));
}
#[tokio::test]
async fn invalid_or_post_terminal_stream_never_emits_success() {
    let good = ChatStreamEvent::TextDelta(r#"{"answer":"ok"}"#.into());
    for events in [
        vec![good.clone()],
        vec![
            good.clone(),
            ChatStreamEvent::Finished(FinishReason::Length),
        ],
        vec![
            ChatStreamEvent::TextDelta("{}".into()),
            ChatStreamEvent::Finished(FinishReason::Stop),
        ],
        vec![
            good.clone(),
            ChatStreamEvent::Finished(FinishReason::Stop),
            good.clone(),
        ],
        vec![
            good,
            ChatStreamEvent::Finished(FinishReason::Stop),
            ChatStreamEvent::Finished(FinishReason::Stop),
        ],
    ] {
        let (model, _) = model("", FinishReason::Stop, true, events);
        let mut stream = model.stream(request()).await.unwrap();
        let mut errors = 0;
        while let Some(item) = stream.next().await {
            match item {
                Err(_) => errors += 1,
                Ok(ChatStreamEvent::Finished(_)) => panic!("false success"),
                _ => {}
            }
        }
        assert_eq!(errors, 1);
        assert!(stream.next().await.is_none());
    }
}
#[tokio::test]
async fn tool_commentary_limits_match_complete_and_stream() {
    let contract = StructuredOutput::new("answer", schema()).unwrap();
    for size in [1024 * 1024, 1024 * 1024 + 1] {
        let text = "x".repeat(size);
        let call = ToolCall::new(
            ToolCallId::new("call").unwrap(),
            ToolName::new("tool").unwrap(),
            json!({}),
        );
        let response = ChatResponse::new(
            AssistantMessage::new(
                vec![group_agent_model::ContentPart::text(&text)],
                vec![call],
            ),
            FinishReason::ToolCalls,
        );
        assert_eq!(
            contract.validate_response(&response).is_ok(),
            size == 1024 * 1024
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let complete = ChatModel::from_adapter(Fake {
            metadata: ModelMetadata::new(
                ProviderId::new("fake").unwrap(),
                ModelId::new("fake").unwrap(),
                ModelCapabilities::new().with_structured_output(true),
            ),
            response,
            events: vec![],
            calls: calls.clone(),
        })
        .unwrap();
        let result = complete.complete(request()).await;
        assert_eq!(result.is_ok(), size == 1024 * 1024);
        if let Err(error) = result {
            assert_eq!(error.kind(), &ModelErrorKind::OutputValidation);
            assert!(
                std::error::Error::source(&error)
                    .unwrap()
                    .is::<StructuredOutputError>()
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = vec![
            ChatStreamEvent::TextDelta(text),
            ChatStreamEvent::ToolCallDelta(
                group_agent_model::ToolCallDelta::new(0)
                    .with_id(ToolCallId::new("call").unwrap())
                    .with_name(ToolName::new("tool").unwrap())
                    .with_arguments_fragment("{}"),
            ),
            ChatStreamEvent::Finished(FinishReason::ToolCalls),
        ];
        let (model, _) = model("", FinishReason::Stop, true, events);
        let result = collect_chat_stream(model.stream(request()).await.unwrap()).await;
        assert_eq!(result.is_ok(), size == 1024 * 1024);
        if let Err(error) = result {
            assert_eq!(error.kind(), &ModelErrorKind::OutputValidation);
        }
    }
}
#[test]
fn schema_limits_and_typed_sources_are_payload_safe() {
    let mut bad = schema();
    bad["properties"]["answer"]["description"] = json!("x".repeat(16385));
    assert!(StructuredOutput::new("answer", bad).is_err());
    let mut bad = schema();
    bad["properties"]["answer"]["pattern"] = json!("SECRET_SCHEMA");
    let error = StructuredOutput::new("answer", bad).unwrap_err();
    assert!(!format!("{error:?} {error}").contains("SECRET_SCHEMA"));
    let output = StructuredOutput::new("answer", schema()).unwrap();
    for finish in [
        FinishReason::Length,
        FinishReason::ContentFilter,
        FinishReason::Error,
        FinishReason::ToolCalls,
        FinishReason::Other("secret".into()),
    ] {
        let response = ChatResponse::new(
            AssistantMessage::text(r#"{"answer":"SECRET_VALUE"}"#),
            finish,
        );
        let error = output.validate_response(&response).unwrap_err();
        assert!(!format!("{error:?} {error}").contains("SECRET_VALUE"));
    }
}
