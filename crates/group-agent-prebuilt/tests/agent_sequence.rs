#![cfg(feature = "agent-sequence")]
use async_trait::async_trait;
use group_agent_model::*;
use group_agent_prebuilt::*;
use group_agent_tool::{ToolRegistry, ToolRuntime};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct Model {
    metadata: ModelMetadata,
    count: Arc<AtomicUsize>,
    expected: &'static str,
}
#[async_trait]
impl ChatModelAdapter for Model {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }
    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.messages().len(), 1);
        assert_eq!(request.messages()[0], Message::user(self.expected));
        Ok(ChatResponse::new(
            AssistantMessage::text(r#"{"answer":"ok"}"#),
            FinishReason::Stop,
        ))
    }
}
fn stage(id: &str, expected: &'static str, count: Arc<AtomicUsize>) -> AgentStage {
    let model = ChatModel::from_adapter(Model {
        metadata: ModelMetadata::new(
            ProviderId::new("test").unwrap(),
            ModelId::new("test").unwrap(),
            ModelCapabilities::new().with_structured_output(true),
        ),
        count,
        expected,
    })
    .unwrap();
    let output = StructuredOutput::new("answer", json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false})).unwrap();
    AgentStage::new(
        AgentStageId::new(id).unwrap(),
        model,
        ToolRuntime::new(ToolRegistry::builder().build()),
        AgentConfig::default(),
        output,
    )
    .unwrap()
}
#[tokio::test]
async fn typed_handoff_isolates_histories_and_runs_each_stage_once() {
    let count = Arc::new(AtomicUsize::new(0));
    let mapper = Arc::new(
        |output: &ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> {
            #[derive(serde::Deserialize)]
            struct Answer {
                answer: String,
            }
            let answer: Answer = output.deserialize().map_err(HandoffError::with_source)?;
            assert_eq!(answer.answer, "ok");
            Ok(vec![Message::user("mapped")])
        },
    );
    let sequence = AgentSequence::new(
        "v1",
        stage("first", "initial", count.clone()),
        stage("second", "mapped", count.clone()),
        mapper,
    )
    .unwrap();
    let result = sequence
        .invoke(vec![Message::user("initial")])
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(
        result.final_output().unwrap().value(),
        &json!({"answer":"ok"})
    );
    assert_eq!(result.stopping_stage().as_str(), "second");
}

#[tokio::test]
async fn mapper_failure_or_incomplete_messages_never_dispatch_second_stage() {
    for invalid in 0..5 {
        let count = Arc::new(AtomicUsize::new(0));
        let mapper = Arc::new(
            move |_: &ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> {
                let pending = Message::Assistant(AssistantMessage::new(
                    vec![],
                    vec![ToolCall::new(
                        ToolCallId::new("pending").unwrap(),
                        ToolName::new("lookup").unwrap(),
                        json!({}),
                    )],
                ));
                match invalid {
                    0 => Err(HandoffError::with_source(std::io::Error::other(
                        "SECRET_MAPPER",
                    ))),
                    1 => Ok(vec![]),
                    2 => Ok(vec![Message::Tool(ToolMessage::new(
                        ToolCallId::new("unknown").unwrap(),
                        ToolResult::text("SECRET"),
                    ))]),
                    3 => Ok(vec![pending]),
                    _ => Ok(vec![pending, Message::user("interleaved")]),
                }
            },
        );
        let seq = AgentSequence::new(
            "v1",
            stage("a", "initial", count.clone()),
            stage("b", "unused", count.clone()),
            mapper,
        )
        .unwrap();
        let error = seq
            .invoke(vec![Message::user("initial")])
            .await
            .unwrap_err();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(error.stage().unwrap().as_str(), "a");
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        if invalid == 0 {
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
            let mut found = false;
            while let Some(error) = source {
                found |= error.is::<std::io::Error>();
                source = error.source();
            }
            assert!(found, "concrete mapper source remains reachable");
        }

        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<group_agent_core::GraphRunError>()
        );
    }
}
#[test]
fn invalid_identity_and_duplicate_stage_names_fail_admission() {
    for id in ["", "bad/name", "你好"] {
        assert!(AgentStageId::new(id).is_err());
    }
    let count = Arc::new(AtomicUsize::new(0));
    let mapper = Arc::new(
        |_: &ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> { unreachable!() },
    );
    assert!(
        AgentSequence::new(
            "v1",
            stage("a", "unused", count.clone()),
            stage("a", "unused", count.clone()),
            mapper
        )
        .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[test]
fn unsupported_output_capability_fails_stage_construction_without_dispatch() {
    let count = Arc::new(AtomicUsize::new(0));
    let model = ChatModel::from_adapter(Model {
        metadata: ModelMetadata::new(
            ProviderId::new("fake").unwrap(),
            ModelId::new("fake").unwrap(),
            ModelCapabilities::new(),
        ),
        count: count.clone(),
        expected: "unused",
    })
    .unwrap();
    let output = StructuredOutput::new(
        "empty",
        json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
    )
    .unwrap();
    let error = AgentStage::new(
        AgentStageId::new("a").unwrap(),
        model,
        ToolRuntime::new(ToolRegistry::builder().build()),
        AgentConfig::default(),
        output,
    )
    .unwrap_err();
    assert_eq!(error.kind(), SequenceErrorKind::Configuration);
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .is::<ModelError>()
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
