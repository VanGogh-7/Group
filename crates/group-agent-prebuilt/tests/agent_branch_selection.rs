#![cfg(feature = "agent-branch")]
use async_trait::async_trait;
use group_agent_model::*;
use group_agent_prebuilt::*;
use group_agent_tool::{ToolRegistry, ToolRuntime};
use serde_json::json;
use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};
struct Model {
    metadata: ModelMetadata,
    calls: Arc<AtomicUsize>,
    expected: &'static str,
    field: &'static str,
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
        self.calls.fetch_add(1, SeqCst);
        assert_eq!(request.messages(), &[Message::user(self.expected)]);
        Ok(ChatResponse::new(
            AssistantMessage::text(json!({self.field:"ok"}).to_string()),
            FinishReason::Stop,
        ))
    }
}
fn stage(
    id: &str,
    field: &'static str,
    expected: &'static str,
    calls: Arc<AtomicUsize>,
) -> AgentStage {
    AgentStage::new(AgentStageId::new(id).unwrap(), ChatModel::from_adapter(Model {
        metadata: ModelMetadata::new(ProviderId::new("offline").unwrap(), ModelId::new("typed").unwrap(), ModelCapabilities::new().with_structured_output(true)),
        calls, expected, field,
    }).unwrap(), ToolRuntime::new(ToolRegistry::builder().build()), AgentConfig::default(),
    StructuredOutput::new(field, json!({"type":"object","properties":{(field):{"type":"string"}},"required":[field],"additionalProperties":false})).unwrap()).unwrap()
}
#[tokio::test]
async fn distinct_typed_outputs_and_isolated_inputs_for_all_choices() {
    #[derive(serde::Deserialize)]
    struct B {
        b: String,
    }
    #[derive(serde::Deserialize)]
    struct C {
        c: String,
    }
    for target in [BranchTarget::B, BranchTarget::C, BranchTarget::Complete] {
        let calls = Arc::new(AtomicUsize::new(0));
        let branch = AgentBranch::new(
            "v1",
            stage("a", "a", "initial", calls.clone()),
            stage("b", "b", "b-only", calls.clone()),
            stage("c", "c", "c-only", calls.clone()),
            Arc::new(move |a: &ValidatedJsonOutput| {
                assert_eq!(a.value(), &json!({"a":"ok"}));
                Ok(match target {
                    BranchTarget::B => BranchSelection::B(vec![Message::user("b-only")]),
                    BranchTarget::C => BranchSelection::C(vec![Message::user("c-only")]),
                    BranchTarget::Complete => BranchSelection::Complete,
                })
            }),
        )
        .unwrap();
        let result = branch.invoke(vec![Message::user("initial")]).await.unwrap();
        let output = result.final_output().unwrap();
        match target {
            BranchTarget::B => assert_eq!(output.deserialize::<B>().unwrap().b, "ok"),
            BranchTarget::C => assert_eq!(output.deserialize::<C>().unwrap().c, "ok"),
            BranchTarget::Complete => assert_eq!(output.value(), &json!({"a":"ok"})),
        }
        assert_eq!(
            calls.load(SeqCst),
            if target == BranchTarget::Complete {
                1
            } else {
                2
            }
        );
        assert_eq!(
            result.stopping_stage().as_str(),
            match target {
                BranchTarget::B => "b",
                BranchTarget::C => "c",
                BranchTarget::Complete => "a",
            }
        );
    }
}
#[tokio::test]
async fn selector_errors_panics_and_invalid_messages_never_dispatch_downstream() {
    for mode in 0..6 {
        let calls = Arc::new(AtomicUsize::new(0));
        let branch = AgentBranch::new(
            "v1",
            stage("a", "a", "initial", calls.clone()),
            stage("b", "b", "unused", calls.clone()),
            stage("c", "c", "unused", calls.clone()),
            Arc::new(move |_: &ValidatedJsonOutput| {
                let messages = match mode {
                    0 => {
                        return Err(BranchSelectionError::with_source(std::io::Error::other(
                            "SECRET_SOURCE",
                        )));
                    }
                    1 => panic!("SECRET_PANIC"),
                    2 => vec![],
                    3 => vec![Message::Tool(ToolMessage::new(
                        ToolCallId::new("unknown").unwrap(),
                        ToolResult::text("SECRET"),
                    ))],
                    _ => {
                        let pending = Message::Assistant(AssistantMessage::new(
                            vec![],
                            vec![ToolCall::new(
                                ToolCallId::new("pending").unwrap(),
                                ToolName::new("lookup").unwrap(),
                                json!({}),
                            )],
                        ));
                        if mode == 4 {
                            vec![pending]
                        } else {
                            vec![pending, Message::user("interleaved")]
                        }
                    }
                };
                Ok(BranchSelection::C(messages))
            }),
        )
        .unwrap();
        let error = branch
            .invoke(vec![Message::user("initial")])
            .await
            .unwrap_err();
        assert_eq!(calls.load(SeqCst), 1);
        assert_eq!(error.stage().unwrap().as_str(), "a");
        assert_eq!(
            error.kind(),
            if mode == 1 {
                BranchErrorKind::SelectorPanicked
            } else {
                BranchErrorKind::Graph
            }
        );
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        assert!(
            error
                .source()
                .unwrap()
                .is::<group_agent_core::GraphRunError>()
        );
        if mode == 0 {
            let mut source = error.source();
            let mut found = false;
            while let Some(e) = source {
                found |= e.is::<std::io::Error>();
                source = e.source();
            }
            assert!(found);
        }
    }
    assert!(!format!("{:?}", BranchSelection::B(vec![Message::user("SECRET")])).contains("SECRET"));
}
#[test]
fn invalid_revision_and_any_duplicate_stage_pair_fail_before_dispatch() {
    for (revision, ids) in [
        ("bad/revision", ["a", "b", "c"]),
        ("v1", ["a", "a", "c"]),
        ("v1", ["a", "b", "a"]),
        ("v1", ["a", "b", "b"]),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let error = AgentBranch::new(
            revision,
            stage(ids[0], "a", "unused", calls.clone()),
            stage(ids[1], "b", "unused", calls.clone()),
            stage(ids[2], "c", "unused", calls.clone()),
            Arc::new(|_: &ValidatedJsonOutput| unreachable!()),
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), BranchErrorKind::Configuration);
        assert_eq!(calls.load(SeqCst), 0);
    }
}
