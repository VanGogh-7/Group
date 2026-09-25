#![cfg(feature = "structured-output")]
#[path = "../test_support/structured_agent.rs"]
mod support;
use futures_util::StreamExt;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, Checkpointer, ForkConfig, InMemoryCheckpointer,
    ReplayConfig, ResumeConfig, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentSnapshotCodec, AgentStopReason, AgentStreamEvent,
};
use serde_json::json;
use std::sync::{Arc, atomic::Ordering};
use support::{agent, output};

#[tokio::test]
async fn tools_then_typed_result_work_for_complete_stream_and_sink() {
    for mode in 0..3 {
        let (agent, probe) = agent(false, 2, Some(output("answer")), false);
        let messages = vec![Message::user("question")];
        let result = match mode {
            0 => agent.invoke(messages).await.unwrap(),
            1 => {
                let events = agent.stream(messages).collect::<Vec<_>>().await;
                let Some(Ok(AgentStreamEvent::Completed(result))) = events.last() else {
                    panic!("missing completion")
                };
                result.clone()
            }
            _ => agent
                .invoke_with_stream_sink(messages, Arc::new(support::fixture::Events::default()))
                .await
                .unwrap(),
        };
        assert_eq!(
            result.structured_output().unwrap().value(),
            &json!({"answer":"ok"})
        );
        assert_eq!(result.model_rounds(), 2);
        assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
        assert!(
            result
                .structured_output()
                .unwrap()
                .deserialize::<u64>()
                .is_err()
        );
        assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn max_rounds_and_plain_construction_do_not_claim_structured_result() {
    for contract in [None, Some(output("answer"))] {
        let (agent, probe) = agent(false, 1, contract, false);
        let result = agent.invoke(vec![Message::user("question")]).await.unwrap();
        assert_eq!(result.stop_reason(), AgentStopReason::MaxRounds);
        assert!(result.structured_output().is_none());
        assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    }
    let (agent, _) = agent(false, 2, None, false);
    assert!(
        agent
            .invoke(vec![Message::user("question")])
            .await
            .unwrap()
            .structured_output()
            .is_none()
    );
}
#[tokio::test]
async fn invalid_final_keeps_saved_tool_result_and_emits_no_completed() {
    for streaming in [false, true] {
        let (agent, _) = agent(false, 2, Some(output("answer")), true);
        let store = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
        let config =
            CheckpointConfig::new("invalid", store.clone(), CheckpointPolicy::EverySuperstep);
        if streaming {
            let events = agent
                .stream_with_checkpoint(vec![Message::user("question")], config)
                .collect::<Vec<_>>()
                .await;
            assert_eq!(events.iter().filter(|e| e.is_err()).count(), 1);
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, Ok(AgentStreamEvent::Completed(_))))
            );
        } else {
            assert!(
                agent
                    .invoke_with_checkpoint(vec![Message::user("question")], config)
                    .await
                    .is_err()
            );
        }
        let head = store
            .latest(&ThreadId::from("invalid"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.superstep(), 2);
        assert!(!head.completed());
    }
}
#[tokio::test]
async fn approval_resume_completed_resume_replay_and_fork_keep_contract() {
    for decision in [
        AgentApprovalDecision::Approve,
        AgentApprovalDecision::Reject,
    ] {
        let store = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
        let (first, _) = agent(true, 2, Some(output("answer")), false);
        let events = first
            .stream_with_checkpoint(
                vec![Message::user("question")],
                CheckpointConfig::new("approval", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            events.last(),
            Some(Ok(AgentStreamEvent::Interrupted(_)))
        ));
        let (fresh, probe) = agent(true, 2, Some(output("answer")), false);
        let resumed = fresh
            .resume(ResumeConfig::new("approval", store.clone()).with_resume_value(decision))
            .await
            .unwrap();
        assert_eq!(
            resumed
                .as_completed()
                .unwrap()
                .structured_output()
                .unwrap()
                .value(),
            &json!({"answer":"ok"})
        );
        assert_eq!(
            probe.executions.load(Ordering::SeqCst),
            usize::from(decision == AgentApprovalDecision::Approve)
        );
        let head = store
            .latest(&ThreadId::from("approval"))
            .await
            .unwrap()
            .unwrap();
        let (fresh, probe) = agent(true, 2, Some(output("answer")), false);
        let events = fresh
            .resume_stream(ResumeConfig::new("approval", store.clone()))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0],Ok(AgentStreamEvent::Completed(result)) if result.structured_output().is_some())
        );
        let replay = fresh
            .replay(ReplayConfig::new("approval", head.id(), store.clone()))
            .await
            .unwrap();
        assert!(
            replay
                .outcome()
                .as_completed()
                .unwrap()
                .structured_output()
                .is_some()
        );
        let fork = fresh
            .fork(ForkConfig::new("approval", head.id(), store.clone()))
            .await
            .unwrap();
        assert!(
            fork.outcome()
                .as_completed()
                .unwrap()
                .structured_output()
                .is_some()
        );
        assert_eq!(
            probe.streams.load(Ordering::SeqCst)
                + probe.completions.load(Ordering::SeqCst)
                + probe.executions.load(Ordering::SeqCst),
            0
        );
    }
}
#[tokio::test]
async fn changed_contract_or_plain_graph_cannot_resume_replay_or_fork() {
    let store = Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec));
    let (first, _) = agent(false, 2, Some(output("answer")), false);
    first
        .invoke_with_checkpoint(
            vec![Message::user("question")],
            CheckpointConfig::new("same", store.clone(), CheckpointPolicy::EverySuperstep),
        )
        .await
        .unwrap();
    let head = store
        .latest(&ThreadId::from("same"))
        .await
        .unwrap()
        .unwrap();
    let mut changed_schema = output("answer").schema().clone();
    changed_schema["description"] = json!("new annotation");
    for contract in [
        None,
        Some(output("different")),
        Some(group_agent_model::StructuredOutput::new("answer", changed_schema).unwrap()),
    ] {
        let (changed, probe) = agent(false, 2, contract, false);
        assert!(
            changed
                .resume(ResumeConfig::new("same", store.clone()))
                .await
                .is_err()
        );
        assert!(
            changed
                .replay(ReplayConfig::new("same", head.id(), store.clone()))
                .await
                .is_err()
        );
        let fork = ForkConfig::new("same", head.id(), store.clone());
        let branch = fork.branch_id();
        let error = changed.fork(fork).await.unwrap_err();
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<group_agent_core::GraphRunError>()
        );
        assert!(
            store
                .branch_head(&ThreadId::from("same"), branch)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            probe.streams.load(Ordering::SeqCst)
                + probe.completions.load(Ordering::SeqCst)
                + probe.executions.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            store
                .latest(&ThreadId::from("same"))
                .await
                .unwrap()
                .unwrap()
                .id(),
            head.id()
        );
    }
}

// A hostile/custom decoder models saved snapshots that retain graph identity
// while losing their final message. Conversion must not report valid completion.
struct ChangedSnapshot(Vec<Message>);
impl group_agent_core::CheckpointCodec<group_agent_prebuilt::AgentSnapshot> for ChangedSnapshot {
    fn snapshot_descriptor(&self) -> group_agent_core::CodecDescriptor {
        group_agent_core::CheckpointCodec::snapshot_descriptor(&AgentSnapshotCodec)
    }
    fn encode_snapshot(
        &self,
        snapshot: &group_agent_prebuilt::AgentSnapshot,
    ) -> Result<Vec<u8>, group_agent_core::CheckpointCodecError> {
        group_agent_core::CheckpointCodec::encode_snapshot(&AgentSnapshotCodec, snapshot)
    }
    fn decode_snapshot(
        &self,
        bytes: &[u8],
    ) -> Result<group_agent_prebuilt::AgentSnapshot, group_agent_core::CheckpointCodecError> {
        let mut value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        value["messages"] = serde_json::to_value(&self.0).unwrap();
        Ok(serde_json::from_value(value).unwrap())
    }
}
#[tokio::test]
async fn malformed_completed_snapshots_fail_conversion_without_model_or_tool_calls() {
    use group_agent_core::{InMemoryCheckpointStore, RecordCheckpointer};
    use group_agent_model::{AssistantMessage, StructuredOutputError};
    use std::error::Error;
    let raw = Arc::new(InMemoryCheckpointStore::new());
    let original = Arc::new(RecordCheckpointer::new(
        raw.clone(),
        Arc::new(AgentSnapshotCodec),
    ));
    let (first, _) = agent(false, 2, Some(output("answer")), false);
    first
        .invoke_with_checkpoint(
            vec![Message::user("question")],
            CheckpointConfig::new(
                "corrupt",
                original.clone(),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .unwrap();
    let head = original
        .latest(&ThreadId::from("corrupt"))
        .await
        .unwrap()
        .unwrap();
    for messages in [
        vec![],
        vec![Message::user("SECRET")],
        vec![Message::Assistant(AssistantMessage::text("{}"))],
    ] {
        let store = Arc::new(RecordCheckpointer::new(
            raw.clone(),
            Arc::new(ChangedSnapshot(messages)),
        ));
        let (fresh, probe) = agent(false, 2, Some(output("answer")), false);
        let error = fresh
            .resume(ResumeConfig::new("corrupt", store.clone()))
            .await
            .unwrap_err();
        assert!(error.source().unwrap().is::<StructuredOutputError>());
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        let error = fresh
            .replay(ReplayConfig::new("corrupt", head.id(), store.clone()))
            .await
            .unwrap_err();
        assert!(error.source().unwrap().is::<StructuredOutputError>());
        let fork = ForkConfig::new("corrupt", head.id(), store.clone());
        let branch = fork.branch_id();
        let error = fresh.fork(fork).await.unwrap_err();
        // Core already persisted this branch before output conversion failed.
        assert!(
            store
                .branch_head(&ThreadId::from("corrupt"), branch)
                .await
                .unwrap()
                .is_some()
        );
        let error_sink = fresh
            .resume_with_stream_sink(
                ResumeConfig::new("corrupt", store.clone()),
                Arc::new(support::fixture::Events::default()),
            )
            .await
            .unwrap_err();
        assert!(error_sink.source().unwrap().is::<StructuredOutputError>());
        assert!(error.source().unwrap().is::<StructuredOutputError>());
        let events = fresh
            .resume_stream(ResumeConfig::new("corrupt", store))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events.len(), 1);
        assert!(events[0].is_err());
        assert_eq!(
            probe.completions.load(Ordering::SeqCst)
                + probe.streams.load(Ordering::SeqCst)
                + probe.executions.load(Ordering::SeqCst),
            0
        );
    }
}
