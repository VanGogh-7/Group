#![cfg(feature = "agent-sequence")]
#[path = "../test_support/sequence_agent.rs"]
mod support;
use group_agent_core::*;
use group_agent_model::Message;
use group_agent_prebuilt::*;
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn saved_second_approval_resumes_without_first_stage_work() {
    for decision in [
        AgentApprovalDecision::Approve,
        AgentApprovalDecision::Reject,
    ] {
        let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
        let (sequence, probes) = support::sequence([false, true], [2, 2]);
        let interrupted = sequence
            .invoke_with_checkpoint(
                vec![Message::user("q")],
                CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .await
            .unwrap();
        let saved = interrupted.as_interrupted().unwrap();
        assert_eq!(saved.approval_request().unwrap().stage().as_str(), "b");
        assert_eq!(probes[0].executions.load(Ordering::SeqCst), 1);
        assert_eq!(probes[1].executions.load(Ordering::SeqCst), 0);
        let (fresh, probes) = support::sequence([false, true], [2, 2]);
        let result = fresh
            .resume(
                ResumeConfig::new("s", store.clone())
                    .with_checkpoint_id(saved.checkpoint_id())
                    .with_resume_value(SequenceApprovalDecision::new(
                        AgentStageId::new("b").unwrap(),
                        decision,
                    ))
                    .with_checkpoint_policy(CheckpointPolicy::FinalOnly),
            )
            .await
            .unwrap();
        assert!(result.as_completed().unwrap().final_output().is_some());
        assert_eq!(
            probes[0].completions.load(Ordering::SeqCst)
                + probes[0].executions.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            probes[1].executions.load(Ordering::SeqCst),
            usize::from(decision == AgentApprovalDecision::Approve)
        );
        let head = store.latest(&ThreadId::from("s")).await.unwrap().unwrap();
        let (fresh, probes) = support::sequence([false, true], [2, 2]);
        assert!(
            fresh
                .resume(ResumeConfig::new("s", store.clone()))
                .await
                .unwrap()
                .is_completed()
        );
        assert!(
            fresh
                .replay(ReplayConfig::new("s", head.id(), store.clone()))
                .await
                .unwrap()
                .outcome()
                .is_completed()
        );
        assert!(
            fresh
                .fork(ForkConfig::new("s", head.id(), store))
                .await
                .unwrap()
                .outcome()
                .is_completed()
        );
        assert_eq!(
            probes
                .iter()
                .map(|p| p.completions.load(Ordering::SeqCst) + p.executions.load(Ordering::SeqCst))
                .sum::<usize>(),
            0
        );
    }
}
#[tokio::test]
async fn wrong_stage_unpinned_or_missing_decision_never_executes_tools() {
    let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
    let (seq, _) = support::sequence([true, false], [2, 2]);
    let result = seq
        .invoke_with_checkpoint(
            vec![Message::user("q")],
            CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
        )
        .await
        .unwrap();
    let saved = result.as_interrupted().unwrap();
    for mode in 0..3 {
        let (seq, probe) = support::sequence([true, false], [2, 2]);
        let config = ResumeConfig::new("s", store.clone());
        let config = match mode {
            0 => config.with_resume_value(SequenceApprovalDecision::new(
                AgentStageId::new("a").unwrap(),
                AgentApprovalDecision::Approve,
            )),
            1 => config
                .with_checkpoint_id(saved.checkpoint_id())
                .with_resume_value(SequenceApprovalDecision::new(
                    AgentStageId::new("b").unwrap(),
                    AgentApprovalDecision::Approve,
                )),
            _ => config.with_checkpoint_id(saved.checkpoint_id()),
        };
        assert!(seq.resume(config).await.is_err());
        assert_eq!(probe[0].executions.load(Ordering::SeqCst), 0);
        assert_eq!(
            store
                .latest(&ThreadId::from("s"))
                .await
                .unwrap()
                .unwrap()
                .id(),
            saved.checkpoint_id()
        );
    }
}
#[tokio::test]
async fn stage_round_limits_stop_without_extra_work() {
    for rounds in [[1, 2], [2, 1]] {
        let (seq, probe) = support::sequence([false, false], rounds);
        let result = seq.invoke(vec![Message::user("q")]).await.unwrap();
        assert_eq!(result.stop_reason(), AgentStopReason::MaxRounds);
        assert!(result.final_output().is_none());
        if rounds[0] == 1 {
            assert!(result.second().is_none());
            assert_eq!(probe[1].completions.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn first_stage_approval_preserves_typed_handoff_and_step_overflow_fails_admission() {
    for decision in [
        AgentApprovalDecision::Approve,
        AgentApprovalDecision::Reject,
    ] {
        let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
        let (seq, _) = support::sequence([true, false], [2, 2]);
        let run = seq
            .invoke_with_checkpoint(
                vec![Message::user("q")],
                CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .await
            .unwrap();
        let saved = run.as_interrupted().unwrap();
        assert_eq!(saved.approval_request().unwrap().stage().as_str(), "a");
        let (seq, probes) = support::sequence([true, false], [2, 2]);
        let result = seq
            .resume(
                ResumeConfig::new("s", store)
                    .with_checkpoint_id(saved.checkpoint_id())
                    .with_resume_value(SequenceApprovalDecision::new(
                        AgentStageId::new("a").unwrap(),
                        decision,
                    )),
            )
            .await
            .unwrap();
        assert!(result.as_completed().unwrap().final_output().is_some());
        assert_eq!(
            probes[0].executions.load(Ordering::SeqCst),
            usize::from(decision == AgentApprovalDecision::Approve)
        );
        assert_eq!(probes[1].executions.load(Ordering::SeqCst), 1);
    }
    let (a, _) = support::stage("a", false, usize::MAX / 2);
    let (b, _) = support::stage("b", false, 2);
    assert!(
        AgentSequence::new(
            "v1",
            a,
            b,
            Arc::new(
                |_: &group_agent_model::ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> {
                    unreachable!()
                }
            )
        )
        .is_err()
    );
}
