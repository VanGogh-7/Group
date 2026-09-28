#![cfg(feature = "agent-branch")]
#[path = "../test_support/branch_agent.rs"]
mod support;
use group_agent_core::*;
use group_agent_model::Message;
use group_agent_prebuilt::*;
use std::error::Error;
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn every_selected_target_recovers_and_rejects_wrong_stage_decisions() {
    for target in [BranchTarget::B, BranchTarget::C] {
        let index = if target == BranchTarget::B { 1 } else { 2 };
        let id = if target == BranchTarget::B { "b" } else { "c" };
        for decision in [
            AgentApprovalDecision::Approve,
            AgentApprovalDecision::Reject,
        ] {
            let store = Arc::new(InMemoryCheckpointer::new(BranchSnapshotCodec));
            let (branch, _) = support::choose(target, [false, true, true], [2; 3]);
            let run = branch
                .invoke_with_checkpoint(
                    vec![Message::user("q")],
                    CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
                )
                .await
                .unwrap();
            let saved = run.as_interrupted().unwrap();
            assert_eq!(saved.approval_request().unwrap().stage().as_str(), id);
            let (fresh, probes) = support::choose(target, [false, true, true], [2; 3]);
            for wrong in ["a", if id == "b" { "c" } else { "b" }] {
                assert!(
                    fresh
                        .resume(
                            ResumeConfig::new("s", store.clone())
                                .with_checkpoint_id(saved.checkpoint_id())
                                .with_resume_value(BranchApprovalDecision::new(
                                    AgentStageId::new(wrong).unwrap(),
                                    decision
                                ))
                        )
                        .await
                        .is_err()
                );
            }
            let result = fresh
                .resume(
                    ResumeConfig::new("s", store.clone())
                        .with_checkpoint_id(saved.checkpoint_id())
                        .with_resume_value(BranchApprovalDecision::new(
                            AgentStageId::new(id).unwrap(),
                            decision,
                        )),
                )
                .await
                .unwrap();
            let outcome = result.as_completed().unwrap();
            assert_eq!(outcome.selected(), Some(target));
            let tool = outcome
                .downstream()
                .unwrap()
                .messages()
                .iter()
                .find_map(Message::as_tool)
                .unwrap();
            assert_eq!(tool.tool_call_id().as_str(), "call-1");
            assert_eq!(
                tool.result().is_error(),
                decision == AgentApprovalDecision::Reject
            );
            assert_eq!(
                probes[index].executions.load(Ordering::SeqCst),
                usize::from(decision == AgentApprovalDecision::Approve)
            );
            for i in [0, 3 - index] {
                assert_eq!(
                    probes[i].completions.load(Ordering::SeqCst)
                        + probes[i].executions.load(Ordering::SeqCst),
                    0
                );
            }
        }
    }
}

#[tokio::test]
async fn saved_selection_is_authoritative_for_resume_replay_and_fork_including_complete() {
    for target in [BranchTarget::B, BranchTarget::C, BranchTarget::Complete] {
        let raw = Arc::new(InMemoryCheckpointStore::new());
        let store = Arc::new(RecordCheckpointer::new(
            raw.clone(),
            Arc::new(BranchSnapshotCodec),
        ));
        let (branch, _) = support::choose(target, [false; 3], [2; 3]);
        branch
            .invoke_with_checkpoint(
                vec![Message::user("q")],
                CheckpointConfig::new("s", store, CheckpointPolicy::EverySuperstep),
            )
            .await
            .unwrap();
        let records = raw.history(&ThreadId::from("s")).await.unwrap();
        for operation in 0..3 {
            let truncated = records
                .iter()
                .filter(|r| r.superstep() <= 4)
                .map(|r| (**r).clone());
            let store = Arc::new(RecordCheckpointer::new(
                Arc::new(InMemoryCheckpointStore::try_from_records(truncated).unwrap()),
                Arc::new(BranchSnapshotCodec),
            ));
            let saved = store.latest(&ThreadId::from("s")).await.unwrap().unwrap();
            let (a, pa) = support::stage("a", false, 2);
            let (b, pb) = support::stage("b", false, 2);
            let (c, pc) = support::stage("c", false, 2);
            let fresh = AgentBranch::new(
                "v1",
                a,
                b,
                c,
                Arc::new(|_: &group_agent_model::ValidatedJsonOutput| {
                    panic!("saved selector must not rerun")
                }),
            )
            .unwrap();
            let outcome = match operation {
                0 => fresh
                    .resume(ResumeConfig::new("s", store.clone()))
                    .await
                    .unwrap()
                    .as_completed()
                    .unwrap()
                    .clone(),
                1 => fresh
                    .replay(ReplayConfig::new("s", saved.id(), store.clone()))
                    .await
                    .unwrap()
                    .outcome()
                    .as_completed()
                    .unwrap()
                    .clone(),
                _ => fresh
                    .fork(ForkConfig::new("s", saved.id(), store.clone()))
                    .await
                    .unwrap()
                    .outcome()
                    .as_completed()
                    .unwrap()
                    .clone(),
            };
            assert_eq!(outcome.selected(), Some(target));
            assert_eq!(
                outcome.final_output().unwrap().value(),
                &serde_json::json!({"answer":"ok"})
            );
            assert_eq!(
                pa.completions.load(Ordering::SeqCst) + pa.executions.load(Ordering::SeqCst),
                0
            );
            for (probe, chosen) in [
                (pb, target == BranchTarget::B),
                (pc, target == BranchTarget::C),
            ] {
                assert_eq!(
                    probe.completions.load(Ordering::SeqCst),
                    2 * usize::from(chosen)
                );
                assert_eq!(probe.executions.load(Ordering::SeqCst), usize::from(chosen));
            }
            if operation == 1 {
                assert_eq!(
                    store
                        .latest(&ThreadId::from("s"))
                        .await
                        .unwrap()
                        .unwrap()
                        .id(),
                    saved.id()
                );
            }
        }
    }
}
#[tokio::test]
async fn saved_second_approval_resumes_without_first_stage_work() {
    for decision in [
        AgentApprovalDecision::Approve,
        AgentApprovalDecision::Reject,
    ] {
        let store = Arc::new(InMemoryCheckpointer::new(BranchSnapshotCodec));
        let (branch, probes) = support::branch([false, true], [2, 2]);
        let interrupted = branch
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
        let (fresh, probes) = support::branch([false, true], [2, 2]);
        let result = fresh
            .resume(
                ResumeConfig::new("s", store.clone())
                    .with_checkpoint_id(saved.checkpoint_id())
                    .with_resume_value(BranchApprovalDecision::new(
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
        let (fresh, probes) = support::branch([false, true], [2, 2]);
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
    let store = Arc::new(InMemoryCheckpointer::new(BranchSnapshotCodec));
    let (seq, _) = support::branch([true, false], [2, 2]);
    let result = seq
        .invoke_with_checkpoint(
            vec![Message::user("q")],
            CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
        )
        .await
        .unwrap();
    let saved = result.as_interrupted().unwrap();
    for mode in 0..4 {
        let (seq, probe) = support::branch([true, false], [2, 2]);
        let config = ResumeConfig::new("s", store.clone());
        let config = match mode {
            0 => config.with_resume_value(BranchApprovalDecision::new(
                AgentStageId::new("a").unwrap(),
                AgentApprovalDecision::Approve,
            )),
            1 => config
                .with_checkpoint_id(saved.checkpoint_id())
                .with_resume_value(BranchApprovalDecision::new(
                    AgentStageId::new("b").unwrap(),
                    AgentApprovalDecision::Approve,
                )),
            2 => config.with_checkpoint_id(saved.checkpoint_id()),
            _ => config
                .with_checkpoint_id(saved.checkpoint_id())
                .with_resume_value(SequenceApprovalDecision::new(
                    AgentStageId::new("a").unwrap(),
                    AgentApprovalDecision::Approve,
                )),
        };
        let error = seq.resume(config).await.unwrap_err();
        if mode == 0 {
            assert_eq!(error.kind(), BranchErrorKind::Configuration);
            assert!(error.source().is_none());
        } else {
            assert_eq!(error.kind(), BranchErrorKind::Graph);
            let core = error
                .source()
                .unwrap()
                .downcast_ref::<GraphRunError>()
                .unwrap();
            if mode == 2 {
                assert!(
                    matches!(core, GraphRunError::MissingResumeValue { node_id, .. } if node_id == &NodePath::from("first_tools"))
                );
            } else {
                assert!(
                    matches!(core, GraphRunError::NodeFailed { node_id, .. } if node_id == &NodePath::from("first_tools"))
                );
                assert_eq!(error.stage().unwrap().as_str(), "a");
                if mode == 3 {
                    let mut source = error.source();
                    let mut found = false;
                    while let Some(e) = source {
                        found |= matches!(
                            e.downcast_ref::<ResumeValueError>(),
                            Some(ResumeValueError::TypeMismatch { .. })
                        );
                        source = e.source();
                    }
                    assert!(found);
                }
            }
        }
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
        let (seq, probe) = support::branch([false, false], rounds);
        let result = seq.invoke(vec![Message::user("q")]).await.unwrap();
        assert_eq!(result.stop_reason(), AgentStopReason::MaxRounds);
        assert!(result.final_output().is_none());
        if rounds[0] == 1 {
            assert!(result.downstream().is_none());
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
        let store = Arc::new(InMemoryCheckpointer::new(BranchSnapshotCodec));
        let (seq, _) = support::branch([true, false], [2, 2]);
        let run = seq
            .invoke_with_checkpoint(
                vec![Message::user("q")],
                CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .await
            .unwrap();
        let saved = run.as_interrupted().unwrap();
        assert_eq!(saved.approval_request().unwrap().stage().as_str(), "a");
        let (seq, probes) = support::branch([true, false], [2, 2]);
        let result = seq
            .resume(
                ResumeConfig::new("s", store)
                    .with_checkpoint_id(saved.checkpoint_id())
                    .with_resume_value(BranchApprovalDecision::new(
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
        AgentBranch::new(
            "v1",
            a,
            b,
            support::stage("c", false, 2).0,
            Arc::new(
                |_: &group_agent_model::ValidatedJsonOutput| -> Result<BranchSelection, BranchSelectionError> {
                    unreachable!()
                }
            )
        )
        .is_err()
    );
}
