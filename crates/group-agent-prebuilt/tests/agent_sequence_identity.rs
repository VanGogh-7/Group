#![cfg(feature = "agent-sequence")]
#[path = "../test_support/sequence_agent.rs"]
mod support;
use group_agent_core::*;
use group_agent_model::{Message, ValidatedJsonOutput};
use group_agent_prebuilt::*;
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn identity_is_golden_and_configuration_mismatch_prevents_calls_and_branch_creation() {
    let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
    let (seq, _) = support::sequence([false, false], [2, 2]);
    seq.invoke_with_checkpoint(
        vec![Message::user("q")],
        CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
    )
    .await
    .unwrap();
    let head = store.latest(&ThreadId::from("s")).await.unwrap().unwrap();
    assert_eq!(
        head.graph_version().unwrap().as_str(),
        "group-agent-prebuilt/agent-sequence/1/4aa4498da99511197b3e1540f5a9cdbb5241a1cc57e44f191633212300aaca22"
    );
    for mode in 0..7 {
        let (a, pa) = support::stage(
            if mode == 1 { "renamed" } else { "a" },
            mode == 3,
            if mode == 2 { 3 } else { 2 },
        );
        let mut schema = support::output().schema().clone();
        if mode == 6 {
            schema["description"] = serde_json::json!("changed");
        }
        let contract = group_agent_model::StructuredOutput::new(
            if mode == 5 { "other" } else { "answer" },
            schema,
        )
        .unwrap();
        let (b, pb) = support::stage_with_output("b", false, 2, contract);
        let (a, b) = if mode == 4 { (b, a) } else { (a, b) };
        let seq = AgentSequence::new(
            if mode == 0 { "v2" } else { "v1" },
            a,
            b,
            Arc::new(
                |_: &ValidatedJsonOutput| -> Result<Vec<Message>, HandoffError> {
                    panic!("mapper must not run")
                },
            ),
        )
        .unwrap();
        assert!(
            seq.resume(ResumeConfig::new("s", store.clone()))
                .await
                .is_err()
        );
        assert!(
            seq.replay(ReplayConfig::new("s", head.id(), store.clone()))
                .await
                .is_err()
        );
        let fork = ForkConfig::new("s", head.id(), store.clone());
        let branch = fork.branch_id();
        assert!(seq.fork(fork).await.is_err());
        assert!(
            store
                .branch_head(&ThreadId::from("s"), branch)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            pa.completions.load(Ordering::SeqCst)
                + pb.completions.load(Ordering::SeqCst)
                + pa.executions.load(Ordering::SeqCst)
                + pb.executions.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            store
                .latest(&ThreadId::from("s"))
                .await
                .unwrap()
                .unwrap()
                .id(),
            head.id()
        );
    }
}
#[tokio::test]
async fn every_superstep_is_enforced_on_resume_and_fork_and_replay_is_lineage_only() {
    let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
    let (seq, _) = support::sequence([false, true], [2, 2]);
    let interrupted = seq
        .invoke_with_checkpoint(
            vec![Message::user("q")],
            CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
        )
        .await
        .unwrap();
    let saved = interrupted.as_interrupted().unwrap();
    let resume = ResumeConfig::new("s", store.clone())
        .with_checkpoint_id(saved.checkpoint_id())
        .with_checkpoint_policy(CheckpointPolicy::FinalOnly)
        .with_resume_value(SequenceApprovalDecision::new(
            AgentStageId::new("b").unwrap(),
            AgentApprovalDecision::Approve,
        ));
    let before = store.history(&ThreadId::from("s")).await.unwrap().len();
    seq.resume(resume).await.unwrap();
    assert_eq!(
        store.history(&ThreadId::from("s")).await.unwrap().len(),
        before + 2
    );
    assert!(
        seq.resume(
            ResumeConfig::new("s", store.clone())
                .with_checkpoint_id(saved.checkpoint_id())
                .with_resume_value(SequenceApprovalDecision::new(
                    AgentStageId::new("b").unwrap(),
                    AgentApprovalDecision::Approve
                ))
        )
        .await
        .is_err()
    );
    let fork = ForkConfig::new("s", saved.checkpoint_id(), store.clone())
        .with_checkpoint_policy(CheckpointPolicy::FinalOnly)
        .with_resume_value(SequenceApprovalDecision::new(
            AgentStageId::new("b").unwrap(),
            AgentApprovalDecision::Reject,
        ));
    let branch = fork.branch_id();
    seq.fork(fork).await.unwrap();
    assert_eq!(
        store
            .branch_history(&ThreadId::from("s"), branch)
            .await
            .unwrap()
            .len(),
        3
    );
    // A has no approval: exact Replay from its pending Tools repeats that Tool.
    let history = store.history(&ThreadId::from("s")).await.unwrap();
    let first = history.iter().find(|c| c.superstep() == 1).unwrap();
    let (seq, probes) = support::sequence([false, true], [2, 2]);
    // Replay reaches B's interrupt and fails: lineage remains untouched, A effect occurred.
    assert!(
        seq.replay(ReplayConfig::new("s", first.id(), store.clone()))
            .await
            .is_err()
    );
    assert_eq!(probes[0].executions.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.history(&ThreadId::from("s")).await.unwrap().len(),
        history.len()
    );
}
