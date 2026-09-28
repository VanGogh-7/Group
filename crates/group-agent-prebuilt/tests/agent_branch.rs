#![cfg(feature = "agent-branch")]
#[path = "../test_support/sequence_agent.rs"]
mod support;
use group_agent_model::{Message, ValidatedJsonOutput};
use group_agent_prebuilt::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

#[tokio::test]
async fn fixed_choices_execute_only_the_selected_stage() {
    for target in [BranchTarget::B, BranchTarget::C, BranchTarget::Complete] {
        let (a, pa) = support::stage("a", false, 2);
        let (b, pb) = support::stage("b", false, 2);
        let (c, pc) = support::stage("c", false, 2);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let branch = AgentBranch::new(
            "v1",
            a,
            b,
            c,
            Arc::new(move |output: &ValidatedJsonOutput| {
                assert_eq!(
                    output.deserialize::<serde_json::Value>().unwrap()["answer"],
                    "ok"
                );
                count.fetch_add(1, SeqCst);
                Ok(match target {
                    BranchTarget::B => BranchSelection::B(vec![Message::user("only-b")]),
                    BranchTarget::C => BranchSelection::C(vec![Message::user("only-c")]),
                    BranchTarget::Complete => BranchSelection::Complete,
                })
            }),
        )
        .unwrap();
        let result = branch.invoke(vec![Message::user("only-a")]).await.unwrap();
        assert_eq!(result.selected(), Some(target));
        assert_eq!(result.stop_reason(), AgentStopReason::FinalAnswer);
        assert!(result.final_output().is_some());
        assert_eq!(calls.load(SeqCst), 1);
        assert_eq!(pa.executions.load(SeqCst), 1);
        for (probe, selected) in [
            (pb, target == BranchTarget::B),
            (pc, target == BranchTarget::C),
        ] {
            assert_eq!(probe.executions.load(SeqCst), usize::from(selected));
            assert_eq!(probe.completions.load(SeqCst), 2 * usize::from(selected));
        }
    }
}

#[tokio::test]
async fn first_exhaustion_skips_selector_and_c_exhaustion_has_no_final_output() {
    for first_limit in [1, 2] {
        let (a, _) = support::stage("a", false, first_limit);
        let (b, pb) = support::stage("b", false, 2);
        let (c, pc) = support::stage("c", false, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let selected = calls.clone();
        let branch = AgentBranch::new(
            "v1",
            a,
            b,
            c,
            Arc::new(move |_: &ValidatedJsonOutput| {
                selected.fetch_add(1, SeqCst);
                Ok(BranchSelection::C(vec![Message::user("mapped")]))
            }),
        )
        .unwrap();
        let result = branch.invoke(vec![Message::user("q")]).await.unwrap();
        assert_eq!(result.stop_reason(), AgentStopReason::MaxRounds);
        assert!(result.final_output().is_none());
        assert!(result.final_message().is_none());
        assert_eq!(calls.load(SeqCst), usize::from(first_limit == 2));
        assert_eq!(
            result.selected(),
            if first_limit == 1 {
                None
            } else {
                Some(BranchTarget::C)
            }
        );
        assert_eq!(pb.completions.load(SeqCst) + pb.executions.load(SeqCst), 0);
        assert_eq!(pc.completions.load(SeqCst), usize::from(first_limit == 2));
    }
}
