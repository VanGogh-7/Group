#![cfg(feature = "agent-branch")]
#[path = "../test_support/branch_agent.rs"]
mod support;
use group_agent_core::*;
use group_agent_model::Message;
use group_agent_prebuilt::*;
use std::sync::{Arc, atomic::Ordering::SeqCst};

fn replace(
    record: &CheckpointRecord,
    frontier: &[&str],
    mutation: Option<(&str, serde_json::Value)>,
) -> CheckpointRecord {
    let mut value: serde_json::Value = serde_json::from_slice(record.snapshot().bytes()).unwrap();
    if let Some((field, replacement)) = mutation {
        value[field] = replacement;
    }
    CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: record.format_version(),
        checkpoint_id: record.id(),
        thread_id: record.thread_id().clone(),
        run_id: record.run_id(),
        parent_id: record.parent_id(),
        graph_version: record.graph_version().cloned(),
        superstep: record.superstep(),
        step: record.step(),
        snapshot: EncodedValue::new(
            record.snapshot().descriptor().clone(),
            serde_json::to_vec(&value).unwrap(),
        ),
        next_frontier: frontier.iter().map(|s| NodePath::from(*s)).collect(),
        completed: frontier.is_empty(),
        interrupt: None,
    })
    .unwrap()
}
#[tokio::test]
async fn single_invalid_frontier_and_selection_fail_but_mixed_frontier_is_not_atomic_preflight() {
    let raw = Arc::new(InMemoryCheckpointStore::new());
    let store = Arc::new(RecordCheckpointer::new(
        raw.clone(),
        Arc::new(BranchSnapshotCodec),
    ));
    let (branch, _) = support::branch([false; 2], [2; 2]);
    branch
        .invoke_with_checkpoint(
            vec![Message::user("q")],
            CheckpointConfig::new("s", store, CheckpointPolicy::EverySuperstep),
        )
        .await
        .unwrap();
    let records = raw.history(&ThreadId::from("s")).await.unwrap();
    let cases = [
        (vec!["c_model"], None),
        (vec!["first_model"], None),
        (vec!["select"], None),
        (vec![], None),
        (vec!["b_model"], Some(("selection", serde_json::json!("C")))),
        (
            vec!["b_model"],
            Some(("selection", serde_json::json!("Complete"))),
        ),
        (
            vec!["b_model"],
            Some(("selection", serde_json::Value::Null)),
        ),
        (
            vec!["b_model"],
            Some(("downstream", serde_json::Value::Null)),
        ),
        (vec!["b_model", "c_model"], None),
    ];
    for (frontier, mutation) in cases {
        let mixed = frontier.len() == 2;
        for operation in 0..3 {
            let modified = records.iter().filter(|r| r.superstep() <= 4).map(|r| {
                if r.superstep() == 4 {
                    replace(r, &frontier, mutation.clone())
                } else {
                    (**r).clone()
                }
            });
            let raw = Arc::new(InMemoryCheckpointStore::try_from_records(modified).unwrap());
            let store = Arc::new(RecordCheckpointer::new(
                raw.clone(),
                Arc::new(BranchSnapshotCodec),
            ));
            let source = raw.latest(&ThreadId::from("s")).await.unwrap().unwrap();
            let (fresh, probes) = support::branch([false; 2], [2; 2]);
            let failed = match operation {
                0 => fresh
                    .resume(ResumeConfig::new("s", store.clone()))
                    .await
                    .is_err(),
                1 => fresh
                    .replay(ReplayConfig::new("s", source.id(), store.clone()))
                    .await
                    .is_err(),
                _ => fresh
                    .fork(ForkConfig::new("s", source.id(), store.clone()))
                    .await
                    .is_err(),
            };
            assert!(failed);
            assert_eq!(probes[0].completions.load(SeqCst), 0);
            assert_eq!(probes[2].completions.load(SeqCst), 0);
            assert_eq!(
                probes
                    .iter()
                    .map(|p| p.executions.load(SeqCst))
                    .sum::<usize>(),
                0
            );
            // The valid B node is polled before the invalid C guard. Its update is discarded,
            // but the external model invocation has already occurred.
            assert_eq!(probes[1].completions.load(SeqCst), usize::from(mixed));
            assert_eq!(
                raw.latest(&ThreadId::from("s"))
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                source.id()
            );
        }
    }
}
