#![cfg(feature = "agent-sequence")]
#[path = "../test_support/sequence_agent.rs"]
mod support;
use group_agent_core::*;
use group_agent_model::Message;
use group_agent_prebuilt::*;
use serde_json::{Value, json};
use std::{
    error::Error,
    sync::{Arc, atomic::Ordering},
};
struct Corrupt(u8);
impl CheckpointCodec<SequenceSnapshot> for Corrupt {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        SequenceSnapshotCodec.snapshot_descriptor()
    }
    fn encode_snapshot(&self, s: &SequenceSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        SequenceSnapshotCodec.encode_snapshot(s)
    }
    fn decode_snapshot(&self, bytes: &[u8]) -> Result<SequenceSnapshot, CheckpointCodecError> {
        let mut v: Value = serde_json::from_slice(bytes).unwrap();
        match self.0 {
            0 => {
                v["first"]["model_rounds"] = json!(0);
                v["first"]["usage_by_round"] = json!([]);
            }
            1 => {
                v["first"]["messages"] =
                    serde_json::to_value(vec![Message::assistant(r#"{"answer":"ok"}"#)]).unwrap();
                v["first"]["stop_reason"] = json!("FinalAnswer");
                let mut second = v["first"].clone();
                second["messages"] = serde_json::to_value(vec![Message::user("mapped")]).unwrap();
                second["model_rounds"] = json!(0);
                second["usage_by_round"] = json!([]);
                second["stop_reason"] = Value::Null;
                v["second"] = second;
                v["phase"] = json!("Second");
            }
            2 => {
                v["first"]["messages"] =
                    serde_json::to_value(vec![Message::assistant("{}")]).unwrap();
            }
            3 => {
                v["first"]["model_rounds"] = json!(3);
                v["first"]["usage_by_round"] = json!([null, null, null]);
            }
            4 => {
                v["first"]["model_rounds"] = json!(2);
                v["first"]["usage_by_round"] = json!([null, null]);
            }
            _ => unreachable!(),
        }
        Ok(serde_json::from_value(v).unwrap())
    }
}
#[tokio::test]
async fn corrupt_phase_pending_calls_budget_and_output_fail_before_external_work() {
    let raw = Arc::new(InMemoryCheckpointStore::new());
    let store = Arc::new(RecordCheckpointer::new(
        raw.clone(),
        Arc::new(SequenceSnapshotCodec),
    ));
    let (seq, _) = support::sequence([false, false], [2, 2]);
    seq.invoke_with_checkpoint(
        vec![Message::user("q")],
        CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
    )
    .await
    .unwrap();
    let history = store.history(&ThreadId::from("s")).await.unwrap();
    // Tool frontier, model frontier, handoff, B model/tools, completed, exhausted model.
    for (step, mutation) in [
        (1, 0),
        (1, 1),
        (2, 1),
        (3, 2),
        (4, 2),
        (5, 2),
        (7, 2),
        (7, 3),
        (2, 4),
    ] {
        let source = history.iter().find(|s| s.superstep() == step).unwrap();
        let corrupt = Arc::new(RecordCheckpointer::new(
            raw.clone(),
            Arc::new(Corrupt(mutation)),
        ));
        let (seq, probes) = support::sequence([false, false], [2, 2]);
        let error = seq
            .replay(ReplayConfig::new("s", source.id(), corrupt.clone()))
            .await
            .unwrap_err();
        if step == 7 {
            assert_eq!(error.stage().unwrap().as_str(), "a");
        } else {
            assert!(error.source().unwrap().is::<GraphRunError>());
        }
        let fork = ForkConfig::new("s", source.id(), corrupt.clone());
        let branch = fork.branch_id();
        assert!(seq.fork(fork).await.is_err());
        if mutation != 0 {
            assert!(
                corrupt
                    .branch_head(&ThreadId::from("s"), branch)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(
            probes
                .iter()
                .map(|p| p.completions.load(Ordering::SeqCst)
                    + p.streams.load(Ordering::SeqCst)
                    + p.executions.load(Ordering::SeqCst))
                .sum::<usize>(),
            0
        );
    }
}
