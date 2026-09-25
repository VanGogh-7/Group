#![cfg(feature = "agent-sequence")]
#[path = "../test_support/sequence_agent.rs"]
mod support;
use async_trait::async_trait;
use futures_util::StreamExt;
use group_agent_core::*;
use group_agent_model::Message;
use group_agent_prebuilt::*;
use std::{
    error::Error,
    fmt,
    sync::{Arc, atomic::Ordering},
};
#[derive(Debug)]
struct SaveFailure;
impl fmt::Display for SaveFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fixture save failure")
    }
}
impl Error for SaveFailure {}

struct FailingStore {
    inner: Arc<InMemoryCheckpointer<SequenceSnapshot>>,
    fail_step: usize,
}
#[async_trait]
impl Checkpointer<SequenceSnapshot> for FailingStore {
    async fn save(
        &self,
        request: CheckpointRequest<SequenceSnapshot>,
    ) -> Result<Arc<Checkpoint<SequenceSnapshot>>, CheckpointWriteError> {
        if (self.fail_step == 0 && request.interrupt().is_some())
            || request.superstep() == self.fail_step
        {
            return Err(CheckpointerError::with_source("save failed", SaveFailure).into());
        }
        self.inner.save(request).await
    }
    async fn latest(
        &self,
        thread: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<SequenceSnapshot>>>, CheckpointerError> {
        self.inner.latest(thread).await
    }
    async fn get(
        &self,
        thread: &ThreadId,
        id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<SequenceSnapshot>>>, CheckpointerError> {
        self.inner.get(thread, id).await
    }
    async fn history(
        &self,
        thread: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<SequenceSnapshot>>>, CheckpointerError> {
        self.inner.history(thread).await
    }
}
fn has_source<T: Error + 'static>(error: &(dyn Error + 'static)) -> bool {
    let mut current = error.source();
    while let Some(error) = current {
        if error.is::<T>() {
            return true;
        }
        current = error.source();
    }
    false
}

#[tokio::test]
async fn failed_first_final_handoff_or_approval_save_preserves_previous_head() {
    for fail_step in [3, 4, 0] {
        for streaming in [false, true] {
            let (seq, probes) = support::sequence([false, true], [2, 2]);
            let store = Arc::new(FailingStore {
                inner: Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec)),
                fail_step,
            });
            let config =
                CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep);
            let error = if streaming {
                let events = seq
                    .stream_with_checkpoint(vec![Message::user("q")], config)
                    .collect::<Vec<_>>()
                    .await;
                assert!(!events.iter().any(|e| matches!(
                    e,
                    Ok(SequenceStreamEvent::Completed(_) | SequenceStreamEvent::Interrupted(_))
                )));
                assert_eq!(events.iter().filter(|e| e.is_err()).count(), 1);
                events.into_iter().find_map(Result::err).unwrap()
            } else {
                seq.invoke_with_checkpoint(vec![Message::user("q")], config)
                    .await
                    .unwrap_err()
            };
            assert!(has_source::<SaveFailure>(&error));
            assert!(error.source().unwrap().is::<GraphRunError>());
            let head = store.latest(&ThreadId::from("s")).await.unwrap().unwrap();
            assert_eq!(
                head.superstep(),
                if fail_step == 0 { 5 } else { fail_step - 1 }
            );
            assert!(!head.completed());
            assert!(head.interrupt().is_none());
            assert_eq!(probes[1].executions.load(Ordering::SeqCst), 0);
            if fail_step != 0 {
                assert_eq!(
                    probes[1].completions.load(Ordering::SeqCst)
                        + probes[1].streams.load(Ordering::SeqCst),
                    0
                );
            }
        }
    }
}
#[tokio::test]
async fn final_only_start_fails_before_any_work() {
    let (seq, probes) = support::sequence([false, false], [2, 2]);
    let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
    assert!(
        seq.invoke_with_checkpoint(
            vec![Message::user("q")],
            CheckpointConfig::new("s", store.clone(), CheckpointPolicy::FinalOnly)
        )
        .await
        .is_err()
    );
    assert!(store.latest(&ThreadId::from("s")).await.unwrap().is_none());
    assert_eq!(
        probes
            .iter()
            .map(|p| p.completions.load(Ordering::SeqCst))
            .sum::<usize>(),
        0
    );
}
