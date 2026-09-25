#![cfg(feature = "agent-sequence")]
#[path = "../test_support/sequence_agent.rs"]
mod support;
use futures_util::StreamExt;
use group_agent_core::*;
use group_agent_model::Message;
use group_agent_prebuilt::*;
use std::sync::{Arc, Mutex, atomic::Ordering};
#[tokio::test]
async fn stream_has_stage_attribution_one_handoff_and_only_one_parent_completion() {
    let (seq, probes) = support::sequence([false, false], [2, 2]);
    let events = seq
        .stream(vec![Message::user("SECRET")])
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(SequenceStreamEvent::Completed(_))))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(SequenceStreamEvent::Handoff { .. })))
            .count(),
        1
    );
    for id in ["a", "b"] {
        assert!(events.iter().any(|e|matches!(e,Ok(SequenceStreamEvent::Stage{stage,event:AgentStreamEvent::ToolStarted{..}}) if stage.as_str()==id)));
    }
    for e in &events {
        assert!(!format!("{e:?}").contains("SECRET"));
    }
    assert!(matches!(
        events.last(),
        Some(Ok(SequenceStreamEvent::Completed(_)))
    ));
    assert_eq!(
        probes
            .iter()
            .map(|p| p.streams.load(Ordering::SeqCst))
            .sum::<usize>(),
        4
    );
}
#[tokio::test]
async fn resume_stream_and_sink_restore_only_active_stage_and_terminal() {
    for sink_mode in [false, true] {
        let store = Arc::new(InMemoryCheckpointer::new(SequenceSnapshotCodec));
        let (seq, _) = support::sequence([false, true], [2, 2]);
        let events = seq
            .stream_with_checkpoint(
                vec![Message::user("q")],
                CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .collect::<Vec<_>>()
            .await;
        let Some(Ok(SequenceStreamEvent::Interrupted(saved))) = events.last() else {
            panic!("missing persisted interrupt")
        };
        let (seq, probes) = support::sequence([false, true], [2, 2]);
        let config = ResumeConfig::new("s", store.clone())
            .with_checkpoint_id(saved.checkpoint_id())
            .with_resume_value(SequenceApprovalDecision::new(
                AgentStageId::new("b").unwrap(),
                AgentApprovalDecision::Approve,
            ));
        let events = if sink_mode {
            let events = Arc::new(Mutex::new(vec![]));
            let copy = events.clone();
            let sink =
                Arc::new(move |e: &SequenceStreamEvent| copy.lock().unwrap().push(e.clone()));
            seq.resume_with_stream_sink(config, sink).await.unwrap();
            events
                .lock()
                .unwrap()
                .clone()
                .into_iter()
                .map(Ok)
                .collect::<Vec<_>>()
        } else {
            seq.resume_stream(config).collect::<Vec<_>>().await
        };
        assert!(
            events.iter().all(
                |e| !matches!(e,Ok(SequenceStreamEvent::Stage{stage,..}) if stage.as_str()=="a")
            )
        );
        assert_eq!(
            probes[0].streams.load(Ordering::SeqCst) + probes[0].executions.load(Ordering::SeqCst),
            0
        );
        assert!(matches!(
            events.last(),
            Some(Ok(SequenceStreamEvent::Completed(_)))
        ));
        let events = seq
            .resume_stream(ResumeConfig::new("s", store))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events.len(), 1);
    }
}

#[tokio::test]
async fn concurrent_invocations_keep_sinks_and_transcripts_separate() {
    let (seq, probes) = support::sequence([false, false], [2, 2]);
    let one = Arc::new(Mutex::new(Vec::new()));
    let two = Arc::new(Mutex::new(Vec::new()));
    let a = one.clone();
    let b = two.clone();
    let sink_a = Arc::new(move |e: &SequenceStreamEvent| a.lock().unwrap().push(e.clone()));
    let sink_b = Arc::new(move |e: &SequenceStreamEvent| b.lock().unwrap().push(e.clone()));
    let (a, b) = tokio::join!(
        seq.invoke_with_stream_sink(vec![Message::user("first-private")], sink_a),
        seq.invoke_with_stream_sink(vec![Message::user("second-private")], sink_b)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.first().messages()[0], Message::user("first-private"));
    assert_eq!(b.first().messages()[0], Message::user("second-private"));
    for events in [one, two] {
        let events = events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, SequenceStreamEvent::Completed(_)))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, SequenceStreamEvent::Handoff { .. }))
                .count(),
            1
        );
    }
    assert_eq!(
        probes
            .iter()
            .map(|p| p.executions.load(Ordering::SeqCst))
            .sum::<usize>(),
        4
    );
}
