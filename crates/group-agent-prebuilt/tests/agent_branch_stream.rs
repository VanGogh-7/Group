#![cfg(feature = "agent-branch")]
#[path = "../test_support/branch_agent.rs"]
mod support;
use futures_util::StreamExt;
use group_agent_core::*;
use group_agent_model::Message;
use group_agent_prebuilt::*;
use std::sync::{Arc, Mutex, atomic::Ordering};
#[tokio::test]
async fn selected_c_stream_error_is_terminal_and_has_no_completed_event() {
    let (branch, probes) = support::choose(BranchTarget::C, [false; 3], [2; 3]);
    probes[2].fail_final_once.store(true, Ordering::SeqCst);
    let mut stream = branch.stream(vec![Message::user("q")]);
    let events = stream.by_ref().collect::<Vec<_>>().await;
    assert_eq!(events.iter().filter(|e| e.is_err()).count(), 1);
    assert!(events.last().unwrap().is_err());
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Ok(BranchStreamEvent::Completed(_))))
    );
    assert_eq!(
        events
            .last()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .stage()
            .unwrap()
            .as_str(),
        "c"
    );
    assert_eq!(
        probes[1].streams.load(Ordering::SeqCst) + probes[1].executions.load(Ordering::SeqCst),
        0
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn direct_completion_stream_and_sink_match_and_emit_one_parent_terminal() {
    let (branch, probes) = support::choose(BranchTarget::Complete, [false; 3], [2; 3]);
    let events = branch
        .stream(vec![Message::user("q")])
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(BranchStreamEvent::Completed(_))))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e,
                Ok(BranchStreamEvent::Selection {
                    target: BranchTarget::Complete,
                    ..
                })
            ))
            .count(),
        1
    );
    let sink_events = Arc::new(Mutex::new(Vec::new()));
    let capture = sink_events.clone();
    let result = branch
        .invoke_with_stream_sink(
            vec![Message::user("q")],
            Arc::new(move |e: &BranchStreamEvent| capture.lock().unwrap().push(e.clone())),
        )
        .await
        .unwrap();
    let Some(Ok(BranchStreamEvent::Completed(streamed))) = events.last() else {
        panic!("missing completion")
    };
    assert_eq!(&result, streamed);
    assert_eq!(
        sink_events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, BranchStreamEvent::Completed(_)))
            .count(),
        1
    );
    for probe in &probes[1..] {
        assert_eq!(
            probe.streams.load(Ordering::SeqCst) + probe.executions.load(Ordering::SeqCst),
            0
        );
    }
}
#[tokio::test]
async fn stream_has_stage_attribution_one_handoff_and_only_one_parent_completion() {
    let (seq, probes) = support::branch([false, false], [2, 2]);
    let events = seq
        .stream(vec![Message::user("SECRET")])
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(BranchStreamEvent::Completed(_))))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(BranchStreamEvent::Selection { .. })))
            .count(),
        1
    );
    for id in ["a", "b"] {
        assert!(events.iter().any(|e|matches!(e,Ok(BranchStreamEvent::Stage{stage,event:AgentStreamEvent::ToolStarted{..}}) if stage.as_str()==id)));
    }
    for e in &events {
        assert!(!format!("{e:?}").contains("SECRET"));
    }
    assert!(matches!(
        events.last(),
        Some(Ok(BranchStreamEvent::Completed(_)))
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
        let store = Arc::new(InMemoryCheckpointer::new(BranchSnapshotCodec));
        let (seq, _) = support::branch([false, true], [2, 2]);
        let events = seq
            .stream_with_checkpoint(
                vec![Message::user("q")],
                CheckpointConfig::new("s", store.clone(), CheckpointPolicy::EverySuperstep),
            )
            .collect::<Vec<_>>()
            .await;
        let Some(Ok(BranchStreamEvent::Interrupted(saved))) = events.last() else {
            panic!("missing persisted interrupt")
        };
        let (seq, probes) = support::branch([false, true], [2, 2]);
        let config = ResumeConfig::new("s", store.clone())
            .with_checkpoint_id(saved.checkpoint_id())
            .with_resume_value(BranchApprovalDecision::new(
                AgentStageId::new("b").unwrap(),
                AgentApprovalDecision::Approve,
            ));
        let events = if sink_mode {
            let events = Arc::new(Mutex::new(vec![]));
            let copy = events.clone();
            let sink = Arc::new(move |e: &BranchStreamEvent| copy.lock().unwrap().push(e.clone()));
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
                |e| !matches!(e,Ok(BranchStreamEvent::Stage{stage,..}) if stage.as_str()=="a")
            )
        );
        assert_eq!(
            probes[0].streams.load(Ordering::SeqCst) + probes[0].executions.load(Ordering::SeqCst),
            0
        );
        assert!(matches!(
            events.last(),
            Some(Ok(BranchStreamEvent::Completed(_)))
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
    let (seq, probes) = support::branch([false, false], [2, 2]);
    let one = Arc::new(Mutex::new(Vec::new()));
    let two = Arc::new(Mutex::new(Vec::new()));
    let a = one.clone();
    let b = two.clone();
    let sink_a = Arc::new(move |e: &BranchStreamEvent| a.lock().unwrap().push(e.clone()));
    let sink_b = Arc::new(move |e: &BranchStreamEvent| b.lock().unwrap().push(e.clone()));
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
                .filter(|e| matches!(e, BranchStreamEvent::Completed(_)))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, BranchStreamEvent::Selection { .. }))
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
