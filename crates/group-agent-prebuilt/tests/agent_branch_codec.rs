#![cfg(feature = "agent-branch")]
use group_agent_core::{CheckpointCodec, CodecDescriptor, EncodedValue, InterruptPayload};
use group_agent_model::{Message, ToolCall, ToolCallId, ToolName};
use group_agent_prebuilt::{
    AgentApprovalRequest, AgentSnapshotCodec, BranchApprovalRequest, BranchSnapshot,
    BranchSnapshotCodec,
};
use serde_json::json;
#[test]
fn branch_formats_are_independent_and_have_stable_initial_encoding() {
    let messages = serde_json::to_string(&vec![Message::user("q")]).unwrap();
    let golden = format!(
        r#"{{"phase":"First","first":{{"messages":{messages},"model_rounds":0,"usage_by_round":[],"stop_reason":null}},"selection":null,"downstream":null}}"#
    );
    let snapshot: BranchSnapshot = serde_json::from_str(&golden).unwrap();
    assert_eq!(
        BranchSnapshotCodec.snapshot_descriptor(),
        CodecDescriptor::new("group-agent-prebuilt-agent-branch-state", 1, "json")
    );
    assert_eq!(
        BranchSnapshotCodec.encode_snapshot(&snapshot).unwrap(),
        golden.as_bytes()
    );
    let decoded = BranchSnapshotCodec
        .decode_snapshot(golden.as_bytes())
        .unwrap();
    assert_eq!(
        BranchSnapshotCodec.encode_snapshot(&decoded).unwrap(),
        golden.as_bytes()
    );
    assert!(
        AgentSnapshotCodec
            .decode_snapshot(golden.as_bytes())
            .is_err()
    );
    let value: serde_json::Value = serde_json::from_str(&golden).unwrap();
    assert!(
        BranchSnapshotCodec
            .decode_snapshot(&serde_json::to_vec(&value["first"]).unwrap())
            .is_err()
    );
    let call = ToolCall::new(
        ToolCallId::new("c").unwrap(),
        ToolName::new("lookup").unwrap(),
        json!({}),
    );
    let request: BranchApprovalRequest =
        serde_json::from_value(json!({"stage":"a","request":{"pending_calls":[call]}})).unwrap();
    let encoded = BranchSnapshotCodec
        .encode_interrupt(&InterruptPayload::new(request.clone()))
        .unwrap();
    assert_eq!(
        encoded.descriptor(),
        &CodecDescriptor::new("group-agent-prebuilt-agent-branch-approval", 1, "json")
    );
    assert_eq!(
        BranchSnapshotCodec
            .decode_interrupt(&encoded)
            .unwrap()
            .downcast_ref::<BranchApprovalRequest>(),
        Some(&request)
    );
    assert!(AgentSnapshotCodec.decode_interrupt(&encoded).is_err());
    let plain: AgentApprovalRequest =
        serde_json::from_value(json!({"pending_calls":request.request().pending_calls()})).unwrap();
    assert!(
        BranchSnapshotCodec
            .encode_interrupt(&InterruptPayload::new(plain))
            .is_err()
    );
    let wrong = EncodedValue::new(
        CodecDescriptor::new("group-agent-prebuilt-agent-branch-approval", 2, "json"),
        encoded.bytes().to_vec(),
    );
    assert!(BranchSnapshotCodec.decode_interrupt(&wrong).is_err());
}

#[test]
fn old_sequence_snapshot_bytes_are_not_accepted_as_branch_state() {
    let messages = serde_json::to_value(vec![Message::user("q")]).unwrap();
    let old = serde_json::to_vec(&json!({"phase":"First", "first":{"messages":messages,"model_rounds":0,"usage_by_round":[],"stop_reason":null},"second":null})).unwrap();
    assert!(
        group_agent_prebuilt::SequenceSnapshotCodec
            .decode_snapshot(&old)
            .is_ok()
    );
    assert!(BranchSnapshotCodec.decode_snapshot(&old).is_err());
}

#[test]
fn malformed_codec_errors_hide_payload_but_preserve_json_source() {
    use std::error::Error;
    for error in [
        BranchSnapshotCodec
            .decode_snapshot(br#"{"phase":"SECRET_PHASE"}"#)
            .unwrap_err(),
        BranchSnapshotCodec
            .decode_interrupt(&EncodedValue::new(
                CodecDescriptor::new("group-agent-prebuilt-agent-branch-approval", 1, "json"),
                br#"{"stage":"a","request":"SECRET_REQUEST"}"#.to_vec(),
            ))
            .unwrap_err(),
    ] {
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        let mut source = error.source();
        let mut found = false;
        while let Some(e) = source {
            found |= e.is::<serde_json::Error>();
            source = e.source();
        }
        assert!(found);
    }
}
