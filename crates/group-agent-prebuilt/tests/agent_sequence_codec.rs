#![cfg(feature = "agent-sequence")]
use group_agent_core::{CheckpointCodec, CodecDescriptor, EncodedValue, InterruptPayload};
use group_agent_model::{Message, ToolCall, ToolCallId, ToolName};
use group_agent_prebuilt::{
    AgentApprovalRequest, AgentSnapshotCodec, SequenceApprovalRequest, SequenceSnapshot,
    SequenceSnapshotCodec,
};
use serde_json::json;
#[test]
fn sequence_formats_are_independent_and_have_stable_initial_encoding() {
    let messages = serde_json::to_string(&vec![Message::user("q")]).unwrap();
    let golden = format!(
        r#"{{"phase":"First","first":{{"messages":{messages},"model_rounds":0,"usage_by_round":[],"stop_reason":null}},"second":null}}"#
    );
    let snapshot: SequenceSnapshot = serde_json::from_str(&golden).unwrap();
    assert_eq!(
        SequenceSnapshotCodec.snapshot_descriptor(),
        CodecDescriptor::new("group-agent-prebuilt-agent-sequence-state", 1, "json")
    );
    assert_eq!(
        SequenceSnapshotCodec.encode_snapshot(&snapshot).unwrap(),
        golden.as_bytes()
    );
    let decoded = SequenceSnapshotCodec
        .decode_snapshot(golden.as_bytes())
        .unwrap();
    assert_eq!(
        SequenceSnapshotCodec.encode_snapshot(&decoded).unwrap(),
        golden.as_bytes()
    );
    assert!(
        AgentSnapshotCodec
            .decode_snapshot(golden.as_bytes())
            .is_err()
    );
    let value: serde_json::Value = serde_json::from_str(&golden).unwrap();
    assert!(
        SequenceSnapshotCodec
            .decode_snapshot(&serde_json::to_vec(&value["first"]).unwrap())
            .is_err()
    );
    let call = ToolCall::new(
        ToolCallId::new("c").unwrap(),
        ToolName::new("lookup").unwrap(),
        json!({}),
    );
    let request: SequenceApprovalRequest =
        serde_json::from_value(json!({"stage":"a","request":{"pending_calls":[call]}})).unwrap();
    let encoded = SequenceSnapshotCodec
        .encode_interrupt(&InterruptPayload::new(request.clone()))
        .unwrap();
    assert_eq!(
        encoded.descriptor(),
        &CodecDescriptor::new("group-agent-prebuilt-agent-sequence-approval", 1, "json")
    );
    assert_eq!(
        SequenceSnapshotCodec
            .decode_interrupt(&encoded)
            .unwrap()
            .downcast_ref::<SequenceApprovalRequest>(),
        Some(&request)
    );
    assert!(AgentSnapshotCodec.decode_interrupt(&encoded).is_err());
    let plain: AgentApprovalRequest =
        serde_json::from_value(json!({"pending_calls":request.request().pending_calls()})).unwrap();
    assert!(
        SequenceSnapshotCodec
            .encode_interrupt(&InterruptPayload::new(plain))
            .is_err()
    );
    let wrong = EncodedValue::new(
        CodecDescriptor::new("group-agent-prebuilt-agent-sequence-approval", 2, "json"),
        encoded.bytes().to_vec(),
    );
    assert!(SequenceSnapshotCodec.decode_interrupt(&wrong).is_err());
}
