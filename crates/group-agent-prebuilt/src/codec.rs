use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CodecDescriptor, EncodedValue, InterruptPayload,
};

use crate::{AgentApprovalRequest, AgentSnapshot};

/// Canonical JSON codec for durable Agent snapshots.
///
/// The codec serializes the opaque [`AgentSnapshot`] through the Model crate's
/// feature-gated serde surface. Its descriptor is a durable compatibility
/// identity: the schema, schema version, and encoding triple is recorded in
/// every stored checkpoint, and changing any component requires a deliberate
/// migration of stored checkpoints.
///
/// Interrupt payloads are supported for the experimental
/// [`AgentApprovalRequest`] type only; every other payload type keeps failing
/// closed with `UnsupportedInterruptPayload`. The approval interrupt
/// descriptor (`group-agent-prebuilt-tool-approval`, 1, `json`) is a durable
/// compatibility identity like the snapshot descriptor
/// (`group-agent-prebuilt-agent-state`, 1, `json`), and both share the `json`
/// encoding identity the Runtime enforces across one codec's descriptors.
///
/// Decoding enforces the Model types' invariants — validated identifiers,
/// extension keys, and token-usage consistency — in addition to JSON
/// structure, so bytes that would construct a value the public constructors
/// reject fail with a typed [`CheckpointCodecError`] instead.
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentSnapshotCodec;

fn approval_interrupt_descriptor() -> CodecDescriptor {
    CodecDescriptor::new("group-agent-prebuilt-tool-approval", 1, "json")
}

impl CheckpointCodec<AgentSnapshot> for AgentSnapshotCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group-agent-prebuilt-agent-state", 1, "json")
    }

    fn encode_snapshot(&self, snapshot: &AgentSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        serde_json::to_vec(snapshot).map_err(|source| {
            CheckpointCodecError::with_source("agent snapshot encoding failed", source)
        })
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<AgentSnapshot, CheckpointCodecError> {
        serde_json::from_slice(bytes).map_err(|source| {
            CheckpointCodecError::with_source("agent snapshot decoding failed", source)
        })
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let Some(request) = payload.downcast_ref::<AgentApprovalRequest>() else {
            return Err(CheckpointCodecError::unsupported_interrupt(payload));
        };
        let bytes = serde_json::to_vec(request).map_err(|source| {
            CheckpointCodecError::with_source("agent approval payload encoding failed", source)
        })?;
        Ok(EncodedValue::new(approval_interrupt_descriptor(), bytes))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        let expected = approval_interrupt_descriptor();
        let actual = value.descriptor();
        if actual != &expected {
            return Err(CheckpointCodecError::message(format!(
                "interrupt descriptor `{actual}` does not match the supported `{expected}`"
            )));
        }
        let request =
            serde_json::from_slice::<AgentApprovalRequest>(value.bytes()).map_err(|source| {
                CheckpointCodecError::with_source("agent approval payload decoding failed", source)
            })?;
        Ok(InterruptPayload::new(request))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use group_agent_core::{
        CheckpointCodec as _, CheckpointCodecError, CheckpointState as _, CodecDescriptor,
        EncodedValue, GraphState as _, InterruptPayload,
    };
    use group_agent_model::{AssistantMessage, Message, ToolCall, ToolCallId, ToolName};
    use serde_json::json;

    use super::AgentSnapshotCodec;
    use crate::state::{AgentState, AgentUpdate};
    use crate::{AgentApprovalRequest, AgentSnapshot};

    fn mid_run_snapshot() -> AgentSnapshot {
        let mut state = AgentState::new(vec![Message::user("SECRET_QUESTION")]);
        state
            .apply(AgentUpdate::ModelCompleted {
                message: AssistantMessage::text("SECRET_ANSWER"),
                usage: None,
            })
            .expect("model update commits");
        state.snapshot().expect("snapshot succeeds")
    }

    #[test]
    fn descriptor_is_the_stable_durable_identity() {
        let descriptor = AgentSnapshotCodec.snapshot_descriptor();

        assert_eq!(descriptor.schema(), "group-agent-prebuilt-agent-state");
        assert_eq!(descriptor.schema_version(), 1);
        assert_eq!(descriptor.encoding(), "json");
    }

    #[test]
    fn encoding_is_deterministic_for_equal_snapshots() {
        let codec = AgentSnapshotCodec;

        let first = codec.encode_snapshot(&mid_run_snapshot()).expect("encode");
        let second = codec.encode_snapshot(&mid_run_snapshot()).expect("encode");

        assert_eq!(first, second);
    }

    #[test]
    fn roundtrip_preserves_the_snapshot_bytes_and_state() {
        let codec = AgentSnapshotCodec;
        let snapshot = mid_run_snapshot();

        let bytes = codec.encode_snapshot(&snapshot).expect("encode");
        let decoded = codec.decode_snapshot(&bytes).expect("decode");
        let reencoded = codec.encode_snapshot(&decoded).expect("re-encode");

        assert_eq!(bytes, reencoded);
        let original = AgentState::restore(&snapshot).expect("restore");
        let restored = AgentState::restore(&decoded).expect("restore decoded");
        assert_eq!(original.messages(), restored.messages());
        assert_eq!(original.model_rounds(), restored.model_rounds());
        assert_eq!(original.usage_by_round(), restored.usage_by_round());
        assert_eq!(original.stop_reason(), restored.stop_reason());
    }

    #[test]
    fn garbage_and_truncated_bytes_fail_with_typed_source() {
        let codec = AgentSnapshotCodec;
        let valid = codec.encode_snapshot(&mid_run_snapshot()).expect("encode");

        for bytes in [b"not json".as_slice(), &valid[..valid.len() / 2]] {
            let error = codec
                .decode_snapshot(bytes)
                .expect_err("invalid bytes must fail");
            assert!(matches!(error, CheckpointCodecError::Failed { .. }));
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn decode_rejects_invalid_model_invariants_with_typed_source() {
        let codec = AgentSnapshotCodec;
        let invalid_call_id = br#"{"messages":[{"Assistant":{"content":[],"tool_calls":[{"id":"","name":"search","arguments":{}}],"extensions":{}}}],"model_rounds":1,"usage_by_round":[null],"stop_reason":null}"#;
        let inconsistent_usage = br#"{"messages":[],"model_rounds":1,"usage_by_round":[{"input_tokens":10,"output_tokens":4,"total_tokens":5,"extensions":{}}],"stop_reason":null}"#;

        for bytes in [invalid_call_id.as_slice(), inconsistent_usage.as_slice()] {
            let error = codec
                .decode_snapshot(bytes)
                .expect_err("invariant-violating bytes must fail");
            assert!(matches!(error, CheckpointCodecError::Failed { .. }));
            assert!(error.source().is_some());
        }
    }

    fn approval_request() -> AgentApprovalRequest {
        AgentApprovalRequest::new(vec![
            ToolCall::new(
                ToolCallId::new("SECRET_CALL_A").expect("valid call id"),
                ToolName::new("SECRET_TOOL").expect("valid tool name"),
                json!({"query": "SECRET_ARGUMENT"}),
            ),
            ToolCall::new(
                ToolCallId::new("SECRET_CALL_B").expect("valid call id"),
                ToolName::new("search").expect("valid tool name"),
                json!({"q": "rust"}),
            ),
        ])
    }

    #[test]
    fn approval_interrupt_roundtrips_with_shared_encoding_identity() {
        let codec = AgentSnapshotCodec;
        let request = approval_request();

        let encoded = codec
            .encode_interrupt(&InterruptPayload::new(request.clone()))
            .expect("approval payload encodes");
        let descriptor = encoded.descriptor();
        assert_eq!(descriptor.schema(), "group-agent-prebuilt-tool-approval");
        assert_eq!(descriptor.schema_version(), 1);
        assert_eq!(
            descriptor.encoding(),
            codec.snapshot_descriptor().encoding(),
            "interrupt and snapshot descriptors share one encoding identity"
        );

        let decoded = codec
            .decode_interrupt(&encoded)
            .expect("approval payload decodes");
        let decoded = decoded
            .downcast_ref::<AgentApprovalRequest>()
            .expect("decoded payload retains the concrete Rust type");
        assert_eq!(decoded, &request);

        let reencoded = codec
            .encode_interrupt(&InterruptPayload::new(decoded.clone()))
            .expect("decoded payload re-encodes");
        assert_eq!(encoded, reencoded, "interrupt roundtrip is canonical");
    }

    #[test]
    fn unknown_interrupt_payload_types_still_fail_closed() {
        let error = AgentSnapshotCodec
            .encode_interrupt(&InterruptPayload::new(String::from("opaque")))
            .expect_err("non-approval payloads have no durable encoding");

        assert!(matches!(
            error,
            CheckpointCodecError::UnsupportedInterruptPayload { .. }
        ));
    }

    #[test]
    fn interrupt_decode_rejects_mismatched_descriptors() {
        let codec = AgentSnapshotCodec;
        let valid = codec
            .encode_interrupt(&InterruptPayload::new(approval_request()))
            .expect("approval payload encodes");
        let mismatched = [
            CodecDescriptor::new("group-agent-prebuilt-agent-state", 1, "json"),
            CodecDescriptor::new("group-agent-prebuilt-tool-approval", 2, "json"),
            CodecDescriptor::new("group-agent-prebuilt-tool-approval", 1, "cbor"),
        ];

        for descriptor in mismatched {
            let value = EncodedValue::new(descriptor, valid.bytes().to_vec());
            let error = codec
                .decode_interrupt(&value)
                .expect_err("mismatched descriptor must fail");
            assert!(matches!(error, CheckpointCodecError::Failed { .. }));
        }
    }

    #[test]
    fn interrupt_decode_rejects_garbage_and_invariant_violating_bytes() {
        let codec = AgentSnapshotCodec;
        let descriptor = || CodecDescriptor::new("group-agent-prebuilt-tool-approval", 1, "json");
        let invalid_call_id =
            br#"{"pending_calls":[{"id":"","name":"search","arguments":{},"extensions":{}}]}"#;

        for bytes in [b"not json".as_slice(), invalid_call_id.as_slice()] {
            let value = EncodedValue::new(descriptor(), bytes.to_vec());
            let error = codec
                .decode_interrupt(&value)
                .expect_err("invalid payload bytes must fail");
            assert!(matches!(error, CheckpointCodecError::Failed { .. }));
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn approval_request_debug_reports_only_the_pending_count() {
        let rendered = format!("{:?}", approval_request());

        assert!(rendered.contains("AgentApprovalRequest"));
        assert!(rendered.contains("pending_calls: 2"));
        for marker in ["SECRET_CALL", "SECRET_TOOL", "SECRET_ARGUMENT"] {
            assert!(
                !rendered.contains(marker),
                "Debug must not contain {marker}"
            );
        }
    }
}
