use group_agent_core::{CheckpointCodec, CheckpointCodecError, CodecDescriptor};

use crate::AgentSnapshot;

/// Canonical JSON codec for durable Agent snapshots.
///
/// The codec serializes the opaque [`AgentSnapshot`] through the Model crate's
/// feature-gated serde surface. Its descriptor is a durable compatibility
/// identity: the schema, schema version, and encoding triple is recorded in
/// every stored checkpoint, and changing any component requires a deliberate
/// migration of stored checkpoints.
///
/// Interrupt payloads are rejected by the trait defaults because this crate's
/// graph contains no Interruptible Nodes.
///
/// Decoding enforces the Model types' invariants — validated identifiers,
/// extension keys, and token-usage consistency — in addition to JSON
/// structure, so bytes that would construct a value the public constructors
/// reject fail with a typed [`CheckpointCodecError`] instead.
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentSnapshotCodec;

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
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use group_agent_core::{
        CheckpointCodec as _, CheckpointCodecError, CheckpointState as _, GraphState as _,
        InterruptPayload,
    };
    use group_agent_model::{AssistantMessage, Message};

    use super::AgentSnapshotCodec;
    use crate::AgentSnapshot;
    use crate::state::{AgentState, AgentUpdate};

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

    #[test]
    fn interrupt_payload_encoding_is_rejected_by_default() {
        let error = AgentSnapshotCodec
            .encode_interrupt(&InterruptPayload::new(7_usize))
            .expect_err("no Interruptible Node exists");

        assert!(matches!(
            error,
            CheckpointCodecError::UnsupportedInterruptPayload { .. }
        ));
    }
}
