use std::error::Error as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointFormatVersion,
    CheckpointId, CheckpointPolicy, CheckpointRecord, CheckpointRecordParts, CheckpointState,
    CheckpointStore, CheckpointWriteError, Checkpointer, CheckpointerError, CodecDescriptor, END,
    EncodedValue, EventConfig, GraphRunError, GraphState, GraphVersion, InMemoryCheckpointer,
    InterruptId, InterruptPayload, InterruptibleNode, Node, NodeContext, NodeError, NodeOutcome,
    NodePath, RecordCheckpointer, ResumeConfig, RunConfig, RunControl, RunFailure, RunId,
    SnapshotError, StateError, StateGraph, ThreadId,
};

#[derive(Debug, Eq, PartialEq)]
struct DurableSnapshot {
    value: u64,
}

#[derive(Debug, Default)]
struct DurableState {
    value: u64,
}

impl GraphState for DurableState {
    type Update = u64;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }
}

impl CheckpointState for DurableState {
    type Snapshot = DurableSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(DurableSnapshot { value: self.value })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            value: snapshot.value,
        })
    }
}

struct Add(u64);

#[async_trait]
impl Node<DurableState> for Add {
    async fn run(&self, _state: &DurableState, _context: &NodeContext) -> Result<u64, NodeError> {
        Ok(self.0)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DurablePrompt {
    message: String,
}

struct Approval;

#[async_trait]
impl InterruptibleNode<DurableState> for Approval {
    async fn run(
        &self,
        _state: &DurableState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<u64>, NodeError> {
        if let Some(value) = context.resume_value::<u64>() {
            Ok(NodeOutcome::update(*value))
        } else {
            Ok(NodeOutcome::interrupt(DurablePrompt {
                message: String::from("approve durable work"),
            }))
        }
    }
}

struct DurableCodec;

impl CheckpointCodec<DurableSnapshot> for DurableCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new(
            "group.tests.durable.snapshot",
            1,
            "group.tests.durable.raw-v1",
        )
    }

    fn encode_snapshot(&self, snapshot: &DurableSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.value.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<DurableSnapshot, CheckpointCodecError> {
        let value = bytes
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid durable snapshot"))?;
        Ok(DurableSnapshot { value })
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let prompt = payload
            .downcast_ref::<DurablePrompt>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        Ok(EncodedValue::new(
            CodecDescriptor::new(
                "group.tests.durable.prompt",
                1,
                "group.tests.durable.raw-v1",
            ),
            prompt.message.as_bytes(),
        ))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        if value.descriptor()
            != &CodecDescriptor::new(
                "group.tests.durable.prompt",
                1,
                "group.tests.durable.raw-v1",
            )
        {
            return Err(CheckpointCodecError::message(
                "unsupported durable interrupt schema",
            ));
        }
        let message = std::str::from_utf8(value.bytes())
            .map_err(|source| CheckpointCodecError::with_source("invalid prompt bytes", source))?;
        Ok(InterruptPayload::new(DurablePrompt {
            message: message.to_owned(),
        }))
    }
}

struct LocalOnlyCodec;

impl CheckpointCodec<DurableSnapshot> for LocalOnlyCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        DurableCodec.snapshot_descriptor()
    }

    fn encode_snapshot(&self, snapshot: &DurableSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        DurableCodec.encode_snapshot(snapshot)
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<DurableSnapshot, CheckpointCodecError> {
        DurableCodec.decode_snapshot(bytes)
    }
}

#[derive(Default)]
struct ExternalRecordStore {
    records: Mutex<Vec<Arc<CheckpointRecord>>>,
}

impl ExternalRecordStore {
    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Vec<Arc<CheckpointRecord>>>, CheckpointerError> {
        self.records
            .lock()
            .map_err(|_| CheckpointerError::message("external record lock poisoned"))
    }
}

#[async_trait]
impl CheckpointStore for ExternalRecordStore {
    async fn save(
        &self,
        record: CheckpointRecord,
    ) -> Result<Arc<CheckpointRecord>, CheckpointWriteError> {
        let mut records = self.lock().map_err(CheckpointWriteError::Failed)?;
        if let Some(existing) = records.iter().find(|existing| existing.id() == record.id()) {
            return if existing.as_ref() == &record {
                Ok(Arc::clone(existing))
            } else {
                Err(CheckpointWriteError::IdempotencyConflict {
                    checkpoint_id: record.id(),
                })
            };
        }
        let actual_parent = records
            .iter()
            .rev()
            .find(|existing| existing.thread_id() == record.thread_id())
            .map(|existing| existing.id());
        if actual_parent != record.parent_id() {
            return Err(CheckpointWriteError::Conflict {
                expected_parent: record.parent_id(),
                actual_parent,
            });
        }
        let record = Arc::new(record);
        records.push(Arc::clone(&record));
        Ok(record)
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError> {
        Ok(self
            .lock()?
            .iter()
            .rev()
            .find(|record| record.thread_id() == thread_id)
            .cloned())
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError> {
        Ok(self
            .lock()?
            .iter()
            .find(|record| record.thread_id() == thread_id && record.id() == checkpoint_id)
            .cloned())
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<CheckpointRecord>>, CheckpointerError> {
        Ok(self
            .lock()?
            .iter()
            .filter(|record| record.thread_id() == thread_id)
            .cloned()
            .collect())
    }
}

fn linear_graph() -> group_agent_core::CompiledGraph<DurableState> {
    let mut graph = StateGraph::new();
    graph.set_version("durable-v1");
    graph.add_node("one", Add(1)).expect("one should register");
    graph.add_node("two", Add(2)).expect("two should register");
    graph
        .add_edge(group_agent_core::START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    graph.compile().expect("linear graph should compile")
}

fn interrupt_graph() -> group_agent_core::CompiledGraph<DurableState> {
    let mut graph = StateGraph::new();
    graph.set_version("durable-interrupt-v1");
    graph
        .add_interruptible_node("approval", Approval)
        .expect("approval should register");
    graph
        .add_edge(group_agent_core::START, "approval")
        .add_edge("approval", END);
    graph.compile().expect("interrupt graph should compile")
}

fn nested_subgraph() -> group_agent_core::CompiledGraph<DurableState> {
    let mut inner = StateGraph::new();
    inner
        .add_node("one", Add(1))
        .expect("inner one should register");
    inner
        .add_node("two", Add(2))
        .expect("inner two should register");
    inner
        .add_edge(group_agent_core::START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);

    let mut middle = StateGraph::new();
    middle
        .add_subgraph("inner", inner.compile().expect("inner should compile"))
        .expect("inner should mount");
    middle
        .add_edge(group_agent_core::START, "inner")
        .add_edge("inner", END);

    let mut root = StateGraph::new();
    root.set_version("durable-nested-v1");
    root.add_subgraph("middle", middle.compile().expect("middle should compile"))
        .expect("middle should mount");
    root.add_edge(group_agent_core::START, "middle")
        .add_edge("middle", END);
    root.compile().expect("root should compile")
}

fn typed_adapter(store: Arc<dyn CheckpointStore>) -> Arc<RecordCheckpointer<DurableSnapshot>> {
    Arc::new(RecordCheckpointer::new(store, Arc::new(DurableCodec)))
}

fn record(
    checkpoint_id: CheckpointId,
    run_id: RunId,
    parent_id: Option<CheckpointId>,
    step: u64,
    value: u64,
    completed: bool,
) -> CheckpointRecord {
    CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: CheckpointFormatVersion::CURRENT,
        checkpoint_id,
        thread_id: ThreadId::from("record-thread"),
        run_id,
        parent_id,
        graph_version: Some(GraphVersion::from("durable-v1")),
        superstep: step,
        step,
        snapshot: EncodedValue::new(
            CodecDescriptor::new(
                "group.tests.durable.snapshot",
                1,
                "group.tests.durable.raw-v1",
            ),
            value.to_le_bytes().to_vec(),
        ),
        next_frontier: if completed {
            Vec::new()
        } else {
            vec![NodePath::from("two")]
        },
        completed,
        interrupt: None,
    })
    .expect("test record should be valid")
}

#[tokio::test]
async fn records_survive_restart_reconstruction_and_resume() {
    let graph = linear_graph();
    let original_store = Arc::new(ExternalRecordStore::default());
    let original = typed_adapter(Arc::clone(&original_store) as Arc<dyn CheckpointStore>);
    let failure = graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "restart",
                Arc::clone(&original) as Arc<dyn Checkpointer<DurableSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("one-step invocation should stop after saving");
    assert!(matches!(
        failure,
        GraphRunError::MaxStepsExceeded { step: 2, .. }
    ));
    let persisted = original_store
        .latest(&ThreadId::from("restart"))
        .await
        .expect("record should load")
        .expect("record should exist")
        .as_ref()
        .clone();

    drop(original);
    drop(original_store);

    let restarted_store = Arc::new(ExternalRecordStore::default());
    restarted_store
        .save(persisted)
        .await
        .expect("persisted record should import");
    let restarted = typed_adapter(Arc::clone(&restarted_store) as Arc<dyn CheckpointStore>);
    let outcome = graph
        .resume(
            ResumeConfig::new(
                "restart",
                Arc::clone(&restarted) as Arc<dyn Checkpointer<DurableSnapshot>>,
            )
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("fresh adapter should decode, restore, and resume");
    assert_eq!(outcome.final_state().value, 3);
    let history = restarted_store
        .history(&ThreadId::from("restart"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].parent_id(), Some(history[0].id()));
    assert!(history[1].completed());
}

#[tokio::test]
async fn durable_nested_subgraph_record_only_restart_resumes_structured_frontier() {
    let graph = nested_subgraph();
    let original_store = Arc::new(ExternalRecordStore::default());
    let original = typed_adapter(Arc::clone(&original_store) as Arc<dyn CheckpointStore>);
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "nested-restart",
                Arc::clone(&original) as Arc<dyn Checkpointer<DurableSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("one-step invocation should stop before the second nested node");
    let persisted = original_store
        .latest(&ThreadId::from("nested-restart"))
        .await
        .expect("record should load")
        .expect("record should exist")
        .as_ref()
        .clone();
    assert_eq!(
        persisted.next_frontier(),
        [NodePath::new(
            &group_agent_core::GraphPath::new(["middle", "inner"]),
            "two",
        )]
    );

    drop(original);
    drop(original_store);

    let restarted_store = Arc::new(ExternalRecordStore::default());
    restarted_store
        .save(persisted)
        .await
        .expect("persisted nested record should import");
    let restarted = typed_adapter(Arc::clone(&restarted_store) as Arc<dyn CheckpointStore>);
    let outcome = graph
        .resume(
            ResumeConfig::new(
                "nested-restart",
                restarted as Arc<dyn Checkpointer<DurableSnapshot>>,
            )
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("fresh adapter should decode and resume nested frontier");
    assert_eq!(outcome.final_state().value, 3);
}

#[tokio::test]
async fn durable_interrupt_payload_survives_restart_and_resume() {
    let graph = interrupt_graph();
    let original_store = Arc::new(ExternalRecordStore::default());
    let original = typed_adapter(Arc::clone(&original_store) as Arc<dyn CheckpointStore>);
    let outcome = graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "durable-interrupt",
                Arc::clone(&original) as Arc<dyn Checkpointer<DurableSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("interrupt should save");
    assert!(outcome.as_interrupted().is_some());
    let persisted = original_store
        .latest(&ThreadId::from("durable-interrupt"))
        .await
        .expect("record should load")
        .expect("record should exist")
        .as_ref()
        .clone();
    drop(outcome);
    drop(original);
    drop(original_store);

    let restarted_store = Arc::new(ExternalRecordStore::default());
    restarted_store
        .save(persisted)
        .await
        .expect("interrupt record should import");
    let restarted = typed_adapter(Arc::clone(&restarted_store) as Arc<dyn CheckpointStore>);
    let decoded = restarted
        .latest(&ThreadId::from("durable-interrupt"))
        .await
        .expect("typed checkpoint should decode")
        .expect("typed checkpoint should exist");
    assert_eq!(
        decoded
            .interrupt()
            .expect("interrupt should remain")
            .payload()
            .downcast_ref::<DurablePrompt>()
            .expect("payload type should reconstruct")
            .message,
        "approve durable work"
    );
    let resumed = graph
        .resume(
            ResumeConfig::new(
                "durable-interrupt",
                restarted as Arc<dyn Checkpointer<DurableSnapshot>>,
            )
            .with_resume_value(7_u64),
        )
        .await
        .expect("durable interrupt should resume");
    assert_eq!(resumed.final_state().value, 7);
}

#[tokio::test]
async fn local_only_interrupt_is_rejected_before_entering_record_store() {
    let graph = interrupt_graph();
    let store = Arc::new(InMemoryCheckpointer::new(LocalOnlyCodec));
    let events = Arc::new(Mutex::new(Vec::new()));
    let error = graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::default(),
            EventConfig::default().with_sink({
                let events = Arc::clone(&events);
                Arc::new(move |event: &group_agent_core::GraphEvent| {
                    events
                        .lock()
                        .expect("event lock should not be poisoned")
                        .push(event.clone());
                })
            }),
            RunControl::default(),
            CheckpointConfig::new(
                "local-only",
                Arc::clone(&store) as Arc<dyn Checkpointer<DurableSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("unencodable payload must fail explicitly");
    assert!(matches!(
        error,
        GraphRunError::CheckpointEncodeFailed { step: 0, .. }
    ));
    assert!(
        error
            .source()
            .expect("encoding source should exist")
            .source()
            .expect("codec source should exist")
            .to_string()
            .contains("no durable encoding")
    );
    assert!(
        store
            .record_store()
            .history(&ThreadId::from("local-only"))
            .await
            .expect("history should load")
            .is_empty()
    );
    assert!(matches!(
        events
            .lock()
            .expect("event lock should not be poisoned")
            .last(),
        Some(group_agent_core::GraphEvent::RunFailed {
            failure: RunFailure::CheckpointEncodeFailed {
                thread_id,
                step: 0,
                ..
            },
            ..
        }) if thread_id == &ThreadId::from("local-only")
    ));
}

#[tokio::test]
async fn content_idempotency_ignores_arc_identity_and_survives_latest_advance() {
    let store = ExternalRecordStore::default();
    let first_id = CheckpointId::new();
    let run_id = RunId::new();
    let first = store
        .save(record(first_id, run_id, None, 1, 1, false))
        .await
        .expect("first record should save");
    let replay = store
        .save(record(first_id, run_id, None, 1, 1, false))
        .await
        .expect("equal bytes from another Arc should replay");
    assert!(Arc::ptr_eq(&first, &replay));

    let second = store
        .save(record(
            CheckpointId::new(),
            run_id,
            Some(first_id),
            2,
            2,
            true,
        ))
        .await
        .expect("latest should advance");
    let old_replay = store
        .save(record(first_id, run_id, None, 1, 1, false))
        .await
        .expect("old identical write should precede CAS");
    assert!(Arc::ptr_eq(&first, &old_replay));
    assert_eq!(
        store
            .latest(&ThreadId::from("record-thread"))
            .await
            .expect("latest should load")
            .expect("latest should exist")
            .id(),
        second.id()
    );
}

#[tokio::test]
async fn same_id_with_different_stable_content_is_an_idempotency_conflict() {
    let store = ExternalRecordStore::default();
    let checkpoint_id = CheckpointId::new();
    let run_id = RunId::new();
    store
        .save(record(checkpoint_id, run_id, None, 1, 1, false))
        .await
        .expect("first record should save");
    assert!(matches!(
        store
            .save(record(checkpoint_id, run_id, None, 1, 9, false))
            .await,
        Err(CheckpointWriteError::IdempotencyConflict {
            checkpoint_id: actual
        }) if actual == checkpoint_id
    ));
}

#[tokio::test]
async fn full_codec_descriptor_participates_in_record_idempotency() {
    let store = ExternalRecordStore::default();
    let checkpoint_id = CheckpointId::new();
    let run_id = RunId::new();
    let base = record(checkpoint_id, run_id, None, 1, 1, false);
    store
        .save(base.clone())
        .await
        .expect("base record should save");
    let different_encoding = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: base.format_version(),
        checkpoint_id,
        thread_id: base.thread_id().clone(),
        run_id,
        parent_id: base.parent_id(),
        graph_version: base.graph_version().cloned(),
        superstep: base.superstep(),
        step: base.step(),
        snapshot: EncodedValue::new(
            CodecDescriptor::new(
                base.snapshot().descriptor().schema(),
                base.snapshot().descriptor().schema_version(),
                "group.tests.durable.alternate-v1",
            ),
            base.snapshot().bytes(),
        ),
        next_frontier: base.next_frontier().to_vec(),
        completed: base.completed(),
        interrupt: None,
    })
    .expect("alternate descriptor remains structurally valid");
    assert!(matches!(
        store.save(different_encoding).await,
        Err(CheckpointWriteError::IdempotencyConflict {
            checkpoint_id: actual
        }) if actual == checkpoint_id
    ));
}

#[test]
fn format_schema_and_decode_failures_are_structured() {
    let checkpoint_id = CheckpointId::new();
    let run_id = RunId::new();
    let base = record(checkpoint_id, run_id, None, 1, 1, false);
    let incompatible_format = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: CheckpointFormatVersion::new(99),
        checkpoint_id,
        thread_id: base.thread_id().clone(),
        run_id,
        parent_id: None,
        graph_version: base.graph_version().cloned(),
        superstep: 1,
        step: 1,
        snapshot: base.snapshot().clone(),
        next_frontier: base.next_frontier().to_vec(),
        completed: false,
        interrupt: None,
    })
    .expect("future record layout should remain storable");
    assert!(matches!(
        Checkpoint::<DurableSnapshot>::from_record(&incompatible_format, &DurableCodec),
        Err(group_agent_core::CheckpointReconstructionError::FormatVersion { .. })
    ));

    let incompatible_schema = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: CheckpointFormatVersion::CURRENT,
        checkpoint_id,
        thread_id: base.thread_id().clone(),
        run_id,
        parent_id: None,
        graph_version: base.graph_version().cloned(),
        superstep: 1,
        step: 1,
        snapshot: EncodedValue::new(
            CodecDescriptor::new(
                "group.tests.durable.snapshot",
                2,
                "group.tests.durable.raw-v1",
            ),
            1_u64.to_le_bytes().to_vec(),
        ),
        next_frontier: base.next_frontier().to_vec(),
        completed: false,
        interrupt: None,
    })
    .expect("schema mismatch record should remain structurally valid");
    assert!(matches!(
        Checkpoint::<DurableSnapshot>::from_record(&incompatible_schema, &DurableCodec),
        Err(group_agent_core::CheckpointReconstructionError::SnapshotSchema { .. })
    ));

    struct DecodeFailure;
    impl CheckpointCodec<DurableSnapshot> for DecodeFailure {
        fn snapshot_descriptor(&self) -> CodecDescriptor {
            DurableCodec.snapshot_descriptor()
        }

        fn encode_snapshot(
            &self,
            _snapshot: &DurableSnapshot,
        ) -> Result<Vec<u8>, CheckpointCodecError> {
            Ok(Vec::new())
        }

        fn decode_snapshot(&self, _bytes: &[u8]) -> Result<DurableSnapshot, CheckpointCodecError> {
            Err(CheckpointCodecError::with_source(
                "decode adapter failed",
                std::io::Error::new(std::io::ErrorKind::InvalidData, "decode root"),
            ))
        }
    }
    let error = Checkpoint::<DurableSnapshot>::from_record(&base, &DecodeFailure)
        .expect_err("decode should fail");
    assert_eq!(
        error
            .source()
            .expect("wrapper source")
            .source()
            .expect("codec root")
            .to_string(),
        "decode root"
    );
}

#[test]
fn codec_identity_mismatch_fails_before_snapshot_decode() {
    struct CountingCodec {
        decode_calls: Arc<AtomicUsize>,
    }

    impl CheckpointCodec<DurableSnapshot> for CountingCodec {
        fn snapshot_descriptor(&self) -> CodecDescriptor {
            CodecDescriptor::new(
                "group.tests.durable.snapshot",
                1,
                "group.tests.durable.json-v1",
            )
        }

        fn encode_snapshot(
            &self,
            snapshot: &DurableSnapshot,
        ) -> Result<Vec<u8>, CheckpointCodecError> {
            Ok(snapshot.value.to_le_bytes().to_vec())
        }

        fn decode_snapshot(&self, _bytes: &[u8]) -> Result<DurableSnapshot, CheckpointCodecError> {
            self.decode_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DurableSnapshot { value: 0 })
        }
    }

    let base = record(CheckpointId::new(), RunId::new(), None, 1, 1, false);
    let mismatched = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: base.format_version(),
        checkpoint_id: base.id(),
        thread_id: base.thread_id().clone(),
        run_id: base.run_id(),
        parent_id: base.parent_id(),
        graph_version: base.graph_version().cloned(),
        superstep: base.superstep(),
        step: base.step(),
        snapshot: EncodedValue::new(
            CodecDescriptor::new(
                "group.tests.durable.snapshot",
                1,
                "group.tests.durable.bincode-v1",
            ),
            base.snapshot().bytes(),
        ),
        next_frontier: base.next_frontier().to_vec(),
        completed: base.completed(),
        interrupt: None,
    })
    .expect("mismatched codec record should remain structurally valid");
    let decode_calls = Arc::new(AtomicUsize::new(0));
    let error = Checkpoint::<DurableSnapshot>::from_record(
        &mismatched,
        &CountingCodec {
            decode_calls: Arc::clone(&decode_calls),
        },
    )
    .expect_err("encoding identity mismatch should fail");
    assert!(matches!(
        error,
        group_agent_core::CheckpointReconstructionError::SnapshotEncoding { .. }
    ));
    assert_eq!(decode_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn record_u64_counters_use_checked_runtime_conversion_at_the_boundary() {
    let base = record(CheckpointId::new(), RunId::new(), None, 1, 1, false);
    let boundary = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: base.format_version(),
        checkpoint_id: base.id(),
        thread_id: base.thread_id().clone(),
        run_id: base.run_id(),
        parent_id: base.parent_id(),
        graph_version: base.graph_version().cloned(),
        superstep: u64::MAX,
        step: u64::MAX,
        snapshot: base.snapshot().clone(),
        next_frontier: base.next_frontier().to_vec(),
        completed: base.completed(),
        interrupt: None,
    })
    .expect("u64 counters are valid durable record values");

    if usize::BITS >= u64::BITS {
        let checkpoint = Checkpoint::<DurableSnapshot>::from_record(&boundary, &DurableCodec)
            .expect("u64 maximum fits a 64-bit Runtime");
        assert_eq!(checkpoint.superstep(), usize::MAX);
        assert_eq!(checkpoint.step(), usize::MAX);
    } else {
        assert!(matches!(
            Checkpoint::<DurableSnapshot>::from_record(&boundary, &DurableCodec),
            Err(
                group_agent_core::CheckpointReconstructionError::CounterOutOfRange {
                    field: "superstep",
                    value: u64::MAX,
                }
            )
        ));
    }
}

#[tokio::test]
async fn snapshot_encode_root_error_preserves_the_complete_source_chain() {
    struct EncodeFailureCodec;

    impl CheckpointCodec<DurableSnapshot> for EncodeFailureCodec {
        fn snapshot_descriptor(&self) -> CodecDescriptor {
            DurableCodec.snapshot_descriptor()
        }

        fn encode_snapshot(
            &self,
            _snapshot: &DurableSnapshot,
        ) -> Result<Vec<u8>, CheckpointCodecError> {
            Err(CheckpointCodecError::with_source(
                "encode adapter failed",
                std::io::Error::new(std::io::ErrorKind::InvalidData, "encode root"),
            ))
        }

        fn decode_snapshot(&self, bytes: &[u8]) -> Result<DurableSnapshot, CheckpointCodecError> {
            DurableCodec.decode_snapshot(bytes)
        }
    }

    let store = Arc::new(InMemoryCheckpointer::new(EncodeFailureCodec));
    let error = linear_graph()
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "encode-source-chain",
                store as Arc<dyn Checkpointer<DurableSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("snapshot encoding should fail");
    let encoding = error.source().expect("run error should expose encoding");
    let codec = encoding
        .source()
        .expect("encoding error should expose codec error");
    let root = codec
        .source()
        .expect("codec error should expose root error");
    assert_eq!(root.to_string(), "encode root");
}

#[tokio::test]
async fn concurrent_parent_cas_allows_one_writer_without_a_fork() {
    let store = Arc::new(ExternalRecordStore::default());
    let base_id = CheckpointId::new();
    let run_id = RunId::new();
    store
        .save(record(base_id, run_id, None, 1, 1, false))
        .await
        .expect("base should save");
    let left = record(CheckpointId::new(), RunId::new(), Some(base_id), 2, 2, true);
    let right = record(CheckpointId::new(), RunId::new(), Some(base_id), 2, 3, true);
    let (left, right) = tokio::join!(store.save(left), store.save(right));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(
        usize::from(matches!(left, Err(CheckpointWriteError::Conflict { .. })))
            + usize::from(matches!(right, Err(CheckpointWriteError::Conflict { .. }))),
        1
    );
    let history = store
        .history(&ThreadId::from("record-thread"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].parent_id(), Some(base_id));
}

#[test]
fn persisted_ids_round_trip_through_display_and_parse() {
    let checkpoint = CheckpointId::new();
    let interrupt = InterruptId::new();
    let run = RunId::new();
    assert_eq!(
        checkpoint.to_string().parse::<CheckpointId>().unwrap(),
        checkpoint
    );
    assert_eq!(
        interrupt.to_string().parse::<InterruptId>().unwrap(),
        interrupt
    );
    assert_eq!(run.to_string().parse::<RunId>().unwrap(), run);
}
