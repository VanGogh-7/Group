use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy, CheckpointState,
    Checkpointer, CodecDescriptor, END, EventConfig, GraphState, InMemoryCheckpointer, Node,
    NodeContext, NodeError, ReplayConfig, RunConfig, RunControl, START, SnapshotError, StateError,
    StateGraph, ThreadId,
};

#[derive(Debug, Default)]
struct CounterState {
    value: usize,
}

#[derive(Debug)]
struct CounterSnapshot {
    value: usize,
}

struct CounterCodec;

impl CheckpointCodec<CounterSnapshot> for CounterCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.example.replay.counter", 1, "little-endian-usize-v1")
    }

    fn encode_snapshot(&self, snapshot: &CounterSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.value.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<CounterSnapshot, CheckpointCodecError> {
        let value = bytes
            .try_into()
            .map(usize::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid replay example snapshot"))?;
        Ok(CounterSnapshot { value })
    }
}

impl GraphState for CounterState {
    type Update = usize;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }
}

impl CheckpointState for CounterState {
    type Snapshot = CounterSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(CounterSnapshot { value: self.value })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            value: snapshot.value,
        })
    }
}

struct Add(usize);

#[async_trait]
impl Node<CounterState> for Add {
    async fn run(&self, _state: &CounterState, _context: &NodeContext) -> Result<usize, NodeError> {
        Ok(self.0)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.set_version("replay-example-v1");
    graph.add_node("one", Add(1))?;
    graph.add_node("two", Add(2))?;
    graph.add_node("three", Add(3))?;
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    let compiled = graph.compile()?;

    let thread_id = ThreadId::from("replay-example");
    let checkpointer = Arc::new(InMemoryCheckpointer::new(CounterCodec));
    compiled
        .invoke_with_checkpoint(
            CounterState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                thread_id.clone(),
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<CounterSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await?;
    let before = checkpointer.history(&thread_id).await?;
    let historical = before.first().expect("first checkpoint").id();
    let latest = before.last().expect("latest checkpoint").id();

    let replay = compiled
        .replay(
            ReplayConfig::new(
                thread_id.clone(),
                historical,
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<CounterSnapshot>>,
            )
            .with_run_config(RunConfig::new(2)),
        )
        .await?;
    let after = checkpointer.history(&thread_id).await?;

    assert_eq!(replay.final_state().value, 6);
    assert_eq!(replay.source_checkpoint_id(), historical);
    assert_eq!(after.len(), before.len());
    assert_eq!(after.last().expect("latest checkpoint").id(), latest);

    println!("replayed checkpoint: {historical}");
    println!("replay run: {}", replay.run_id());
    println!("final value: {}", replay.final_state().value);
    println!("source history unchanged: {} checkpoints", after.len());
    Ok(())
}
