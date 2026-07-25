use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy, CheckpointState,
    Checkpointer, CodecDescriptor, END, EventConfig, ForkConfig, GraphState, InMemoryCheckpointer,
    Node, NodeContext, NodeError, RunConfig, RunControl, START, SnapshotError, StateError,
    StateGraph, ThreadId,
};

#[derive(Debug, Default)]
struct CounterState {
    value: u64,
}

impl GraphState for CounterState {
    type Update = u64;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }
}

impl CheckpointState for CounterState {
    type Snapshot = u64;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(self.value)
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self { value: *snapshot })
    }
}

struct CounterCodec;

impl CheckpointCodec<u64> for CounterCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.example.fork.counter", 1, "u64-le-v1")
    }

    fn encode_snapshot(&self, snapshot: &u64) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<u64, CheckpointCodecError> {
        let bytes = <[u8; 8]>::try_from(bytes)
            .map_err(|_| CheckpointCodecError::message("invalid fork example snapshot"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}

struct Add(u64);

#[async_trait]
impl Node<CounterState> for Add {
    async fn run(&self, _state: &CounterState, _context: &NodeContext) -> Result<u64, NodeError> {
        Ok(self.0)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.set_version("fork-example-v1");
    graph.add_node("one", Add(1))?;
    graph.add_node("two", Add(2))?;
    graph.add_node("three", Add(3))?;
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    let graph = graph.compile()?;

    let thread_id = ThreadId::from("fork-example");
    let checkpointer = Arc::new(InMemoryCheckpointer::new(CounterCodec));
    let typed = Arc::clone(&checkpointer) as Arc<dyn Checkpointer<u64>>;
    graph
        .invoke_with_checkpoint(
            CounterState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                thread_id.clone(),
                Arc::clone(&typed),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await?;
    let original = checkpointer.history(&thread_id).await?;
    let source = original.first().expect("source checkpoint").id();
    let original_latest = original.last().expect("latest checkpoint").id();

    let fork = graph
        .fork(ForkConfig::new(
            thread_id.clone(),
            source,
            Arc::clone(&typed),
        ))
        .await?;
    let branch = checkpointer
        .branch_history(&thread_id, fork.branch_id())
        .await?;

    assert_eq!(
        fork.outcome()
            .as_completed()
            .expect("fork completed")
            .final_state()
            .value,
        6
    );
    assert_eq!(
        checkpointer
            .latest(&thread_id)
            .await?
            .expect("original latest")
            .id(),
        original_latest
    );
    println!("source checkpoint: {source}");
    println!("branch: {}", fork.branch_id());
    println!("branch checkpoints including source: {}", branch.len());
    println!("original latest unchanged: {original_latest}");
    Ok(())
}
