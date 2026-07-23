use async_trait::async_trait;
use group_agent_core::{
    END, GraphState, Node, NodeContext, NodeError, START, StateError, StateGraph,
};

#[derive(Clone, Debug, Default)]
struct CounterState {
    count: i32,
}

#[derive(Clone, Copy, Debug)]
struct CounterUpdate {
    amount: i32,
}

impl GraphState for CounterState {
    type Update = CounterUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.count += update.amount;
        Ok(())
    }
}

struct IncrementNode {
    amount: i32,
}

#[async_trait]
impl Node<CounterState> for IncrementNode {
    async fn run(
        &self,
        state: &CounterState,
        context: &NodeContext,
    ) -> Result<CounterUpdate, NodeError> {
        println!(
            "step {}: {} observed count {}",
            context.step(),
            context.node_id(),
            state.count
        );
        Ok(CounterUpdate {
            amount: self.amount,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::<CounterState>::new();
    graph.add_node("increment_a", IncrementNode { amount: 1 })?;
    graph.add_node("increment_b", IncrementNode { amount: 2 })?;
    graph
        .add_edge(START, "increment_a")
        .add_edge("increment_a", "increment_b")
        .add_edge("increment_b", END);

    let compiled = graph.compile()?;
    let report = compiled.invoke(CounterState::default()).await?;

    assert_eq!(report.final_state().count, 3);
    assert_eq!(report.steps(), 2);
    assert_eq!(
        report
            .visited_nodes()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["increment_a", "increment_b"]
    );

    println!("final count: {}", report.final_state().count);
    println!("visited nodes: {:?}", report.visited_nodes());
    println!("execution steps: {}", report.steps());

    Ok(())
}
