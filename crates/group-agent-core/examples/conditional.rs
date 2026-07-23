use async_trait::async_trait;
use group_agent_core::{
    END, GraphEvent, GraphState, Node, NodeContext, NodeError, NodeId, START, StateError,
    StateGraph,
};

#[derive(Debug)]
struct DraftState {
    revisions: usize,
    required_revisions: usize,
}

#[derive(Clone, Copy)]
struct DraftUpdate {
    revisions: usize,
}

impl GraphState for DraftState {
    type Update = DraftUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.revisions += update.revisions;
        Ok(())
    }
}

struct RouterNode;

#[async_trait]
impl Node<DraftState> for RouterNode {
    async fn run(
        &self,
        _state: &DraftState,
        _context: &NodeContext,
    ) -> Result<DraftUpdate, NodeError> {
        Ok(DraftUpdate { revisions: 0 })
    }
}

struct ReviseNode;

#[async_trait]
impl Node<DraftState> for ReviseNode {
    async fn run(
        &self,
        _state: &DraftState,
        _context: &NodeContext,
    ) -> Result<DraftUpdate, NodeError> {
        Ok(DraftUpdate { revisions: 1 })
    }
}

struct AnswerNode;

#[async_trait]
impl Node<DraftState> for AnswerNode {
    async fn run(
        &self,
        _state: &DraftState,
        _context: &NodeContext,
    ) -> Result<DraftUpdate, NodeError> {
        Ok(DraftUpdate { revisions: 0 })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.add_node("router", RouterNode)?;
    graph.add_node("revise", ReviseNode)?;
    graph.add_node("answer", AnswerNode)?;
    graph.add_edge(START, "router");
    graph.add_conditional_edges("router", ["answer", "revise"], |state: &DraftState| {
        if state.revisions >= state.required_revisions {
            Ok(NodeId::from("answer"))
        } else {
            Ok(NodeId::from("revise"))
        }
    })?;
    graph.add_edge("revise", "router").add_edge("answer", END);

    let compiled = graph.compile()?;
    let report = compiled
        .invoke(DraftState {
            revisions: 0,
            required_revisions: 2,
        })
        .await?;

    let routes = report
        .events()
        .iter()
        .filter_map(|event| match event {
            GraphEvent::RouteSelected { source, target, .. } => {
                Some(format!("{source} -> {target}"))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(report.final_state().revisions, 2);
    assert_eq!(
        report.visited_nodes(),
        [
            NodeId::from("router"),
            NodeId::from("revise"),
            NodeId::from("router"),
            NodeId::from("revise"),
            NodeId::from("router"),
            NodeId::from("answer"),
        ]
    );

    println!("final revisions: {}", report.final_state().revisions);
    println!("visited nodes: {:?}", report.visited_nodes());
    println!("selected routes: {routes:?}");
    println!("execution steps: {}", report.steps());

    Ok(())
}
