use async_trait::async_trait;
use group_agent_core::{
    END, GraphEvent, GraphState, Node, NodeContext, NodeError, START, StateError, StateGraph,
};

#[derive(Debug, Default)]
struct ResearchState {
    evidence: Vec<&'static str>,
}

impl GraphState for ResearchState {
    type Update = &'static str;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.evidence.push(update);
        Ok(())
    }
}

struct Record(&'static str);

#[async_trait]
impl Node<ResearchState> for Record {
    async fn run(
        &self,
        _state: &ResearchState,
        context: &NodeContext,
    ) -> Result<&'static str, NodeError> {
        println!("running {}", context.node_path());
        Ok(self.0)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut research = StateGraph::new();
    research.add_node("local_search", Record("local"))?;
    research.add_node("verify", Record("verified"))?;
    research
        .add_edge(START, "local_search")
        .add_edge("local_search", "verify")
        .add_edge("verify", END);
    let research = research.compile()?;

    let mut graph = StateGraph::new();
    graph.set_version("research-workflow-v1");
    graph.add_node("prepare", Record("prepared"))?;
    graph.add_subgraph("research", research)?;
    graph.add_node("answer", Record("answered"))?;
    graph
        .add_edge(START, "prepare")
        .add_edge("prepare", "research")
        .add_edge("research", "answer")
        .add_edge("answer", END);

    let report = graph.compile()?.invoke(ResearchState::default()).await?;
    assert_eq!(
        report.final_state().evidence,
        ["prepared", "local", "verified", "answered"]
    );
    assert_eq!(report.steps(), 4);
    for event in report.events() {
        if let GraphEvent::SubgraphStarted { graph_path, .. }
        | GraphEvent::SubgraphCompleted { graph_path, .. } = event
        {
            println!("subgraph boundary: {graph_path}");
        }
    }
    Ok(())
}
