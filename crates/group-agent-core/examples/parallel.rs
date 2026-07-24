use async_trait::async_trait;
use group_agent_core::{
    END, GraphState, Node, NodeContext, NodeError, NodeId, NodeUpdate, START, StateError,
    StateGraph,
};

#[derive(Debug, Default)]
struct ResearchState {
    prepared: bool,
    findings: Vec<String>,
    answer: Option<String>,
}

#[derive(Debug)]
enum ResearchUpdate {
    Prepared,
    Finding(String),
    Answer(String),
}

impl GraphState for ResearchState {
    type Update = ResearchUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        match update {
            ResearchUpdate::Prepared => self.prepared = true,
            ResearchUpdate::Finding(finding) => self.findings.push(finding),
            ResearchUpdate::Answer(answer) => self.answer = Some(answer),
        }
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        let mut findings = Vec::with_capacity(updates.len());
        for update in &updates {
            let ResearchUpdate::Finding(finding) = update.update() else {
                return Err(StateError::message(format!(
                    "parallel search node `{}` returned a non-finding update",
                    update.node_id()
                )));
            };
            findings.push(finding.clone());
        }

        self.findings.extend(findings);
        Ok(())
    }
}

struct Prepare;

#[async_trait]
impl Node<ResearchState> for Prepare {
    async fn run(
        &self,
        _state: &ResearchState,
        _context: &NodeContext,
    ) -> Result<ResearchUpdate, NodeError> {
        Ok(ResearchUpdate::Prepared)
    }
}

struct Search {
    result: &'static str,
}

#[async_trait]
impl Node<ResearchState> for Search {
    async fn run(
        &self,
        state: &ResearchState,
        context: &NodeContext,
    ) -> Result<ResearchUpdate, NodeError> {
        assert!(state.prepared);
        assert!(state.findings.is_empty());
        println!("{} read the same prepared snapshot", context.node_id());
        tokio::task::yield_now().await;
        Ok(ResearchUpdate::Finding(self.result.to_owned()))
    }
}

struct Synthesis;

#[async_trait]
impl Node<ResearchState> for Synthesis {
    async fn run(
        &self,
        state: &ResearchState,
        _context: &NodeContext,
    ) -> Result<ResearchUpdate, NodeError> {
        Ok(ResearchUpdate::Answer(state.findings.join(" + ")))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.add_node("prepare", Prepare)?;
    graph.add_node(
        "local_search",
        Search {
            result: "local result",
        },
    )?;
    graph.add_node(
        "web_search",
        Search {
            result: "web result",
        },
    )?;
    graph.add_node("synthesis", Synthesis)?;
    graph.add_edge(START, "prepare");
    graph.add_fan_out("prepare", ["local_search", "web_search"])?;
    graph
        .add_edge("local_search", "synthesis")
        .add_edge("web_search", "synthesis")
        .add_edge("synthesis", END);

    let report = graph.compile()?.invoke(ResearchState::default()).await?;

    assert_eq!(
        report.final_state().findings,
        ["local result", "web result"]
    );
    assert_eq!(
        report.final_state().answer.as_deref(),
        Some("local result + web result")
    );
    assert_eq!(
        report.visited_nodes(),
        [
            NodeId::from("prepare"),
            NodeId::from("local_search"),
            NodeId::from("web_search"),
            NodeId::from("synthesis"),
        ]
    );

    println!(
        "answer: {}",
        report.final_state().answer.as_deref().unwrap()
    );
    println!("visited nodes: {:?}", report.visited_nodes());
    Ok(())
}
