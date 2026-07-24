use async_trait::async_trait;
use group_agent_core::{
    END, GraphEvent, GraphState, Node, NodeContext, NodeError, NodeId, NodeUpdate, START,
    StateError, StateGraph,
};

#[derive(Debug, Default)]
struct ResearchState {
    use_web: bool,
    prepared: bool,
    findings: Vec<String>,
}

enum ResearchUpdate {
    Prepared,
    Finding(String),
}

impl GraphState for ResearchState {
    type Update = ResearchUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        match update {
            ResearchUpdate::Prepared => self.prepared = true,
            ResearchUpdate::Finding(finding) => self.findings.push(finding),
        }
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        let mut findings = Vec::with_capacity(updates.len());
        for update in updates {
            let (_, update) = update.into_parts();
            let ResearchUpdate::Finding(finding) = update else {
                return Err(StateError::message(
                    "parallel research branches must return findings",
                ));
            };
            findings.push(finding);
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

struct Search(&'static str);

#[async_trait]
impl Node<ResearchState> for Search {
    async fn run(
        &self,
        state: &ResearchState,
        _context: &NodeContext,
    ) -> Result<ResearchUpdate, NodeError> {
        assert!(state.prepared);
        assert!(state.findings.is_empty());
        Ok(ResearchUpdate::Finding(self.0.to_owned()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.add_node("router", Prepare)?;
    graph.add_node("local", Search("local result"))?;
    graph.add_node("web", Search("web result"))?;
    graph.add_edge(START, "router");
    graph.add_conditional_fan_out("router", ["local", "web", END], |state: &ResearchState| {
        let mut targets = vec![NodeId::from("local")];
        if state.use_web {
            targets.push(NodeId::from("web"));
        }
        Ok(targets)
    })?;
    graph.add_edge("local", END).add_edge("web", END);

    let report = graph
        .compile()?
        .invoke(ResearchState {
            use_web: true,
            ..ResearchState::default()
        })
        .await?;
    assert_eq!(
        report.final_state().findings,
        ["local result", "web result"]
    );
    let selected = report
        .events()
        .iter()
        .find_map(|event| match event {
            GraphEvent::RoutesSelected { targets, .. } => Some(targets),
            _ => None,
        })
        .expect("conditional fan-out emits RoutesSelected");

    println!("selected routes: {selected:?}");
    println!("findings: {:?}", report.final_state().findings);
    Ok(())
}
