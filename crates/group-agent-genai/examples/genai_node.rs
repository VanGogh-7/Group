use async_trait::async_trait;
use group_agent_core::{
    END, GraphState, Node, NodeContext, NodeError, START, StateError, StateGraph,
};
use group_agent_genai::{GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig};
use group_agent_model::{ChatModel, ChatRequest, Message, ModelCapabilities, ModelId, ProviderId};

struct AgentState {
    prompt: String,
    answer: Option<String>,
}

struct AgentUpdate(String);

impl GraphState for AgentState {
    type Update = AgentUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.answer = Some(update.0);
        Ok(())
    }
}

struct ModelNode {
    model: ChatModel,
}

#[async_trait]
impl Node<AgentState> for ModelNode {
    async fn run(
        &self,
        state: &AgentState,
        _context: &NodeContext,
    ) -> Result<AgentUpdate, NodeError> {
        let response = self
            .model
            .complete(ChatRequest::new(vec![Message::user(state.prompt.as_str())]))
            .await
            .map_err(|source| NodeError::with_source("chat model failed", source))?;
        Ok(AgentUpdate(response.message().text_content()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(requested_model) = std::env::var("GROUP_GENAI_MODEL") else {
        println!("Skipped: set GROUP_GENAI_MODEL to a genai model or namespaced model identifier.");
        return Ok(());
    };
    if requested_model.trim().is_empty() {
        println!("Skipped: GROUP_GENAI_MODEL must not be empty.");
        return Ok(());
    }
    let provider = std::env::var("GROUP_GENAI_PROVIDER").unwrap_or_else(|_| "genai".to_owned());
    let prompt = std::env::var("GROUP_GENAI_PROMPT")
        .unwrap_or_else(|_| "Answer in one short sentence.".to_owned());

    // Authentication, model mapping, and endpoint resolution remain genai
    // Client concerns. This example never reads or prints an API key itself.
    let client = genai::Client::default();
    let model_config = GenaiModelConfig::new(
        requested_model.clone(),
        ProviderId::new(provider)?,
        ModelId::new(requested_model)?,
        ModelCapabilities::new()
            .with_tool_calling(true)
            .with_usage_reporting(true),
    )?;
    let adapter = GenaiChatModelAdapter::new(
        client,
        GenaiAdapterConfig::new(model_config).with_response_id_continuation(true),
    )?;
    let model = ChatModel::from_adapter(adapter)?;

    let mut graph = StateGraph::new();
    graph.add_node("model", ModelNode { model })?;
    graph.add_edge(START, "model").add_edge("model", END);
    let report = graph
        .compile()?
        .invoke(AgentState {
            prompt,
            answer: None,
        })
        .await?;
    println!(
        "{}",
        report.final_state().answer.as_deref().unwrap_or_default()
    );
    Ok(())
}
