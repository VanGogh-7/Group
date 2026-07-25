use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    END, GraphState, Node, NodeContext, NodeError, START, StateError, StateGraph,
};
use group_agent_model::{
    AssistantMessage, ChatModel, ChatModelAdapter, ChatRequest, ChatResponse, FinishReason,
    Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId,
    ValidatedChatRequest,
};

struct AgentState {
    prompt: String,
    answer: Option<String>,
}

struct AgentUpdate {
    answer: String,
}

impl GraphState for AgentState {
    type Update = AgentUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.answer = Some(update.answer);
        Ok(())
    }
}

struct MockChatModel {
    metadata: ModelMetadata,
}

impl MockChatModel {
    fn new() -> Result<Self, group_agent_model::IdentifierError> {
        Ok(Self {
            metadata: ModelMetadata::new(
                ProviderId::new("mock")?,
                ModelId::new("mock-echo")?,
                ModelCapabilities::new(),
            ),
        })
    }
}

#[async_trait]
impl ChatModelAdapter for MockChatModel {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        // An independent provider adapter receives only a facade-validated
        // wrapper. It may inspect accessors or consume the original request
        // for provider mapping without cloning.
        let _provider_request = request.into_inner();
        Ok(ChatResponse::new(
            AssistantMessage::text("This answer came from the offline mock model."),
            FinishReason::Stop,
        )
        .with_model(self.metadata.model().clone()))
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
        let request = ChatRequest::new(vec![Message::user(state.prompt.as_str())]);
        let response = self
            .model
            .complete(request)
            .await
            .map_err(|source| NodeError::with_source("chat model failed", source))?;
        Ok(AgentUpdate {
            answer: response.message().text_content(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = ChatModel::new(Arc::new(MockChatModel::new()?))?;
    let mut graph = StateGraph::new();
    graph.add_node("model", ModelNode { model })?;
    graph.add_edge(START, "model").add_edge("model", END);

    // Group cancellation and node timeouts drop the node future. That drop
    // propagates directly into ChatModel::complete; the model crate does not
    // duplicate Group's cancellation or timeout controls.
    let report = graph
        .compile()?
        .invoke(AgentState {
            prompt: "Explain the integration boundary.".to_owned(),
            answer: None,
        })
        .await?;

    println!(
        "{}",
        report
            .final_state()
            .answer
            .as_deref()
            .expect("model node sets the answer")
    );
    Ok(())
}
