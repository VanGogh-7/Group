mod support;

use std::error::Error as _;
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    END, EventConfig, GraphRunError, GraphState, Node, NodeContext, NodeError, RunConfig,
    RunControl, START, StateError, StateGraph,
};
use group_agent_model::{ChatModel, ChatRequest, Message, ModelCapabilities};
use support::{
    HangingRequestServer, MockResponse, MockServer, model, openai_client, stable_responses_model,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
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

fn capabilities() -> ModelCapabilities {
    ModelCapabilities::new()
        .with_streaming(true)
        .with_tool_calling(true)
        .with_usage_reporting(true)
}

fn graph(model: ChatModel) -> group_agent_core::CompiledGraph<AgentState> {
    let mut graph = StateGraph::new();
    graph.add_node("model", ModelNode { model }).expect("node");
    graph.add_edge(START, "model").add_edge("model", END);
    graph.compile().expect("compile")
}

fn initial_state() -> AgentState {
    AgentState {
        prompt: "node prompt".to_owned(),
        answer: None,
    }
}

#[tokio::test]
async fn ordinary_group_node_uses_real_genai_adapter_without_core_changes() {
    let server = MockServer::start(MockResponse::json(
        r#"{
          "model":"provider-model",
          "choices":[{"message":{"role":"assistant","content":"node answer"},"finish_reason":"stop"}]
        }"#,
    ))
    .await
    .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let report = graph(model)
        .invoke(initial_state())
        .await
        .expect("graph run");
    assert_eq!(report.final_state().answer.as_deref(), Some("node answer"));
}

#[tokio::test]
async fn graph_error_source_chain_reaches_concrete_genai_error() {
    let server = MockServer::start(MockResponse::json("{malformed"))
        .await
        .expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let error = graph(model)
        .invoke(initial_state())
        .await
        .expect_err("node must fail");
    assert!(matches!(error, GraphRunError::NodeFailed { .. }));

    let mut source = error.source();
    let mut found_genai = false;
    while let Some(current) = source {
        if current.is::<genai::Error>() {
            found_genai = true;
            break;
        }
        source = current.source();
    }
    assert!(found_genai, "source chain must retain genai::Error");
}

#[tokio::test]
async fn malformed_responses_body_is_redacted_by_model_node_and_graph_default_formatting() {
    let sentinel = "malformed-responses-provider-body-sentinel";
    let malformed_body = format!(r#"{{"provider_raw":"{sentinel}","#);

    let direct_server = MockServer::start(MockResponse::json(malformed_body.clone()))
        .await
        .expect("direct server");
    let direct_model =
        stable_responses_model(direct_server.base_url(), capabilities()).expect("direct model");
    let model_error = direct_model
        .complete(ChatRequest::new(vec![Message::user("hello")]))
        .await
        .expect_err("malformed Responses body must fail");
    assert!(model_error.source().is_some());
    for rendered in [format!("{model_error:?}"), model_error.to_string()] {
        assert!(!rendered.contains(sentinel));
    }

    let graph_server = MockServer::start(MockResponse::json(malformed_body))
        .await
        .expect("graph server");
    let graph_model =
        stable_responses_model(graph_server.base_url(), capabilities()).expect("graph model");
    let graph_error = graph(graph_model)
        .invoke(initial_state())
        .await
        .expect_err("node must preserve the provider error");
    assert!(graph_error.source().is_some());
    for rendered in [format!("{graph_error:?}"), graph_error.to_string()] {
        assert!(!rendered.contains(sentinel));
    }
    let GraphRunError::NodeFailed { source, .. } = &graph_error else {
        panic!("model failure must remain a node failure");
    };
    for rendered in [format!("{source:?}"), source.to_string()] {
        assert!(!rendered.contains(sentinel));
    }
}

#[tokio::test]
async fn node_timeout_drops_the_in_flight_genai_future() {
    let mut server = HangingRequestServer::start().await.expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let compiled = graph(model);
    let task = tokio::spawn(async move {
        compiled
            .invoke_with_control(
                initial_state(),
                RunConfig::default(),
                EventConfig::default(),
                RunControl::new().with_node_timeout(Duration::from_millis(200)),
            )
            .await
    });
    server.wait_received().await;
    let error = task.await.expect("task").expect_err("node timeout");
    assert!(matches!(error, GraphRunError::NodeTimedOut { .. }));
    tokio::time::timeout(Duration::from_secs(2), server.wait_closed())
        .await
        .expect("timeout must drop HTTP future");
}

#[tokio::test]
async fn cancellation_drops_the_in_flight_genai_future() {
    let mut server = HangingRequestServer::start().await.expect("server");
    let model = model(openai_client(server.base_url()), capabilities()).expect("model");
    let compiled = graph(model);
    let cancellation = CancellationToken::new();
    let task_token = cancellation.clone();
    let task = tokio::spawn(async move {
        compiled
            .invoke_with_control(
                initial_state(),
                RunConfig::default(),
                EventConfig::default(),
                RunControl::new().with_cancellation_token(task_token),
            )
            .await
    });
    server.wait_received().await;
    cancellation.cancel();
    let error = task.await.expect("task").expect_err("cancelled run");
    assert!(matches!(error, GraphRunError::Cancelled { .. }));
    tokio::time::timeout(Duration::from_secs(2), server.wait_closed())
        .await
        .expect("cancellation must drop HTTP future");
}
