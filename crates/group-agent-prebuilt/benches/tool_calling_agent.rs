#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use criterion::{Criterion, criterion_group, criterion_main};
use group_agent_model::Message;
use group_agent_prebuilt::{AgentConfig, ToolCallingAgent};
use offline_agent::{ScriptedModel, empty_runtime, local_runtime};

fn orchestration(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime builds");
    let model_only = ToolCallingAgent::new(
        ScriptedModel::model_only().expect("static model metadata is valid"),
        empty_runtime(),
        AgentConfig::new(1).expect("static configuration is valid"),
    )
    .expect("model-only Agent graph compiles");
    let one_tool_round = ToolCallingAgent::new(
        ScriptedModel::one_tool_round().expect("static model metadata is valid"),
        local_runtime().expect("static local Tool is valid"),
        AgentConfig::new(2).expect("static configuration is valid"),
    )
    .expect("Tool-calling Agent graph compiles");

    criterion.bench_function("prebuilt/model_only_final_answer", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            model_only
                .invoke(vec![Message::user("independent model-only input")])
                .await
                .expect("offline invocation succeeds")
        });
    });
    criterion.bench_function("prebuilt/one_tool_round_final_answer", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            one_tool_round
                .invoke(vec![Message::user("independent Tool input")])
                .await
                .expect("offline invocation succeeds")
        });
    });
}

criterion_group!(benches, orchestration);
criterion_main!(benches);
