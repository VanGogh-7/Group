#[path = "../tests/support/server.rs"]
mod server;

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use group_agent_mcp::{
    McpClientSession, McpDiscoveryConfig, McpServerId, McpToolNamePolicy, McpToolPrefix,
    McpToolSet, map_call_tool_result,
};
use group_agent_model::{ToolCall, ToolCallId, ToolName};
use group_agent_tool::ToolBehavior;
use rmcp::model::{CallToolResult, ContentBlock, Tool as ProtocolTool};
use serde_json::{Map, json};

fn mapping_benchmarks(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");
    let state = Arc::new(server::ServerState::default());
    let (client, server_stream) = tokio::io::duplex(256 * 1024);
    let (server_read, server_write) = tokio::io::split(server_stream);
    let server_state = Arc::clone(&state);
    let server_task = runtime.spawn(async move {
        server::serve(
            server_read,
            server_write,
            server::ServerScenario::Standard,
            server_state,
        )
        .await;
    });
    let session = runtime
        .block_on(McpClientSession::connect(
            McpServerId::new("benchmark").expect("valid server id"),
            client,
        ))
        .expect("mock session initializes");

    let hundred_tools = (0..100)
        .map(|index| {
            let schema = serde_json::from_value::<Map<String, serde_json::Value>>(json!({
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
                "additionalProperties": false
            }))
            .expect("object schema");
            ProtocolTool::new(
                format!("tool_{index:03}"),
                "Offline mapping benchmark",
                schema,
            )
        })
        .collect::<Vec<_>>();
    let mut group = criterion.benchmark_group("mcp_mapping");
    group.bench_function("discover_100_tools", |bencher| {
        bencher.iter_batched(
            || hundred_tools.clone(),
            |tools| {
                McpToolSet::from_discovered(session.clone(), tools, McpDiscoveryConfig::new())
                    .expect("mapping succeeds")
            },
            BatchSize::SmallInput,
        );
    });

    let prefix =
        McpToolNamePolicy::Prefix(McpToolPrefix::new("server_namespace").expect("valid prefix"));
    let server_id = McpServerId::new("benchmark").expect("valid server id");
    let remote_names = (0..100)
        .map(|index| format!("remote_{index:03}"))
        .collect::<Vec<_>>();
    group.bench_function("namespace_100_names", |bencher| {
        bencher.iter(|| {
            for name in &remote_names {
                black_box(
                    prefix
                        .local_name(&server_id, black_box(name))
                        .expect("mapping succeeds"),
                );
            }
        });
    });

    let text_result = CallToolResult::success(vec![
        ContentBlock::text("first"),
        ContentBlock::text("second"),
    ]);
    group.bench_function("text_result", |bencher| {
        bencher.iter_batched(
            || text_result.clone(),
            |result| map_call_tool_result(result).expect("text mapping"),
            BatchSize::SmallInput,
        );
    });

    let mut structured_result = CallToolResult::success(Vec::new());
    structured_result.structured_content = Some(json!({
        "answer": 42,
        "items": [1, 2, 3],
        "stable": true
    }));
    group.bench_function("structured_result", |bencher| {
        bencher.iter_batched(
            || structured_result.clone(),
            |result| map_call_tool_result(result).expect("structured mapping"),
            BatchSize::SmallInput,
        );
    });

    let tool_runtime = runtime
        .block_on(
            session.discover(
                McpDiscoveryConfig::new()
                    .with_behavior_override("echo", ToolBehavior::read_only())
                    .expect("valid override"),
            ),
        )
        .expect("mock discovery")
        .runtime();
    let call = ToolCall::new(
        ToolCallId::new("benchmark-call").expect("valid call id"),
        ToolName::new("echo").expect("valid name"),
        json!({"text": "benchmark"}),
    );
    group.bench_function("dispatch_mock_session", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(
                tool_runtime
                    .execute(black_box(&call))
                    .await
                    .expect("mock dispatch"),
            )
        });
    });
    group.finish();

    runtime
        .block_on(session.shutdown())
        .expect("benchmark session shuts down");
    runtime
        .block_on(server_task)
        .expect("benchmark server joins");
}

criterion_group!(benches, mapping_benchmarks);
criterion_main!(benches);
