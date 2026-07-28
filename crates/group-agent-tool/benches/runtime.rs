use std::future::poll_fn;
use std::hint::black_box;
use std::task::Poll;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use group_agent_tool::{
    Tool, ToolBatchConfig, ToolBehavior, ToolCall, ToolCallId, ToolDefinition, ToolError,
    ToolInput, ToolName, ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::{Value, json};

struct ImmediateTool {
    definition: ToolDefinition,
}

struct ReverseReadyTool {
    definition: ToolDefinition,
    batch_width: usize,
}

#[async_trait]
impl Tool for ReverseReadyTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let slot = input
            .arguments()
            .get("value")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .expect("validated benchmark slot");
        let mut pending_polls = self.batch_width - slot;
        poll_fn(move |context| {
            if pending_polls == 0 {
                Poll::Ready(())
            } else {
                pending_polls -= 1;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
        Ok(ToolOutput::success_text(slot.to_string()))
    }
}

#[async_trait]
impl Tool for ImmediateTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        Ok(ToolOutput::success_text(input.call_id().as_str()))
    }
}

fn definition(name: &str, schema: Value) -> ToolDefinition {
    ToolDefinition::new(
        ToolName::new(name).expect("benchmark name"),
        "Immediate benchmark tool",
        schema,
    )
}

fn registry(size: usize) -> ToolRegistry {
    let mut builder = ToolRegistry::builder();
    for index in 0..size {
        builder
            .register(ImmediateTool {
                definition: definition(
                    &format!("tool-{index:04}"),
                    json!({
                        "type": "object",
                        "properties": {"value": {"type": "integer"}},
                        "required": ["value"],
                        "additionalProperties": false
                    }),
                ),
            })
            .expect("benchmark tool registers");
    }
    builder.build()
}

fn call(id: &str, name: &str, value: usize) -> ToolCall {
    ToolCall::new(
        ToolCallId::new(id).expect("benchmark call id"),
        ToolName::new(name).expect("benchmark tool name"),
        json!({"value": value}),
    )
}

fn registry_lookup(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("registry_lookup");
    for size in [1, 100, 1_000] {
        let registry = registry(size);
        let target = ToolName::new(format!("tool-{:04}", size - 1)).expect("target name");
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |bencher, _| {
            bencher.iter(|| black_box(registry.get(black_box(&target))));
        });
    }
    group.finish();
}

fn schema_validation(criterion: &mut Criterion) {
    let simple_schema = json!({
        "type": "object",
        "properties": {"value": {"type": "integer"}},
        "required": ["value"],
        "additionalProperties": false
    });
    let complex_schema = json!({
        "type": "object",
        "properties": {
            "request": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1, "maxLength": 64},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string", "pattern": "^[a-z][a-z0-9_-]+$"},
                        "minItems": 1,
                        "maxItems": 8,
                        "uniqueItems": true
                    },
                    "priority": {"type": "integer", "minimum": 0, "maximum": 10}
                },
                "required": ["name", "tags", "priority"],
                "additionalProperties": false
            }
        },
        "required": ["request"],
        "additionalProperties": false
    });
    let mut simple_builder = ToolRegistry::builder();
    simple_builder
        .register(ImmediateTool {
            definition: definition("simple", simple_schema),
        })
        .expect("simple schema compiles");
    let simple_runtime = ToolRuntime::new(simple_builder.build());
    let simple_call = ToolCall::new(
        ToolCallId::new("simple-validation").expect("call id"),
        ToolName::new("simple").expect("tool name"),
        json!({"value": 7}),
    );
    let mut complex_builder = ToolRegistry::builder();
    complex_builder
        .register(ImmediateTool {
            definition: definition("complex", complex_schema),
        })
        .expect("complex schema compiles");
    let complex_runtime = ToolRuntime::new(complex_builder.build());
    let complex_call = ToolCall::new(
        ToolCallId::new("complex-validation").expect("call id"),
        ToolName::new("complex").expect("tool name"),
        json!({
            "request": {
                "name": "benchmark",
                "tags": ["alpha", "beta_2", "gamma-3"],
                "priority": 5
            }
        }),
    );

    let mut group = criterion.benchmark_group("schema_validation");
    group.bench_function("simple", |bencher| {
        bencher.iter(|| {
            black_box(
                futures_executor::block_on(simple_runtime.execute(black_box(&simple_call)))
                    .expect("simple cached validation succeeds"),
            )
        });
    });
    group.bench_function("complex", |bencher| {
        bencher.iter(|| {
            black_box(
                futures_executor::block_on(complex_runtime.execute(black_box(&complex_call)))
                    .expect("complex cached validation succeeds"),
            )
        });
    });
    group.finish();
}

fn dispatch(criterion: &mut Criterion) {
    let runtime = ToolRuntime::new(registry(1));
    let call = call("dispatch-call", "tool-0000", 1);
    criterion.bench_function("dispatch/immediate", |bencher| {
        bencher.iter(|| {
            black_box(
                futures_executor::block_on(runtime.execute(black_box(&call)))
                    .expect("dispatch succeeds"),
            )
        });
    });
}

fn batch(criterion: &mut Criterion) {
    let runtime = ToolRuntime::new(registry(1));
    let calls = (0..8)
        .map(|index| call(&format!("batch-{index}"), "tool-0000", index))
        .collect::<Vec<_>>();
    let mut group = criterion.benchmark_group("batch");
    group.bench_function("eight_calls", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut measured = Duration::ZERO;
            for _ in 0..iterations {
                let iteration_calls = calls.clone();
                let started = Instant::now();
                let report = futures_executor::block_on(
                    runtime.execute_batch(iteration_calls, ToolBatchConfig::new(8)),
                )
                .expect("batch succeeds");
                black_box(report.results());
                measured += started.elapsed();
                drop(report);
            }
            measured
        });
    });

    let mut reverse_builder = ToolRegistry::builder();
    reverse_builder
        .register(ReverseReadyTool {
            definition: definition(
                "reverse-ready",
                json!({
                    "type": "object",
                    "properties": {"value": {"type": "integer", "minimum": 0, "maximum": 7}},
                    "required": ["value"],
                    "additionalProperties": false
                }),
            ),
            batch_width: 8,
        })
        .expect("reverse-ready tool registers");
    let reverse_runtime = ToolRuntime::new(reverse_builder.build());
    let reverse_calls = (0..8)
        .map(|index| call(&format!("reverse-{index}"), "reverse-ready", index))
        .collect::<Vec<_>>();
    let reverse_expected = (0..8).map(|index| index.to_string()).collect::<Vec<_>>();
    group.bench_function("stable_result_order", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut measured = Duration::ZERO;
            for _ in 0..iterations {
                let iteration_calls = reverse_calls.clone();
                let started = Instant::now();
                let report = futures_executor::block_on(
                    reverse_runtime.execute_batch(iteration_calls, ToolBatchConfig::new(8)),
                )
                .expect("batch succeeds");
                let ordered = report.results().iter().enumerate().all(|(index, result)| {
                    result.as_ref().is_ok_and(|result| {
                        result.content().first().and_then(|part| part.as_text())
                            == Some(reverse_expected[index].as_str())
                    })
                });
                black_box(ordered);
                measured += started.elapsed();
                drop(report);
            }
            measured
        });
    });
    group.finish();
}

criterion_group!(benches, registry_lookup, schema_validation, dispatch, batch);
criterion_main!(benches);
