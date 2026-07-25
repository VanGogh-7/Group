use std::hint::black_box;
use std::sync::Arc;

use async_trait::async_trait;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_executor::block_on;
use futures_util::stream;
use group_agent_model::{
    AssistantMessage, ChatModel, ChatModelAdapter, ChatRequest, ChatResponse, ChatStreamCollector,
    ChatStreamEvent, Extensions, FinishReason, Message, ModelCapabilities, ModelError, ModelId,
    ModelMetadata, ProviderId, TokenUsage, ToolCallDelta, ToolCallId, ToolName,
    ValidatedChatRequest, collect_chat_stream,
};
use serde_json::json;

fn model_id() -> ModelId {
    ModelId::new("mock-model").expect("static model id is valid")
}

fn request_validation(criterion: &mut Criterion) {
    let request = ChatRequest::new(
        (0..100)
            .map(|index| Message::user(format!("message-{index}")))
            .collect(),
    );
    criterion.bench_function("model/request/validate_100_messages", |bencher| {
        bencher.iter(|| black_box(&request).validate().expect("valid request"));
    });
}

fn text_streams(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("model/stream/text");
    for count in [100_usize, 1_000] {
        let events = (0..count)
            .map(|_| ChatStreamEvent::TextDelta("x".to_owned()))
            .chain([ChatStreamEvent::Finished(FinishReason::Stop)])
            .collect::<Vec<_>>();
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &events,
            |bencher, events| {
                bencher.iter_batched(
                    || events.clone(),
                    |events| {
                        black_box(block_on(collect_chat_stream(stream::iter(
                            events.into_iter().map(Ok),
                        ))))
                        .expect("valid stream")
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn tool_stream(criterion: &mut Criterion) {
    let mut events = Vec::new();
    for fragment in 0..2 {
        for index in (0..8).rev() {
            let delta = if fragment == 0 {
                ToolCallDelta::new(index)
                    .with_id(ToolCallId::new(format!("call-{index}")).expect("valid call id"))
                    .with_name(ToolName::new("lookup").expect("valid tool name"))
                    .with_arguments_fragment("{\"query\":\"")
            } else {
                ToolCallDelta::new(index).with_arguments_fragment("value\"}")
            };
            events.push(ChatStreamEvent::ToolCallDelta(delta));
        }
    }
    events.push(ChatStreamEvent::Finished(FinishReason::ToolCalls));

    criterion.bench_function("model/stream/8_interleaved_tool_calls", |bencher| {
        bencher.iter_batched(
            || events.clone(),
            |events| {
                black_box(block_on(collect_chat_stream(stream::iter(
                    events.into_iter().map(Ok),
                ))))
                .expect("valid stream")
            },
            BatchSize::SmallInput,
        );
    });
}

fn extension_merge(criterion: &mut Criterion) {
    let first = Extensions::new()
        .with("a", json!({"opaque": 1}))
        .expect("valid extension");
    let second = Extensions::new()
        .with("b", json!({"opaque": 2}))
        .expect("valid extension");
    let events = vec![
        ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(ToolCallId::new("call-0").expect("valid call id"))
                .with_extensions(first),
        ),
        ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_name(ToolName::new("lookup").expect("valid tool name"))
                .with_arguments_fragment("{}")
                .with_extensions(second),
        ),
        ChatStreamEvent::Finished(FinishReason::ToolCalls),
    ];

    criterion.bench_function("model/extensions/stream_merge_round_trip", |bencher| {
        bencher.iter_batched(
            || events.clone(),
            |events| {
                black_box(block_on(collect_chat_stream(stream::iter(
                    events.into_iter().map(Ok),
                ))))
                .expect("valid stream")
            },
            BatchSize::SmallInput,
        );
    });
}

fn atomic_tool_delta_merge(criterion: &mut Criterion) {
    let first = ChatStreamEvent::ToolCallDelta(
        ToolCallDelta::new(0)
            .with_id(ToolCallId::new("call-0").expect("valid call id"))
            .with_arguments_fragment("{\"query\":\"")
            .with_extensions(
                Extensions::new()
                    .with("continuation", json!({"part": 1}))
                    .expect("valid extension"),
            ),
    );
    let next = ChatStreamEvent::ToolCallDelta(
        ToolCallDelta::new(0)
            .with_name(ToolName::new("lookup").expect("valid tool name"))
            .with_arguments_fragment("value\"}")
            .with_extensions(
                Extensions::new()
                    .with("metadata", json!({"part": 2}))
                    .expect("valid extension"),
            ),
    );

    criterion.bench_function("model/stream/tool_delta_atomic_merge", |bencher| {
        bencher.iter_batched(
            || {
                let mut collector = ChatStreamCollector::new();
                collector.push(first.clone()).expect("base fragment");
                (collector, next.clone())
            },
            |(mut collector, event)| black_box(collector.push(event)).expect("atomic delta merge"),
            BatchSize::SmallInput,
        );
    });
}

fn large_usage_extension_merge(criterion: &mut Criterion) {
    let existing_extensions = Extensions::try_from_iter(
        (0..256).map(|index| (format!("existing.{index:03}"), json!({"value": index}))),
    )
    .expect("valid extensions");
    let existing = TokenUsage::from_parts(Some(100), Some(20), Some(200))
        .expect("valid usage")
        .with_extensions(existing_extensions);
    let next = TokenUsage::from_parts(None, Some(21), None)
        .expect("valid usage")
        .with_extensions(
            Extensions::new()
                .with("new.entry", json!({"value": 256}))
                .expect("valid extension"),
        );

    criterion.bench_function("model/usage/merge_1_into_256_extensions", |bencher| {
        bencher.iter_batched(
            || (existing.clone(), next.clone()),
            |(mut existing, next)| {
                black_box(existing.merge_snapshot(next)).expect("valid cumulative merge");
                existing
            },
            BatchSize::SmallInput,
        );
    });
}

fn atomic_extension_conflict(criterion: &mut Criterion) {
    let existing = Extensions::try_from_iter(
        (0..256).map(|index| (format!("existing.{index:03}"), json!({"value": index}))),
    )
    .expect("valid extensions");
    let conflicting = Extensions::try_from_iter([
        ("a-new", json!(1)),
        ("existing.128", json!("conflict")),
        ("z-new", json!(2)),
    ])
    .expect("valid extensions");

    criterion.bench_function("model/extensions/atomic_conflict_check_256", |bencher| {
        bencher.iter_batched(
            || (existing.clone(), conflicting.clone()),
            |(mut existing, conflicting)| {
                black_box(existing.merge_idempotent(conflicting))
                    .expect_err("conflict is expected");
                existing
            },
            BatchSize::SmallInput,
        );
    });
}

struct MockAdapter {
    metadata: ModelMetadata,
}

#[async_trait]
impl ChatModelAdapter for MockAdapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(
        &self,
        _request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        Ok(
            ChatResponse::new(AssistantMessage::text("ok"), FinishReason::Stop)
                .with_model(self.metadata.model().clone()),
        )
    }
}

fn validated_complete(criterion: &mut Criterion) {
    let adapter: Arc<dyn ChatModelAdapter> = Arc::new(MockAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("mock").expect("valid provider"),
            model_id(),
            ModelCapabilities::new(),
        ),
    });
    let model = ChatModel::new(adapter).expect("valid metadata");
    let request = ChatRequest::new(vec![Message::user("hello")]);
    criterion.bench_function("model/facade/validated_complete", |bencher| {
        bencher.iter_batched(
            || request.clone(),
            |request| black_box(block_on(model.complete(request))).expect("mock response"),
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    request_validation,
    text_streams,
    tool_stream,
    extension_merge,
    atomic_tool_delta_merge,
    large_usage_extension_merge,
    atomic_extension_conflict,
    validated_complete
);
criterion_main!(benches);
