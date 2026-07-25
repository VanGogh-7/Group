use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_executor::block_on;
use futures_util::stream;
use genai::ModelIden;
use genai::adapter::AdapterKind;
use genai::chat::{
    ChatResponse as GenaiResponse, ContentPart as GenaiPart, MessageContent, StopReason,
    ToolCall as GenaiToolCall, Usage,
};
use group_agent_genai::extensions::THOUGHT_SIGNATURES;
use group_agent_genai::{
    GenaiAdapterConfig, GenaiModelConfig, map_chat_request, map_chat_response, map_genai_usage,
};
use group_agent_model::{
    AssistantMessage, ChatRequest, ChatStreamEvent, ContentPart, Extensions, FinishReason, Message,
    ModelCapabilities, ModelId, ProviderId, ToolCall, ToolCallDelta, ToolCallId, ToolDefinition,
    ToolName, collect_chat_stream,
};
use serde_json::json;

fn config() -> GenaiAdapterConfig {
    GenaiAdapterConfig::new(
        GenaiModelConfig::new(
            "benchmark-model",
            ProviderId::new("benchmark-provider").expect("provider"),
            ModelId::new("benchmark-model").expect("model"),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true)
                .with_usage_reporting(true),
        )
        .expect("config"),
    )
    .with_reasoning_content(true)
}

fn genai_response(content: Vec<GenaiPart>, usage: Usage) -> GenaiResponse {
    GenaiResponse {
        content: MessageContent::from_parts(content),
        reasoning_content: None,
        model_iden: ModelIden::new(AdapterKind::OpenAI, "resolved"),
        provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "provider"),
        stop_reason: Some(StopReason::Completed("stop".to_owned())),
        usage,
        captured_raw_body: None,
        response_id: None,
    }
}

fn request_mapping(criterion: &mut Criterion) {
    let config = config();
    let messages = ChatRequest::new(
        (0..100)
            .map(|index| Message::user(format!("message-{index}")))
            .collect(),
    );
    criterion.bench_function("genai/request/100_messages", |bencher| {
        bencher.iter_batched(
            || messages.clone(),
            |request| black_box(map_chat_request(request, &config)).expect("mapping"),
            BatchSize::SmallInput,
        );
    });

    let tools = ChatRequest::new(vec![Message::user("tools")]).with_tools(
        (0..32)
            .map(|index| {
                ToolDefinition::new(
                    ToolName::new(format!("tool_{index}")).expect("name"),
                    "description",
                    json!({"type":"object","properties":{"x":{"type":"string"}}}),
                )
            })
            .collect(),
    );
    criterion.bench_function("genai/request/32_tools", |bencher| {
        bencher.iter_batched(
            || tools.clone(),
            |request| black_box(map_chat_request(request, &config)).expect("mapping"),
            BatchSize::SmallInput,
        );
    });

    let calls = ChatRequest::new(vec![Message::Assistant(AssistantMessage::new(
        vec![ContentPart::text("text")],
        (0..8)
            .map(|index| {
                ToolCall::new(
                    ToolCallId::new(format!("call-{index}")).expect("id"),
                    ToolName::new(format!("tool_{index}")).expect("name"),
                    json!({"index":index}),
                )
            })
            .collect(),
    ))]);
    criterion.bench_function("genai/request/8_assistant_tool_calls", |bencher| {
        bencher.iter_batched(
            || calls.clone(),
            |request| black_box(map_chat_request(request, &config)).expect("mapping"),
            BatchSize::SmallInput,
        );
    });
}

fn response_mapping(criterion: &mut Criterion) {
    let config = config();
    let text = genai_response(vec![GenaiPart::Text("x".repeat(1_000))], Usage::default());
    criterion.bench_function("genai/response/1000_byte_text", |bencher| {
        bencher.iter_batched(
            || text.clone(),
            |response| black_box(map_chat_response(response, &config)).expect("mapping"),
            BatchSize::SmallInput,
        );
    });

    let calls = genai_response(
        (0..8)
            .map(|index| {
                GenaiPart::ToolCall(GenaiToolCall {
                    call_id: format!("call-{index}"),
                    fn_name: format!("tool_{index}"),
                    fn_arguments: json!({"index":index}),
                    thought_signatures: None,
                })
            })
            .collect(),
        Usage::default(),
    );
    criterion.bench_function("genai/response/8_tool_calls", |bencher| {
        bencher.iter_batched(
            || calls.clone(),
            |response| black_box(map_chat_response(response, &config)).expect("mapping"),
            BatchSize::SmallInput,
        );
    });

    let usage = Usage {
        prompt_tokens: Some(100),
        completion_tokens: None,
        total_tokens: Some(120),
        ..Usage::default()
    };
    criterion.bench_function("genai/usage/partial", |bencher| {
        bencher.iter_batched(
            || usage.clone(),
            |usage| black_box(map_genai_usage(usage, true)).expect("mapping"),
            BatchSize::SmallInput,
        );
    });
}

fn stream_projection(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("genai/stream/text_projection");
    for count in [100_usize, 1_000] {
        let chunks = (0..count).map(|_| "x".to_owned()).collect::<Vec<_>>();
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &chunks,
            |bencher, chunks| {
                bencher.iter_batched(
                    || chunks.clone(),
                    |chunks| {
                        chunks
                            .into_iter()
                            .map(ChatStreamEvent::TextDelta)
                            .collect::<Vec<_>>()
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();

    let mut events = Vec::new();
    for fragment in 0..2 {
        for index in (0..8).rev() {
            let delta = if fragment == 0 {
                ToolCallDelta::new(index)
                    .with_id(ToolCallId::new(format!("call-{index}")).expect("id"))
                    .with_name(ToolName::new(format!("tool_{index}")).expect("name"))
                    .with_arguments_fragment("{\"value\":")
            } else {
                ToolCallDelta::new(index).with_arguments_fragment("1}")
            };
            events.push(ChatStreamEvent::ToolCallDelta(delta));
        }
    }
    events.push(ChatStreamEvent::Finished(FinishReason::ToolCalls));
    criterion.bench_function("genai/stream/8_interleaved_tool_calls", |bencher| {
        bencher.iter_batched(
            || events.clone(),
            |events| {
                black_box(block_on(collect_chat_stream(stream::iter(
                    events.into_iter().map(Ok),
                ))))
                .expect("stream")
            },
            BatchSize::SmallInput,
        );
    });
}

fn thought_signature_round_trip(criterion: &mut Criterion) {
    let config = config();
    let extensions = Extensions::new()
        .with(THOUGHT_SIGNATURES, json!(["signature"]))
        .expect("extension");
    let request = ChatRequest::new(vec![Message::Assistant(AssistantMessage::new(
        Vec::new(),
        vec![
            ToolCall::new(
                ToolCallId::new("call").expect("id"),
                ToolName::new("tool").expect("name"),
                json!({}),
            )
            .with_extensions(extensions),
        ],
    ))]);
    criterion.bench_function("genai/continuation/thought_signature", |bencher| {
        bencher.iter_batched(
            || request.clone(),
            |request| black_box(map_chat_request(request, &config)).expect("mapping"),
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    request_mapping,
    response_mapping,
    stream_projection,
    thought_signature_round_trip
);
criterion_main!(benches);
