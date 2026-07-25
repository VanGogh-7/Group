use std::io;

use genai::adapter::AdapterKind;
use genai::chat::{ChatResponse as GenaiResponse, ContentPart as GenaiContentPart, StopReason};
use group_agent_model::{
    AssistantMessage, ChatResponse, ContentPart, Extensions, FinishReason, ModelId, ResponseId,
    ToolCall, ToolCallId, ToolName,
};

use crate::GenaiMappingError;
use crate::config::GenaiAdapterConfig;
use crate::extensions::{
    ADAPTER_KIND, PROVIDER_MODEL, RAW_STOP_REASON, REASONING_CONTENT, RESOLVED_MODEL,
    THOUGHT_SIGNATURES, insert_string, insert_string_list,
};
use crate::usage::map_usage;

/// Maps one completed genai response without performing network I/O.
pub fn map_chat_response(
    mut response: GenaiResponse,
    config: &GenaiAdapterConfig,
) -> Result<ChatResponse, GenaiMappingError> {
    attach_openai_responses_continuation(&mut response, config)?;
    let GenaiResponse {
        content,
        reasoning_content,
        model_iden,
        provider_model_iden,
        stop_reason,
        usage,
        captured_raw_body: _,
        response_id,
    } = response;

    let mut text_parts = Vec::new();
    let mut tool_calls = Vec::new();
    let mut pending_signatures = Vec::new();
    let mut reasoning_parts = Vec::new();

    for part in content.into_parts() {
        match part {
            GenaiContentPart::Text(text) => text_parts.push(ContentPart::text(text)),
            GenaiContentPart::ToolCall(call) => {
                let signatures =
                    reconcile_signatures(&pending_signatures, call.thought_signatures.as_deref())?;
                pending_signatures.clear();
                tool_calls.push(map_tool_call(call, signatures)?);
            }
            GenaiContentPart::ThoughtSignature(signature) => {
                pending_signatures.push(signature);
            }
            GenaiContentPart::ReasoningContent(reasoning) => {
                reasoning_parts.push(reasoning);
            }
            GenaiContentPart::Binary(_) => {
                return Err(GenaiMappingError::UnsupportedResponseContent { kind: "Binary" });
            }
            GenaiContentPart::Custom(_) => {
                return Err(GenaiMappingError::UnsupportedResponseContent { kind: "Custom" });
            }
            GenaiContentPart::ToolResponse(_) => {
                return Err(GenaiMappingError::UnsupportedResponseContent {
                    kind: "ToolResponse in assistant response",
                });
            }
        }
    }

    if let Some(reasoning) = reasoning_content {
        reasoning_parts.push(reasoning);
    }

    let mut message_extensions = Extensions::new();
    let mut response_extensions = Extensions::new();
    if config.retain_reasoning_content() && !reasoning_parts.is_empty() {
        insert_string_list(
            &mut message_extensions,
            REASONING_CONTENT,
            reasoning_parts.clone(),
        )?;
        insert_string_list(&mut response_extensions, REASONING_CONTENT, reasoning_parts)?;
    }
    if !pending_signatures.is_empty() {
        insert_string_list(
            &mut response_extensions,
            THOUGHT_SIGNATURES,
            pending_signatures,
        )?;
    }

    let resolved_model = model_iden.model_name.as_str().to_owned();
    let provider_model = provider_model_iden.model_name.as_str().to_owned();
    insert_string(&mut response_extensions, RESOLVED_MODEL, resolved_model)?;
    insert_string(
        &mut response_extensions,
        PROVIDER_MODEL,
        provider_model.clone(),
    )?;
    insert_string(
        &mut response_extensions,
        ADAPTER_KIND,
        model_iden.adapter_kind.as_lower_str(),
    )?;

    let (finish_reason, raw_stop_reason) = map_finish_reason(stop_reason);
    insert_string(&mut response_extensions, RAW_STOP_REASON, raw_stop_reason)?;

    let message = AssistantMessage::new(text_parts, tool_calls).with_extensions(message_extensions);
    let mut mapped = ChatResponse::new(message, finish_reason).with_extensions(response_extensions);

    if let Some(usage) = map_usage(usage, config.retain_usage_details())? {
        mapped = mapped.with_usage(usage);
    }
    if let Some(response_id) = response_id {
        mapped = mapped.with_response_id(ResponseId::new(response_id).map_err(|source| {
            GenaiMappingError::InvalidIdentifier {
                field: "response_id",
                source,
            }
        })?);
    }
    mapped = mapped.with_model(ModelId::new(provider_model).map_err(|source| {
        GenaiMappingError::InvalidIdentifier {
            field: "provider_model",
            source,
        }
    })?);
    Ok(mapped)
}

#[derive(Debug)]
struct CapturedToolCall {
    call_id: String,
    name: String,
    arguments: serde_json::Value,
    signatures: Vec<String>,
}

fn attach_openai_responses_continuation(
    response: &mut GenaiResponse,
    config: &GenaiAdapterConfig,
) -> Result<(), GenaiMappingError> {
    let is_responses = matches!(response.model_iden.adapter_kind, AdapterKind::OpenAIResp);
    if !is_responses {
        response.captured_raw_body = None;
        return Ok(());
    }

    let has_tool_calls = response
        .content
        .iter()
        .any(|part| matches!(part, GenaiContentPart::ToolCall(_)));
    let Some(raw_body) = response.captured_raw_body.take() else {
        return if has_tool_calls {
            Err(GenaiMappingError::MissingResponsesRawBody)
        } else {
            Ok(())
        };
    };
    if !has_tool_calls {
        return Ok(());
    }

    measure_parser_admission(&raw_body, config.responses_parser_admission_limit(), 0)?;

    let raw_id = raw_body
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or(GenaiMappingError::InvalidResponsesRawField { field: "id" })?;
    if response.response_id.as_deref() != Some(raw_id) {
        return Err(GenaiMappingError::ConflictingResponsesIdentity);
    }
    let output = raw_body
        .get("output")
        .and_then(serde_json::Value::as_array)
        .ok_or(GenaiMappingError::InvalidResponsesRawField { field: "output" })?;

    let mut captured_calls = Vec::new();
    let mut pending_signatures = Vec::new();
    let mut signature_bytes = 0usize;
    for item in output {
        let item_type = item.get("type").and_then(serde_json::Value::as_str).ok_or(
            GenaiMappingError::InvalidResponsesRawField {
                field: "output[].type",
            },
        )?;
        match item_type {
            "reasoning" => {
                if let Some(signature) = item.get("encrypted_content") {
                    let signature =
                        signature
                            .as_str()
                            .ok_or(GenaiMappingError::InvalidResponsesRawField {
                                field: "output[].encrypted_content",
                            })?;
                    if signature.is_empty() {
                        return Err(GenaiMappingError::InvalidResponsesRawField {
                            field: "output[].encrypted_content",
                        });
                    }
                    push_distinct_signature(
                        &mut pending_signatures,
                        &mut signature_bytes,
                        signature,
                        config.streaming_limits(),
                    )?;
                }
            }
            "function_call" => {
                // Ordered encrypted reasoning belongs to the immediately
                // following function call. Crossing any other output item is
                // ambiguous and rejected by the wildcard arm below.
                let call_id = required_raw_string(item, "call_id")?;
                let name = required_raw_string(item, "name")?;
                let arguments = required_raw_string(item, "arguments")?;
                let arguments = serde_json::from_str(arguments)
                    .map_err(GenaiMappingError::InvalidResponsesToolArguments)?;
                captured_calls.push(CapturedToolCall {
                    call_id: call_id.to_owned(),
                    name: name.to_owned(),
                    arguments,
                    signatures: std::mem::take(&mut pending_signatures),
                });
            }
            _ if !pending_signatures.is_empty() => {
                return Err(GenaiMappingError::AmbiguousResponsesThoughtSignature);
            }
            _ => {}
        }
    }
    if !pending_signatures.is_empty() {
        return Err(GenaiMappingError::AmbiguousResponsesThoughtSignature);
    }

    let normalized_calls = response
        .content
        .iter_mut()
        .filter_map(|part| match part {
            GenaiContentPart::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect::<Vec<_>>();
    if normalized_calls.len() != captured_calls.len() {
        return Err(GenaiMappingError::ConflictingResponsesToolCall);
    }
    for (normalized, captured) in normalized_calls.into_iter().zip(captured_calls) {
        if normalized.call_id != captured.call_id
            || normalized.fn_name != captured.name
            || normalized.fn_arguments != captured.arguments
        {
            return Err(GenaiMappingError::ConflictingResponsesToolCall);
        }
        match (
            normalized.thought_signatures.as_deref(),
            captured.signatures.as_slice(),
        ) {
            (None | Some([]), []) => {}
            (None | Some([]), signatures) => {
                normalized.thought_signatures = Some(signatures.to_vec());
            }
            (Some(existing), signatures) if existing == signatures => {}
            (Some(_), _) => return Err(GenaiMappingError::ConflictingThoughtSignatures),
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionFailure {
    LimitExceeded,
    LengthOverflow,
}

struct AdmissionCountingWriter {
    written: usize,
    maximum: usize,
    failure: Option<AdmissionFailure>,
}

impl io::Write for AdmissionCountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(next) = self.written.checked_add(buffer.len()) else {
            self.failure = Some(AdmissionFailure::LengthOverflow);
            return Err(io::Error::other("parser admission length overflow"));
        };
        if next > self.maximum {
            self.failure = Some(AdmissionFailure::LimitExceeded);
            return Err(io::Error::other("parser admission limit exceeded"));
        }
        self.written = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn measure_parser_admission(
    value: &serde_json::Value,
    maximum: usize,
    initial: usize,
) -> Result<usize, GenaiMappingError> {
    let mut writer = AdmissionCountingWriter {
        written: initial,
        maximum,
        failure: None,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.written),
        Err(source) => match writer.failure {
            Some(AdmissionFailure::LimitExceeded) => {
                Err(GenaiMappingError::ResponsesParserAdmissionLimitExceeded { maximum, source })
            }
            Some(AdmissionFailure::LengthOverflow) => Err(
                GenaiMappingError::ResponsesParserAdmissionLengthOverflow(source),
            ),
            None => Err(GenaiMappingError::ResponsesParserAdmissionMeasurement(
                source,
            )),
        },
    }
}

fn push_distinct_signature(
    signatures: &mut Vec<String>,
    total_bytes: &mut usize,
    signature: &str,
    limits: crate::config::GenaiStreamingLimits,
) -> Result<(), GenaiMappingError> {
    if signatures.iter().any(|existing| existing == signature) {
        return Ok(());
    }
    if signatures.len() >= limits.max_thought_signatures_per_tool_call() {
        return Err(GenaiMappingError::ThoughtSignatureCountExceeded {
            maximum: limits.max_thought_signatures_per_tool_call(),
        });
    }
    let next = total_bytes
        .checked_add(signature.len())
        .ok_or(GenaiMappingError::ThoughtSignatureLengthOverflow)?;
    if next > limits.max_thought_signature_bytes() {
        return Err(GenaiMappingError::ThoughtSignatureLimitExceeded {
            maximum: limits.max_thought_signature_bytes(),
        });
    }
    *total_bytes = next;
    signatures.push(signature.to_owned());
    Ok(())
}

fn required_raw_string<'a>(
    item: &'a serde_json::Value,
    field: &'static str,
) -> Result<&'a str, GenaiMappingError> {
    item.get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or(GenaiMappingError::InvalidResponsesRawField { field })
}

pub(crate) fn map_response(
    response: GenaiResponse,
    config: &GenaiAdapterConfig,
) -> Result<ChatResponse, GenaiMappingError> {
    map_chat_response(response, config)
}

fn map_tool_call(
    call: genai::chat::ToolCall,
    signatures: Vec<String>,
) -> Result<ToolCall, GenaiMappingError> {
    let id =
        ToolCallId::new(call.call_id).map_err(|source| GenaiMappingError::InvalidIdentifier {
            field: "tool_call.call_id",
            source,
        })?;
    let name =
        ToolName::new(call.fn_name).map_err(|source| GenaiMappingError::InvalidIdentifier {
            field: "tool_call.fn_name",
            source,
        })?;
    let mut extensions = Extensions::new();
    if !signatures.is_empty() {
        insert_string_list(&mut extensions, THOUGHT_SIGNATURES, signatures)?;
    }
    Ok(ToolCall::new(id, name, call.fn_arguments).with_extensions(extensions))
}

pub(crate) fn map_finish_reason(reason: Option<StopReason>) -> (FinishReason, String) {
    match reason {
        Some(StopReason::Completed(raw)) | Some(StopReason::StopSequence(raw)) => {
            (FinishReason::Stop, raw)
        }
        Some(StopReason::MaxTokens(raw)) => (FinishReason::Length, raw),
        Some(StopReason::ToolCall(raw)) => (FinishReason::ToolCalls, raw),
        Some(StopReason::ContentFilter(raw)) => (FinishReason::ContentFilter, raw),
        Some(StopReason::Other(raw)) => (FinishReason::Other(raw.clone()), raw),
        None => (
            FinishReason::Other("unspecified".to_owned()),
            "unspecified".to_owned(),
        ),
    }
}

pub(crate) fn reconcile_signatures(
    streamed_or_ordered: &[String],
    attached: Option<&[String]>,
) -> Result<Vec<String>, GenaiMappingError> {
    match (streamed_or_ordered.is_empty(), attached) {
        (true, None | Some([])) => Ok(Vec::new()),
        (true, Some(attached)) => Ok(attached.to_vec()),
        (false, None | Some([])) => Ok(streamed_or_ordered.to_vec()),
        (false, Some(attached)) if streamed_or_ordered == attached => {
            Ok(streamed_or_ordered.to_vec())
        }
        (false, Some(_)) => Err(GenaiMappingError::ConflictingThoughtSignatures),
    }
}

#[cfg(test)]
mod tests {
    use genai::ModelIden;
    use genai::adapter::AdapterKind;
    use genai::chat::{
        ChatResponse as GenaiResponse, ContentPart as GenaiPart, MessageContent,
        ToolCall as GenaiToolCall, ToolResponse as GenaiToolResponse, Usage,
    };
    use group_agent_model::{ModelCapabilities, ModelId, ProviderId};
    use serde_json::json;

    use super::*;
    use crate::{GenaiAdapterConfig, GenaiModelConfig, GenaiStreamingLimits};

    fn config() -> GenaiAdapterConfig {
        let model = GenaiModelConfig::new(
            "test-model",
            ProviderId::new("test-provider").expect("provider"),
            ModelId::new("test-model").expect("model"),
            ModelCapabilities::new().with_tool_calling(true),
        )
        .expect("config");
        GenaiAdapterConfig::new(model).with_reasoning_content(true)
    }

    fn response(content: Vec<GenaiPart>) -> GenaiResponse {
        GenaiResponse {
            content: MessageContent::from_parts(content),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAI, "resolved-model"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "provider-model"),
            stop_reason: None,
            usage: Usage::default(),
            captured_raw_body: None,
            response_id: None,
        }
    }

    fn responses_response(
        content: Vec<GenaiPart>,
        raw: Option<serde_json::Value>,
    ) -> GenaiResponse {
        GenaiResponse {
            content: MessageContent::from_parts(content),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAIResp, "resolved-model"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAIResp, "provider-model"),
            stop_reason: None,
            usage: Usage::default(),
            captured_raw_body: raw,
            response_id: Some("resp-1".to_owned()),
        }
    }

    #[test]
    fn preserves_text_part_order_and_associates_signatures_once() {
        let mapped = map_response(
            response(vec![
                GenaiPart::Text("first".to_owned()),
                GenaiPart::Text(String::new()),
                GenaiPart::ThoughtSignature("sig".to_owned()),
                GenaiPart::ToolCall(GenaiToolCall {
                    call_id: "call".to_owned(),
                    fn_name: "tool".to_owned(),
                    fn_arguments: json!({"x":1}),
                    thought_signatures: Some(vec!["sig".to_owned()]),
                }),
            ]),
            &config(),
        )
        .expect("mapping");
        let parts: Vec<_> = mapped.message().text_parts().collect();
        assert_eq!(parts, ["first", ""]);
        assert_eq!(
            mapped.message().tool_calls()[0]
                .extensions()
                .get(THOUGHT_SIGNATURES),
            Some(&json!(["sig"]))
        );
    }

    #[test]
    fn rejects_unsupported_or_role_incompatible_content() {
        let cases = [
            GenaiPart::from_binary_base64("image/png", "AA==", None),
            GenaiPart::from_custom(json!({"type":"future"}), None),
            GenaiPart::ToolResponse(GenaiToolResponse::new("call", "output")),
        ];
        for part in cases {
            let error = map_response(response(vec![part]), &config()).expect_err("must reject");
            assert!(matches!(
                error,
                GenaiMappingError::UnsupportedResponseContent { .. }
            ));
        }
    }

    #[test]
    fn rejects_invalid_tool_identity_and_conflicting_signatures() {
        let empty_id = GenaiPart::ToolCall(GenaiToolCall {
            call_id: String::new(),
            fn_name: "tool".to_owned(),
            fn_arguments: json!({}),
            thought_signatures: None,
        });
        assert!(matches!(
            map_response(response(vec![empty_id]), &config()),
            Err(GenaiMappingError::InvalidIdentifier { .. })
        ));

        let conflicting = vec![
            GenaiPart::ThoughtSignature("first".to_owned()),
            GenaiPart::ToolCall(GenaiToolCall {
                call_id: "call".to_owned(),
                fn_name: "tool".to_owned(),
                fn_arguments: json!({}),
                thought_signatures: Some(vec!["second".to_owned()]),
            }),
        ];
        assert!(matches!(
            map_response(response(conflicting), &config()),
            Err(GenaiMappingError::ConflictingThoughtSignatures)
        ));
    }

    #[test]
    fn captures_responses_signature_from_the_restricted_raw_schema() {
        let call = GenaiPart::ToolCall(GenaiToolCall {
            call_id: "call-1".to_owned(),
            fn_name: "lookup".to_owned(),
            fn_arguments: json!({"query":"rust"}),
            thought_signatures: None,
        });
        let raw = json!({
            "id": "resp-1",
            "output": [
                {"type":"reasoning","encrypted_content":"signature-sentinel"},
                {
                    "type":"function_call",
                    "call_id":"call-1",
                    "name":"lookup",
                    "arguments":"{\"query\":\"rust\"}"
                }
            ]
        });
        let mapped =
            map_response(responses_response(vec![call], Some(raw)), &config()).expect("mapping");
        assert_eq!(
            mapped.message().tool_calls()[0]
                .extensions()
                .get(THOUGHT_SIGNATURES),
            Some(&json!(["signature-sentinel"]))
        );
        let debug = format!("{mapped:?}");
        assert!(!debug.contains("signature-sentinel"));
        assert!(!debug.contains("resp-1"));
    }

    #[test]
    fn responses_raw_failures_have_exact_decode_and_protocol_kinds() {
        let provider = ProviderId::new("provider").expect("provider");
        let model = ModelId::new("model").expect("model");
        let call = || {
            GenaiPart::ToolCall(GenaiToolCall {
                call_id: "call-1".to_owned(),
                fn_name: "lookup".to_owned(),
                fn_arguments: json!({"query":"rust"}),
                thought_signatures: None,
            })
        };

        let invalid_arguments = json!({
            "id":"resp-1",
            "output":[{
                "type":"function_call",
                "call_id":"call-1",
                "name":"lookup",
                "arguments":"{"
            }]
        });
        let error = map_response(
            responses_response(vec![call()], Some(invalid_arguments)),
            &config(),
        )
        .expect_err("invalid JSON")
        .into_model_error(&provider, &model);
        assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Decode);
        let mapping = std::error::Error::source(&error).expect("mapping source");
        assert!(
            std::error::Error::source(mapping).is_some(),
            "serde source must remain"
        );

        let mismatched_identity = json!({
            "id":"different-response",
            "output":[{
                "type":"function_call",
                "call_id":"call-1",
                "name":"lookup",
                "arguments":"{\"query\":\"rust\"}"
            }]
        });
        let error = map_response(
            responses_response(vec![call()], Some(mismatched_identity)),
            &config(),
        )
        .expect_err("identity mismatch")
        .into_model_error(&provider, &model);
        assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Protocol);

        let ambiguous_signature = json!({
            "id":"resp-1",
            "output":[
                {"type":"reasoning","encrypted_content":"secret"},
                {"type":"message","role":"assistant","content":[]},
                {
                    "type":"function_call",
                    "call_id":"call-1",
                    "name":"lookup",
                    "arguments":"{\"query\":\"rust\"}"
                }
            ]
        });
        let error = map_response(
            responses_response(vec![call()], Some(ambiguous_signature)),
            &config(),
        )
        .expect_err("ownership mismatch")
        .into_model_error(&provider, &model);
        assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Protocol);

        let error = map_response(responses_response(vec![call()], None), &config())
            .expect_err("raw body required")
            .into_model_error(&provider, &model);
        assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Protocol);
    }

    #[test]
    fn responses_parser_admission_counting_is_exact_and_retains_no_raw_value() {
        let sentinel = "raw-response-secret-sentinel";
        let call = GenaiPart::ToolCall(GenaiToolCall {
            call_id: "call-1".to_owned(),
            fn_name: "lookup".to_owned(),
            fn_arguments: json!({}),
            thought_signatures: None,
        });
        let raw = json!({
            "id":"resp-1",
            "output":[{
                "type":"function_call",
                "call_id":"call-1",
                "name":"lookup",
                "arguments":"{}",
                "unknown": sentinel
            }]
        });
        let serialized_length =
            measure_parser_admission(&raw, usize::MAX, 0).expect("measure without allocation");
        assert_eq!(
            measure_parser_admission(&raw, serialized_length + 1, 0).expect("below limit"),
            serialized_length
        );
        assert_eq!(
            measure_parser_admission(&raw, serialized_length, 0).expect("equal limit"),
            serialized_length
        );
        assert!(matches!(
            measure_parser_admission(&raw, serialized_length - 1, 0),
            Err(GenaiMappingError::ResponsesParserAdmissionLimitExceeded { .. })
        ));

        let mapped = map_response(
            responses_response(vec![call.clone()], Some(raw.clone())),
            &config().with_responses_parser_admission_limit(serialized_length),
        )
        .expect("admitted raw value maps");
        assert!(!format!("{mapped:?}").contains(sentinel));
        assert!(
            mapped
                .extensions()
                .iter()
                .all(|(_, value)| !value.to_string().contains(sentinel))
        );

        let config = config().with_responses_parser_admission_limit(1);
        let error = map_response(responses_response(vec![call], Some(raw)), &config)
            .expect_err("limit")
            .into_model_error(
                &ProviderId::new("provider").expect("provider"),
                &ModelId::new("model").expect("model"),
            );
        assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Decode);
        assert!(!format!("{error:?}").contains(sentinel));
        assert!(!error.to_string().contains(sentinel));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn responses_parser_admission_length_overflow_is_structured_decode() {
        let error = measure_parser_admission(&json!(null), usize::MAX, usize::MAX)
            .expect_err("checked length overflow")
            .into_model_error(
                &ProviderId::new("provider").expect("provider"),
                &ModelId::new("model").expect("model"),
            );
        assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Decode);
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn responses_signatures_are_deduplicated_per_call_in_provider_order() {
        let calls = vec![
            GenaiPart::ToolCall(GenaiToolCall {
                call_id: "call-1".to_owned(),
                fn_name: "first".to_owned(),
                fn_arguments: json!({}),
                thought_signatures: None,
            }),
            GenaiPart::ToolCall(GenaiToolCall {
                call_id: "call-2".to_owned(),
                fn_name: "second".to_owned(),
                fn_arguments: json!([]),
                thought_signatures: None,
            }),
        ];
        let raw = json!({
            "id":"resp-1",
            "output":[
                {"type":"reasoning","encrypted_content":"sig-a"},
                {"type":"reasoning","encrypted_content":"sig-a"},
                {"type":"reasoning","encrypted_content":"sig-b"},
                {"type":"function_call","call_id":"call-1","name":"first","arguments":"{}"},
                {"type":"reasoning","encrypted_content":"sig-a"},
                {"type":"function_call","call_id":"call-2","name":"second","arguments":"[]"}
            ]
        });
        let mapped =
            map_response(responses_response(calls, Some(raw)), &config()).expect("mapping");
        assert_eq!(
            mapped.message().tool_calls()[0]
                .extensions()
                .get(THOUGHT_SIGNATURES),
            Some(&json!(["sig-a", "sig-b"]))
        );
        assert_eq!(
            mapped.message().tool_calls()[1]
                .extensions()
                .get(THOUGHT_SIGNATURES),
            Some(&json!(["sig-a"])),
            "deduplication must not cross function-call boundaries"
        );
    }

    #[test]
    fn responses_signature_limits_and_checked_overflow_are_structured() {
        let mut signatures = Vec::new();
        let mut bytes = usize::MAX;
        let overflow = push_distinct_signature(
            &mut signatures,
            &mut bytes,
            "x",
            GenaiStreamingLimits::new().with_max_thought_signature_bytes(usize::MAX),
        );
        assert!(matches!(
            overflow,
            Err(GenaiMappingError::ThoughtSignatureLengthOverflow)
        ));
        assert!(signatures.is_empty());

        let mut signatures = vec!["first".to_owned()];
        let mut bytes = 5;
        let count = push_distinct_signature(
            &mut signatures,
            &mut bytes,
            "second",
            GenaiStreamingLimits::new().with_max_thought_signatures_per_tool_call(1),
        );
        assert!(matches!(
            count,
            Err(GenaiMappingError::ThoughtSignatureCountExceeded { maximum: 1 })
        ));
        assert_eq!(signatures, ["first"]);

        let mut signatures = Vec::new();
        let mut bytes = 0;
        let length = push_distinct_signature(
            &mut signatures,
            &mut bytes,
            "too-long",
            GenaiStreamingLimits::new().with_max_thought_signature_bytes(1),
        );
        assert!(matches!(
            length,
            Err(GenaiMappingError::ThoughtSignatureLimitExceeded { maximum: 1 })
        ));
        assert!(signatures.is_empty());
    }

    #[test]
    fn responses_call_correlation_conflicts_are_exact_protocol_errors() {
        let provider = ProviderId::new("provider").expect("provider");
        let model = ModelId::new("model").expect("model");
        let normalized = |id: &str, name: &str, arguments: serde_json::Value| {
            GenaiPart::ToolCall(GenaiToolCall {
                call_id: id.to_owned(),
                fn_name: name.to_owned(),
                fn_arguments: arguments,
                thought_signatures: None,
            })
        };
        let raw_call = |id: &str, name: &str, arguments: &str| {
            json!({
                "type":"function_call",
                "call_id":id,
                "name":name,
                "arguments":arguments
            })
        };
        let cases = [
            (
                vec![normalized("call-1", "lookup", json!({"x":1}))],
                vec![raw_call("different", "lookup", r#"{"x":1}"#)],
            ),
            (
                vec![normalized("call-1", "lookup", json!({"x":1}))],
                vec![raw_call("call-1", "different", r#"{"x":1}"#)],
            ),
            (
                vec![normalized("call-1", "lookup", json!({"x":1}))],
                vec![raw_call("call-1", "lookup", r#"{"x":2}"#)],
            ),
            (
                vec![
                    normalized("call-1", "first", json!({})),
                    normalized("call-2", "second", json!({})),
                ],
                vec![
                    raw_call("call-2", "second", "{}"),
                    raw_call("call-1", "first", "{}"),
                ],
            ),
        ];
        for (calls, output) in cases {
            let error = map_response(
                responses_response(calls, Some(json!({"id":"resp-1","output":output}))),
                &config(),
            )
            .expect_err("correlation mismatch")
            .into_model_error(&provider, &model);
            assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Protocol);
        }
    }

    #[test]
    fn responses_unowned_reasoning_and_empty_signature_fail_deterministically() {
        let call = || {
            GenaiPart::ToolCall(GenaiToolCall {
                call_id: "call-1".to_owned(),
                fn_name: "lookup".to_owned(),
                fn_arguments: json!({}),
                thought_signatures: None,
            })
        };
        let provider = ProviderId::new("provider").expect("provider");
        let model = ModelId::new("model").expect("model");

        for output in [
            json!([
                {"type":"function_call","call_id":"call-1","name":"lookup","arguments":"{}"},
                {"type":"reasoning","encrypted_content":"orphan"}
            ]),
            json!([
                {"type":"reasoning","encrypted_content":"orphan"},
                {"type":"unknown"},
                {"type":"function_call","call_id":"call-1","name":"lookup","arguments":"{}"}
            ]),
        ] {
            let error = map_response(
                responses_response(vec![call()], Some(json!({"id":"resp-1","output":output}))),
                &config(),
            )
            .expect_err("ambiguous ownership")
            .into_model_error(&provider, &model);
            assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Protocol);
        }

        let empty = json!({
            "id":"resp-1",
            "output":[
                {"type":"reasoning","encrypted_content":""},
                {"type":"function_call","call_id":"call-1","name":"lookup","arguments":"{}"}
            ]
        });
        let error = map_response(responses_response(vec![call()], Some(empty)), &config())
            .expect_err("empty signature")
            .into_model_error(&provider, &model);
        assert_eq!(error.kind(), &group_agent_model::ModelErrorKind::Decode);
    }
}
