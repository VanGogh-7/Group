//! Stable extension keys owned by the genai adapter.
//!
//! Values are read only from the context documented for each key. Unknown
//! `group.genai.*` request keys are rejected, while keys owned by other
//! adapters are ignored and never forwarded to genai or HTTP.

use group_agent_model::Extensions;
use serde_json::{Value, json};

use crate::GenaiMappingError;

/// Prefix reserved by this adapter.
pub const PREFIX: &str = "group.genai.";

/// `Vec<String>` thought signatures on a tool call or response.
pub const THOUGHT_SIGNATURES: &str = "group.genai.thought_signatures";

/// `Vec<String>` reasoning segments on an assistant message or response.
pub const REASONING_CONTENT: &str = "group.genai.reasoning_content";

/// `serde_json::Value` prompt-token detail object on [`group_agent_model::TokenUsage`].
pub const PROMPT_TOKEN_DETAILS: &str = "group.genai.prompt_token_details";

/// `serde_json::Value` completion-token detail object on [`group_agent_model::TokenUsage`].
pub const COMPLETION_TOKEN_DETAILS: &str = "group.genai.completion_token_details";

/// Non-empty `String` used as genai `ChatRequest.previous_response_id`.
pub const PREVIOUS_RESPONSE_ID: &str = "group.genai.previous_response_id";

/// `bool` used as genai `ChatRequest.store`.
pub const STORE: &str = "group.genai.store";

/// Resolved model name reported by genai.
pub const RESOLVED_MODEL: &str = "group.genai.resolved_model";

/// Provider-reported model name returned by genai.
pub const PROVIDER_MODEL: &str = "group.genai.provider_model";

/// Lowercase genai adapter kind.
pub const ADAPTER_KIND: &str = "group.genai.adapter_kind";

/// Original provider stop-reason string, or `"unspecified"`.
pub const RAW_STOP_REASON: &str = "group.genai.raw_stop_reason";

pub(crate) fn validate_request_extensions(
    extensions: &Extensions,
    allow_response_id_continuation: bool,
) -> Result<(Option<String>, Option<bool>), GenaiMappingError> {
    let mut previous_response_id = None;
    let mut store = None;

    for (key, value) in extensions.iter() {
        if !key.starts_with(PREFIX) {
            continue;
        }
        match key {
            PREVIOUS_RESPONSE_ID => {
                if !allow_response_id_continuation {
                    return Err(GenaiMappingError::ResponseIdContinuationDisabled);
                }
                let value = value
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or(GenaiMappingError::InvalidExtensionType {
                        key: PREVIOUS_RESPONSE_ID,
                        expected: "non-empty string",
                    })?;
                previous_response_id = Some(value.to_owned());
            }
            STORE => {
                store = Some(
                    value
                        .as_bool()
                        .ok_or(GenaiMappingError::InvalidExtensionType {
                            key: STORE,
                            expected: "boolean",
                        })?,
                );
            }
            _ => {
                return Err(GenaiMappingError::UnknownRequestExtension {
                    key: key.to_owned(),
                });
            }
        }
    }

    Ok((previous_response_id, store))
}

pub(crate) fn string_list(
    extensions: &Extensions,
    key: &'static str,
) -> Result<Option<Vec<String>>, GenaiMappingError> {
    let Some(value) = extensions.get(key) else {
        return Ok(None);
    };
    let values = value
        .as_array()
        .ok_or(GenaiMappingError::InvalidExtensionType {
            key,
            expected: "array of strings",
        })?;
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        let value = value
            .as_str()
            .ok_or(GenaiMappingError::InvalidExtensionType {
                key,
                expected: "array of strings",
            })?;
        result.push(value.to_owned());
    }
    Ok(Some(result))
}

pub(crate) fn validate_context_extensions(
    extensions: &Extensions,
    allowed: &[&str],
) -> Result<(), GenaiMappingError> {
    for key in extensions.keys() {
        if key.starts_with(PREFIX) && !allowed.contains(&key) {
            return Err(GenaiMappingError::UnknownRequestExtension {
                key: key.to_owned(),
            });
        }
    }
    Ok(())
}

pub(crate) fn insert(
    extensions: &mut Extensions,
    key: &'static str,
    value: Value,
) -> Result<(), GenaiMappingError> {
    extensions
        .insert(key, value)
        .map_err(|source| GenaiMappingError::ExtensionConstruction { key, source })
}

pub(crate) fn insert_string(
    extensions: &mut Extensions,
    key: &'static str,
    value: impl Into<String>,
) -> Result<(), GenaiMappingError> {
    insert(extensions, key, json!(value.into()))
}

pub(crate) fn insert_string_list(
    extensions: &mut Extensions,
    key: &'static str,
    values: Vec<String>,
) -> Result<(), GenaiMappingError> {
    insert(extensions, key, json!(values))
}
