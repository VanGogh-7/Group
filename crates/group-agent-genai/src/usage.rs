use genai::chat::Usage;
use group_agent_model::{Extensions, TokenUsage};

use crate::GenaiMappingError;
use crate::extensions::{COMPLETION_TOKEN_DETAILS, PROMPT_TOKEN_DETAILS, insert};

/// Maps independently optional genai counters and detail objects.
pub fn map_genai_usage(
    usage: Usage,
    retain_details: bool,
) -> Result<Option<TokenUsage>, GenaiMappingError> {
    let has_any = usage.prompt_tokens.is_some()
        || usage.completion_tokens.is_some()
        || usage.total_tokens.is_some()
        || usage.prompt_tokens_details.is_some()
        || usage.completion_tokens_details.is_some();
    if !has_any {
        return Ok(None);
    }

    let input = map_counter("prompt_tokens", usage.prompt_tokens)?;
    let output = map_counter("completion_tokens", usage.completion_tokens)?;
    let total = map_counter("total_tokens", usage.total_tokens)?;
    let mut extensions = Extensions::new();

    if retain_details {
        if let Some(details) = usage.prompt_tokens_details {
            let value = serde_json::to_value(details).map_err(|source| {
                GenaiMappingError::UsageDetailSerialization {
                    key: PROMPT_TOKEN_DETAILS,
                    source,
                }
            })?;
            insert(&mut extensions, PROMPT_TOKEN_DETAILS, value)?;
        }
        if let Some(details) = usage.completion_tokens_details {
            let value = serde_json::to_value(details).map_err(|source| {
                GenaiMappingError::UsageDetailSerialization {
                    key: COMPLETION_TOKEN_DETAILS,
                    source,
                }
            })?;
            insert(&mut extensions, COMPLETION_TOKEN_DETAILS, value)?;
        }
    }

    let usage = TokenUsage::from_parts(input, output, total)
        .map_err(GenaiMappingError::InvalidTokenUsage)?
        .with_extensions(extensions);
    Ok(Some(usage))
}

pub(crate) fn map_usage(
    usage: Usage,
    retain_details: bool,
) -> Result<Option<TokenUsage>, GenaiMappingError> {
    map_genai_usage(usage, retain_details)
}

fn map_counter(field: &'static str, value: Option<i32>) -> Result<Option<u64>, GenaiMappingError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| GenaiMappingError::NegativeTokenCount { field })
        })
        .transpose()
}
