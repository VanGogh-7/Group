use std::fmt;

use async_trait::async_trait;
use genai::adapter::AdapterKind;
use genai::{ClientConfig, ModelSpec, ServiceTarget};
use group_agent_model::{
    ChatEventStream, ChatModelAdapter, ChatResponse, ModelError, ModelMetadata, ToolChoice,
    ValidatedChatRequest,
};

use crate::config::{GenaiAdapterConfig, GenaiStreamingPolicy};
use crate::error::map_genai_error;
use crate::request::map_request;
use crate::response::map_response;
use crate::stream::GenaiEventStream;
use crate::{GenaiAdapterConfigError, GenaiMappingError};

/// A reusable adapter backed by an application-configured genai client.
///
/// The adapter is immutable and contains no conversation state. Authentication,
/// endpoint selection, and model mapping remain properties of the injected
/// [`genai::Client`].
pub struct GenaiChatModelAdapter {
    client: genai::Client,
    config: GenaiAdapterConfig,
    metadata: ModelMetadata,
    bound_adapter_kind: Option<AdapterKind>,
    stable_target: Option<ServiceTarget>,
}

impl GenaiChatModelAdapter {
    /// Creates an adapter without reading environment variables or rebuilding
    /// the supplied client.
    pub fn new(
        client: genai::Client,
        config: GenaiAdapterConfig,
    ) -> Result<Self, GenaiAdapterConfigError> {
        Self::new_inner(client, config, None)
    }

    /// Creates an adapter whose requests use one immutable service target.
    ///
    /// This is the only construction path that enables non-streaming tool
    /// generation and OpenAI Responses signature recovery. `client_config`
    /// must bind the same adapter as `target` and must not contain a
    /// `ServiceTargetResolver`; consequently genai dispatches the supplied
    /// target without a second mutable resolution step. Target authentication
    /// and endpoint data are never exposed by this adapter's `Debug`.
    pub fn new_with_stable_target(
        client_config: ClientConfig,
        target: ServiceTarget,
        config: GenaiAdapterConfig,
    ) -> Result<Self, GenaiAdapterConfigError> {
        if client_config.service_target_resolver().is_some() {
            return Err(GenaiAdapterConfigError::StableTargetResolverUnsupported);
        }
        let Some(bound_adapter_kind) = client_config.adapter_kind() else {
            return Err(GenaiAdapterConfigError::StableTargetClientUnbound);
        };
        if bound_adapter_kind != target.model.adapter_kind {
            return Err(GenaiAdapterConfigError::StableTargetAdapterMismatch);
        }
        let client = genai::Client::builder().with_config(client_config).build();
        Self::new_inner(client, config, Some(target))
    }

    fn new_inner(
        client: genai::Client,
        config: GenaiAdapterConfig,
        stable_target: Option<ServiceTarget>,
    ) -> Result<Self, GenaiAdapterConfigError> {
        let metadata = config.model().metadata().clone();
        metadata
            .validate()
            .map_err(GenaiAdapterConfigError::InvalidMetadata)?;
        if metadata.capabilities().parallel_tool_calls() {
            return Err(GenaiAdapterConfigError::ParallelToolCallsUnsupported);
        }
        let bound_adapter_kind = client.adapter_kind();
        match config.streaming_policy() {
            GenaiStreamingPolicy::Disabled => {}
            GenaiStreamingPolicy::TextOnly | GenaiStreamingPolicy::AuditedTextOnly => {
                if !metadata.capabilities().streaming() {
                    return Err(GenaiAdapterConfigError::StreamingCapabilityMissing);
                }
                match bound_adapter_kind {
                    Some(AdapterKind::OpenAI) => {}
                    Some(adapter) => {
                        return Err(GenaiAdapterConfigError::StreamingAdapterUnsupported {
                            adapter: adapter.as_lower_str(),
                        });
                    }
                    None => return Err(GenaiAdapterConfigError::StreamingClientUnbound),
                }
            }
        }
        Ok(Self {
            client,
            config,
            metadata,
            bound_adapter_kind,
            stable_target,
        })
    }

    fn validate_stream_request(
        &self,
        request: &ValidatedChatRequest,
    ) -> Result<(), GenaiMappingError> {
        if matches!(
            self.config.streaming_policy(),
            GenaiStreamingPolicy::Disabled
        ) {
            return Err(GenaiMappingError::StreamingDisabled);
        }
        if request_may_produce_tool_call(request) {
            return Err(GenaiMappingError::ToolStreamingUnsupported);
        }
        Ok(())
    }

    /// Returns immutable adapter policy.
    #[must_use]
    pub const fn config(&self) -> &GenaiAdapterConfig {
        &self.config
    }

    fn dispatch_model_spec(&self) -> ModelSpec {
        self.stable_target.clone().map_or_else(
            || ModelSpec::from_name(self.config.model().requested_model()),
            ModelSpec::from_target,
        )
    }
}

impl fmt::Debug for GenaiChatModelAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenaiChatModelAdapter")
            .field("config", &self.config)
            .field(
                "bound_adapter_kind",
                &self
                    .bound_adapter_kind
                    .map(|adapter| adapter.as_lower_str()),
            )
            .field("stable_target", &self.stable_target.is_some())
            .field("client_configured", &true)
            .finish()
    }
}

#[async_trait]
impl ChatModelAdapter for GenaiChatModelAdapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        let may_produce_tool_call = request_may_produce_tool_call(&request);
        let capture_responses_continuation =
            matches!(self.bound_adapter_kind, Some(AdapterKind::OpenAIResp))
                && may_produce_tool_call;
        let mut mapped = map_request(request, &self.config).map_err(|error| {
            error.into_model_error(self.metadata.provider(), self.metadata.model())
        })?;
        if may_produce_tool_call && self.stable_target.is_none() {
            return Err(GenaiMappingError::UntrustedToolCallBinding
                .into_model_error(self.metadata.provider(), self.metadata.model()));
        }
        if capture_responses_continuation {
            mapped.options.capture_raw_body = Some(true);
            mapped.options.capture_reasoning_content = Some(true);
        }
        let response = self
            .client
            .exec_chat(
                self.dispatch_model_spec(),
                mapped.request,
                Some(&mapped.options),
            )
            .await
            .map_err(|error| {
                map_genai_error(error, self.metadata.provider(), self.metadata.model())
            })?;
        map_response(response, &self.config).map_err(|error| {
            error.into_model_error(self.metadata.provider(), self.metadata.model())
        })
    }

    async fn stream_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        self.validate_stream_request(&request).map_err(|error| {
            error.into_model_error(self.metadata.provider(), self.metadata.model())
        })?;
        let mapped = map_request(request, &self.config).map_err(|error| {
            error.into_model_error(self.metadata.provider(), self.metadata.model())
        })?;
        let response = self
            .client
            .exec_chat_stream(
                self.dispatch_model_spec(),
                mapped.request,
                Some(&mapped.options),
            )
            .await
            .map_err(|error| {
                map_genai_error(error, self.metadata.provider(), self.metadata.model())
            })?;
        // genai 0.6.5 resolves the ServiceTarget exactly once while creating
        // ChatStreamResponse. Its HTTP request remains lazy until this same
        // returned stream is polled, so checking its model_iden avoids a
        // resolver check/dispatch TOCTOU window.
        if !matches!(response.model_iden.adapter_kind, AdapterKind::OpenAI) {
            return Err(GenaiMappingError::ResolvedStreamingAdapterUnsupported {
                adapter: response.model_iden.adapter_kind.as_lower_str(),
            }
            .into_model_error(self.metadata.provider(), self.metadata.model()));
        }
        Ok(Box::pin(GenaiEventStream::new(
            response,
            self.config.clone(),
            self.metadata.provider().clone(),
            self.metadata.model().clone(),
        )))
    }
}

fn request_may_produce_tool_call(request: &ValidatedChatRequest) -> bool {
    !request.tools().is_empty()
        || matches!(
            request.tool_choice(),
            ToolChoice::Required | ToolChoice::Named(_)
        )
        || !matches!(request.tool_choice(), ToolChoice::Auto | ToolChoice::None)
}
