#[cfg(feature = "structured-output")]
mod complete;
mod decode;
mod request;
mod sse;

use bytes::Buf;
use genai::{ClientConfig, ServiceTarget};
use group_agent_model::{
    ChatEventStream, ChatStreamEvent, ModelError, ModelErrorKind, Retryability,
};

use crate::{GenaiAdapterConfig, GenaiAdapterConfigError, GenaiMappingError, MappedChatRequest};

pub(crate) struct OpenAiChat {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    defaults: genai::chat::ChatOptions,
}

impl OpenAiChat {
    pub(crate) fn new(
        config: &ClientConfig,
        target: &ServiceTarget,
    ) -> Result<Self, GenaiAdapterConfigError> {
        request::validate_config(config, target)?;
        let endpoint = request::endpoint(target)?;
        let web = config.web_config().cloned().unwrap_or_default();
        let client = web
            .apply_to_builder(reqwest::Client::builder())
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|source| {
                GenaiAdapterConfigError::OpenAiChatClientBuild(Box::new(ModelError::with_source(
                    ModelErrorKind::InvalidRequest,
                    "HTTP client construction failed",
                    source,
                )))
            })?;
        Ok(Self {
            client,
            endpoint,
            defaults: config.chat_options().cloned().unwrap_or_default(),
        })
    }

    pub(crate) fn client(&self) -> reqwest::Client {
        self.client.clone()
    }

    pub(crate) fn stream(
        &self,
        target: &ServiceTarget,
        mapped: MappedChatRequest,
        config: GenaiAdapterConfig,
        #[cfg(feature = "structured-output")] output: Option<&group_agent_model::StructuredOutput>,
    ) -> Result<ChatEventStream, GenaiMappingError> {
        let request = request::build(
            &self.client,
            &self.endpoint,
            target,
            &self.defaults,
            mapped,
            true,
            #[cfg(feature = "structured-output")]
            output,
        )?;
        let state = StreamState {
            request: Some(request),
            response: None,
            bytes: bytes::Bytes::new(),
            framer: sse::Framer::new(config.streaming_limits().max_sse_event_bytes()),
            decoder: decode::Decoder::new(
                config.clone(),
                target.model.model_name.as_str().to_owned(),
                #[cfg(feature = "structured-output")]
                output.is_some(),
            ),
            config,
        };
        Ok(Box::pin(futures_util::stream::try_unfold(
            state,
            |mut state| async move {
                let event = state.next().await?;
                Ok(event.map(|event| (event, state)))
            },
        )))
    }
}

struct StreamState {
    request: Option<reqwest::RequestBuilder>,
    response: Option<reqwest::Response>,
    bytes: bytes::Bytes,
    framer: sse::Framer,
    decoder: decode::Decoder,
    config: GenaiAdapterConfig,
}

impl StreamState {
    async fn next(&mut self) -> Result<Option<ChatStreamEvent>, ModelError> {
        loop {
            if let Some(event) = self.decoder.pending.pop_front() {
                return Ok(Some(event));
            }
            if self.decoder.done {
                return Ok(None);
            }
            if let Some(request) = self.request.take() {
                let response = request
                    .send()
                    .await
                    .map_err(|source| self.http_error(source))?;
                if !response.status().is_success() {
                    let status = response.status();
                    let headers = response.headers().clone();
                    // Do not read, retain, or log a provider error body.
                    let source = genai::Error::WebModelCall {
                        model_iden: genai::ModelIden::new(
                            genai::adapter::AdapterKind::OpenAI,
                            self.config.model().requested_model(),
                        ),
                        webc_error: genai::webc::Error::ResponseFailedStatus {
                            status,
                            headers: Box::new(headers),
                            body: String::new(),
                        },
                    };
                    return Err(crate::error::map_genai_error(
                        source,
                        self.config.model().metadata().provider(),
                        self.config.model().metadata().model(),
                    ));
                }
                let content_type = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !content_type.eq_ignore_ascii_case("text/event-stream") {
                    return Err(self.mapping_error(invalid("Content-Type")));
                }
                self.response = Some(response);
            }
            while self.bytes.has_remaining() {
                let byte = self.bytes.get_u8();
                if let Some(data) = self.framer.push(byte).map_err(|e| self.mapping_error(e))? {
                    self.decoder
                        .push(&data)
                        .map_err(|e| self.mapping_error(e))?;
                    if self.decoder.done {
                        // [DONE] is the logical boundary; release the connection immediately.
                        self.response = None;
                        self.bytes = bytes::Bytes::new();
                    }
                    if !self.decoder.pending.is_empty() {
                        break;
                    }
                }
            }
            if !self.decoder.pending.is_empty() || self.decoder.done {
                continue;
            }
            let Some(response) = self.response.as_mut() else {
                return Err(self.mapping_error(GenaiMappingError::MissingStreamEnd));
            };
            let chunk = response
                .chunk()
                .await
                .map_err(|source| self.http_error(source))?;
            match chunk {
                Some(bytes) => self.bytes = bytes,
                None => return Err(self.mapping_error(GenaiMappingError::MissingStreamEnd)),
            }
        }
    }

    fn mapping_error(&self, error: GenaiMappingError) -> ModelError {
        error.into_model_error(
            self.config.model().metadata().provider(),
            self.config.model().metadata().model(),
        )
    }

    fn http_error(&self, source: reqwest::Error) -> ModelError {
        let kind = if source.is_timeout() {
            ModelErrorKind::Timeout
        } else if source.is_builder() {
            ModelErrorKind::InvalidRequest
        } else {
            ModelErrorKind::ProviderUnavailable
        };
        ModelError::with_source(kind, "OpenAI Chat transport failed", source)
            .with_model_context(
                self.config.model().metadata().provider().clone(),
                self.config.model().metadata().model().clone(),
            )
            .with_retryability(Retryability::Never)
    }
}

fn invalid(field: &'static str) -> GenaiMappingError {
    GenaiMappingError::InvalidOpenAiChatField { field }
}
