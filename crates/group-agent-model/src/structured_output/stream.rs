use super::{MAX_TEXT, StructuredOutput, StructuredOutputError};
use crate::{ChatEventStream, ChatStreamCollector, ChatStreamEvent};
use futures_util::StreamExt;

pub(crate) fn validate(raw: ChatEventStream, output: StructuredOutput) -> ChatEventStream {
    let state = Some((
        raw,
        ChatStreamCollector::new().with_max_text_bytes(MAX_TEXT),
        output,
        None,
        0usize,
    ));
    Box::pin(
        futures_util::stream::unfold(state, |state| async move {
            let (mut raw, mut collector, output, mut terminal, mut bytes) = state?;
            loop {
                match raw.next().await {
                    Some(Err(error)) => return Some((Err(error), None)),
                    Some(Ok(event)) => {
                        if terminal.is_none()
                            && let ChatStreamEvent::TextDelta(text) = &event
                        {
                            let Some(total) =
                                bytes.checked_add(text.len()).filter(|n| *n <= MAX_TEXT)
                            else {
                                return Some((
                                    Err(StructuredOutputError::LimitExceeded.into_model_error()),
                                    None,
                                ));
                            };
                            bytes = total;
                        }
                        if let Err(error) = collector.push(event.clone()) {
                            return Some((Err(error), None));
                        }
                        if let ChatStreamEvent::Finished(reason) = event {
                            terminal = Some(reason);
                        } else {
                            return Some((
                                Ok(event),
                                Some((raw, collector, output, terminal, bytes)),
                            ));
                        }
                    }
                    None => {
                        let result = collector.finish().and_then(|response| {
                            output
                                .validate_response(&response)
                                .map_err(StructuredOutputError::into_model_error)?;
                            Ok(ChatStreamEvent::Finished(
                                terminal.expect("collector verified terminal"),
                            ))
                        });
                        return Some((result, None));
                    }
                }
            }
        })
        .fuse(),
    )
}
