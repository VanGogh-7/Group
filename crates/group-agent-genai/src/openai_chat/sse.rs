use crate::GenaiMappingError;

/// Incremental, bounded SSE framing. JSON is decoded only after a blank line.
/// Source: https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream
pub(super) struct Framer {
    line: Vec<u8>,
    data: String,
    event: String,
    first_line: bool,
    after_cr: bool,
    size: usize,
    maximum: usize,
}

impl Framer {
    pub(super) fn new(maximum: usize) -> Self {
        Self {
            line: Vec::new(),
            data: String::new(),
            event: String::new(),
            first_line: true,
            after_cr: false,
            size: 0,
            maximum,
        }
    }

    pub(super) fn push(&mut self, byte: u8) -> Result<Option<String>, GenaiMappingError> {
        if self.after_cr && byte == b'\n' {
            self.after_cr = false;
            return Ok(None);
        }
        self.after_cr = byte == b'\r';
        self.size = self
            .size
            .checked_add(1)
            .filter(|size| *size <= self.maximum)
            .ok_or(GenaiMappingError::OpenAiChatByteLimit {
                field: "SSE event",
                maximum: self.maximum,
            })?;
        if byte != b'\r' && byte != b'\n' {
            self.line.push(byte);
            return Ok(None);
        }
        let line = std::str::from_utf8(&self.line).map_err(GenaiMappingError::OpenAiChatUtf8)?;
        let line = if self.first_line {
            line.strip_prefix('\u{feff}').unwrap_or(line)
        } else {
            line
        };
        self.first_line = false;
        if line.is_empty() {
            self.size = 0;
            if !self.event.is_empty() && self.event != "message" {
                return Err(super::invalid("SSE event type"));
            }
            self.event.clear();
            self.line.clear();
            if self.data.is_empty() {
                return Ok(None);
            }
            self.data.pop(); // The final newline is added by SSE field processing.
            return Ok(Some(std::mem::take(&mut self.data)));
        }
        if !line.starts_with(':') {
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "data" => {
                    self.data.push_str(value);
                    self.data.push('\n');
                }
                "event" => {
                    self.event.clear();
                    self.event.push_str(value);
                }
                _ => {} // SSE id/retry and unknown fields do not initiate reconnection.
            }
        }
        self.line.clear();
        Ok(None)
    }
}
