use super::decode::parse_length;
use super::{CodecLimits, DecodeError, DecodeErrorKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRequest<'a> {
    pub parts: Vec<&'a [u8]>,
    pub consumed: usize,
}

#[derive(Clone, Debug)]
pub struct RequestDecoder {
    limits: CodecLimits,
}

impl RequestDecoder {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }

    pub fn decode<'a>(&self, input: &'a [u8]) -> Result<DecodedRequest<'a>, DecodeError> {
        if input.first() != Some(&b'*') {
            return self.invalid_shape(input);
        }
        let mut cursor = RequestCursor {
            input,
            position: 0,
            limits: self.limits,
        };
        cursor.expect_byte(b'*')?;
        let length_offset = cursor.position;
        let Some(length) = parse_length(cursor.line()?, false)
            .map_err(|kind| DecodeError::new(length_offset, kind))?
        else {
            return self.invalid_shape(input);
        };
        cursor.check_aggregate_limit(length, length_offset)?;

        let mut parts = Vec::with_capacity(length);
        for _ in 0..length {
            if cursor.peek_byte() != Some(b'$') {
                return self.invalid_shape(input);
            }
            cursor.expect_byte(b'$')?;
            let length_offset = cursor.position;
            let Some(length) = parse_length(cursor.line()?, false)
                .map_err(|kind| DecodeError::new(length_offset, kind))?
            else {
                return self.invalid_shape(input);
            };
            cursor.check_bulk_limit(length, length_offset)?;
            parts.push(cursor.framed_payload(length, length_offset)?);
        }
        Ok(DecodedRequest {
            parts,
            consumed: cursor.position,
        })
    }

    fn invalid_shape<'a>(&self, input: &'a [u8]) -> Result<DecodedRequest<'a>, DecodeError> {
        match super::Decoder::new(self.limits).decode(input) {
            Ok(decoded) => Err(DecodeError::new(
                decoded.consumed,
                DecodeErrorKind::InvalidCommandFraming,
            )),
            Err(error) => Err(error),
        }
    }
}

impl Default for RequestDecoder {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}

struct RequestCursor<'a> {
    input: &'a [u8],
    position: usize,
    limits: CodecLimits,
}

impl<'a> RequestCursor<'a> {
    fn peek_byte(&self) -> Option<u8> {
        self.input.get(self.position).copied()
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), DecodeError> {
        let offset = self.position;
        let actual = self
            .input
            .get(self.position)
            .copied()
            .ok_or_else(|| DecodeError::new(offset, DecodeErrorKind::IncompleteInput))?;
        if actual != expected {
            return Err(DecodeError::new(
                offset,
                DecodeErrorKind::UnknownPrefix(actual),
            ));
        }
        self.position += 1;
        Ok(())
    }

    fn line(&mut self) -> Result<&'a [u8], DecodeError> {
        let start = self.position;
        let mut index = start;
        while index < self.input.len() {
            match self.input[index] {
                b'\r' => {
                    let newline = index
                        .checked_add(1)
                        .ok_or_else(|| DecodeError::new(index, DecodeErrorKind::NumericOverflow))?;
                    let Some(byte) = self.input.get(newline) else {
                        return Err(DecodeError::new(
                            self.input.len(),
                            DecodeErrorKind::IncompleteInput,
                        ));
                    };
                    if *byte != b'\n' {
                        return Err(DecodeError::new(index, DecodeErrorKind::InvalidCrlf));
                    }
                    self.position = newline + 1;
                    return Ok(&self.input[start..index]);
                }
                b'\n' => {
                    return Err(DecodeError::new(index, DecodeErrorKind::InvalidCrlf));
                }
                _ => index += 1,
            }
        }
        Err(DecodeError::new(
            self.input.len(),
            DecodeErrorKind::IncompleteInput,
        ))
    }

    fn framed_payload(
        &mut self,
        length: usize,
        length_offset: usize,
    ) -> Result<&'a [u8], DecodeError> {
        let start = self.position;
        let end = start
            .checked_add(length)
            .ok_or_else(|| DecodeError::new(length_offset, DecodeErrorKind::NumericOverflow))?;
        let Some(first_terminator) = self.input.get(end) else {
            return Err(DecodeError::new(
                self.input.len(),
                DecodeErrorKind::IncompleteInput,
            ));
        };
        if *first_terminator != b'\r' {
            return Err(DecodeError::new(end, DecodeErrorKind::InvalidCrlf));
        }
        let newline = end
            .checked_add(1)
            .ok_or_else(|| DecodeError::new(length_offset, DecodeErrorKind::NumericOverflow))?;
        let Some(second_terminator) = self.input.get(newline) else {
            return Err(DecodeError::new(
                self.input.len(),
                DecodeErrorKind::IncompleteInput,
            ));
        };
        if *second_terminator != b'\n' {
            return Err(DecodeError::new(end, DecodeErrorKind::InvalidCrlf));
        }
        self.position = newline + 1;
        Ok(&self.input[start..end])
    }

    fn check_bulk_limit(&self, length: usize, offset: usize) -> Result<(), DecodeError> {
        if length > self.limits.max_bulk_len {
            Err(DecodeError::new(
                offset,
                DecodeErrorKind::BulkLimitExceeded {
                    len: length,
                    max: self.limits.max_bulk_len,
                },
            ))
        } else {
            Ok(())
        }
    }

    fn check_aggregate_limit(&self, length: usize, offset: usize) -> Result<(), DecodeError> {
        if length > self.limits.max_aggregate_len {
            Err(DecodeError::new(
                offset,
                DecodeErrorKind::AggregateLimitExceeded {
                    len: length,
                    max: self.limits.max_aggregate_len,
                },
            ))
        } else {
            Ok(())
        }
    }
}
