use super::{CodecLimits, DecodeError, DecodeErrorKind, Decoded, RespValue};

#[derive(Clone, Debug)]
pub struct Decoder {
    limits: CodecLimits,
}

impl Decoder {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }

    pub fn decode(&self, input: &[u8]) -> Result<Decoded, DecodeError> {
        let mut cursor = Cursor {
            input,
            position: 0,
            limits: self.limits,
        };
        let value = cursor.parse_value(0)?;
        Ok(Decoded {
            value,
            consumed: cursor.position,
        })
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}

struct Cursor<'a> {
    input: &'a [u8],
    position: usize,
    limits: CodecLimits,
}

impl Cursor<'_> {
    fn parse_value(&mut self, depth: usize) -> Result<RespValue, DecodeError> {
        if depth > self.limits.max_depth {
            return Err(self.error(DecodeErrorKind::DepthLimitExceeded {
                depth,
                max: self.limits.max_depth,
            }));
        }
        let prefix_offset = self.position;
        let prefix = self.take_byte()?;
        match prefix {
            b'+' => Ok(RespValue::SimpleString(self.line()?.to_vec())),
            b'-' => Ok(RespValue::SimpleError(self.line()?.to_vec())),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk(false),
            b'*' => self.parse_array(depth),
            b'_' => self.parse_null(),
            b'#' => self.parse_boolean(),
            b',' => self.parse_double(),
            b'(' => self.parse_big_number(),
            b'!' => self.parse_bulk(true),
            b'=' => self.parse_verbatim(),
            b'%' => self.parse_map(depth),
            other => Err(DecodeError::new(
                prefix_offset,
                DecodeErrorKind::UnknownPrefix(other),
            )),
        }
    }

    fn parse_integer(&mut self) -> Result<RespValue, DecodeError> {
        let offset = self.position;
        let line = self.line()?;
        let integer = parse_i64(line).map_err(|kind| DecodeError::new(offset, kind))?;
        Ok(RespValue::Integer(integer))
    }

    fn parse_bulk(&mut self, error: bool) -> Result<RespValue, DecodeError> {
        let length_offset = self.position;
        let line = self.line()?;
        let length =
            parse_length(line, error).map_err(|kind| DecodeError::new(length_offset, kind))?;
        let Some(length) = length else {
            return Ok(RespValue::NullBulkString);
        };
        self.check_bulk_limit(length, length_offset)?;
        let data = self.framed_payload(length, length_offset)?.to_vec();
        if error {
            Ok(RespValue::BulkError(data))
        } else {
            Ok(RespValue::BulkString(data))
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<RespValue, DecodeError> {
        let length_offset = self.position;
        let line = self.line()?;
        let length =
            parse_length(line, false).map_err(|kind| DecodeError::new(length_offset, kind))?;
        let Some(length) = length else {
            return Ok(RespValue::NullArray);
        };
        self.check_aggregate_limit(length, length_offset)?;
        let mut values = Vec::with_capacity(length);
        for _ in 0..length {
            values.push(self.parse_value(depth + 1)?);
        }
        Ok(RespValue::Array(values))
    }

    fn parse_null(&mut self) -> Result<RespValue, DecodeError> {
        let offset = self.position;
        if self.line()?.is_empty() {
            Ok(RespValue::Null)
        } else {
            Err(DecodeError::new(offset, DecodeErrorKind::InvalidNull))
        }
    }

    fn parse_boolean(&mut self) -> Result<RespValue, DecodeError> {
        let offset = self.position;
        match self.line()? {
            b"t" => Ok(RespValue::Boolean(true)),
            b"f" => Ok(RespValue::Boolean(false)),
            _ => Err(DecodeError::new(offset, DecodeErrorKind::InvalidBoolean)),
        }
    }

    fn parse_double(&mut self) -> Result<RespValue, DecodeError> {
        let offset = self.position;
        let line = self.line()?;
        let value = match line {
            b"inf" => f64::INFINITY,
            b"-inf" => f64::NEG_INFINITY,
            b"nan" => f64::NAN,
            _ => {
                if !is_double(line) {
                    return Err(DecodeError::new(offset, DecodeErrorKind::InvalidDouble));
                }
                let text = std::str::from_utf8(line)
                    .map_err(|_| DecodeError::new(offset, DecodeErrorKind::InvalidDouble))?;
                let value = text
                    .parse::<f64>()
                    .map_err(|_| DecodeError::new(offset, DecodeErrorKind::InvalidDouble))?;
                if !value.is_finite() {
                    return Err(DecodeError::new(offset, DecodeErrorKind::NumericOverflow));
                }
                value
            }
        };
        Ok(RespValue::Double(value))
    }

    fn parse_big_number(&mut self) -> Result<RespValue, DecodeError> {
        let offset = self.position;
        let line = self.line()?;
        if !is_decimal(line) {
            return Err(DecodeError::new(offset, DecodeErrorKind::InvalidBigNumber));
        }
        Ok(RespValue::BigNumber(line.to_vec()))
    }

    fn parse_verbatim(&mut self) -> Result<RespValue, DecodeError> {
        let length_offset = self.position;
        let line = self.line()?;
        let length = parse_length(line, true)
            .map_err(|kind| DecodeError::new(length_offset, kind))?
            .ok_or_else(|| DecodeError::new(length_offset, DecodeErrorKind::InvalidLength))?;
        self.check_bulk_limit(length, length_offset)?;
        let data_offset = self.position;
        let body = self.framed_payload(length, length_offset)?;
        if body.len() < 4 || body[3] != b':' {
            return Err(DecodeError::new(
                data_offset,
                DecodeErrorKind::InvalidVerbatim,
            ));
        }
        let format = [body[0], body[1], body[2]];
        let data = body[4..].to_vec();
        Ok(RespValue::VerbatimString { format, data })
    }

    fn parse_map(&mut self, depth: usize) -> Result<RespValue, DecodeError> {
        let length_offset = self.position;
        let line = self.line()?;
        let length = parse_length(line, true)
            .map_err(|kind| DecodeError::new(length_offset, kind))?
            .ok_or_else(|| DecodeError::new(length_offset, DecodeErrorKind::InvalidLength))?;
        self.check_aggregate_limit(length, length_offset)?;
        length
            .checked_mul(2)
            .ok_or_else(|| DecodeError::new(length_offset, DecodeErrorKind::NumericOverflow))?;
        let mut entries = Vec::with_capacity(length);
        for _ in 0..length {
            let key = self.parse_value(depth + 1)?;
            let value = self.parse_value(depth + 1)?;
            entries.push((key, value));
        }
        Ok(RespValue::Map(entries))
    }

    fn line(&mut self) -> Result<&[u8], DecodeError> {
        let start = self.position;
        let mut index = start;
        while index < self.input.len() {
            match self.input[index] {
                b'\r' => {
                    if index + 1 >= self.input.len() {
                        return Err(DecodeError::new(
                            self.input.len(),
                            DecodeErrorKind::IncompleteInput,
                        ));
                    }
                    if self.input[index + 1] != b'\n' {
                        return Err(DecodeError::new(index, DecodeErrorKind::InvalidCrlf));
                    }
                    self.position = index + 2;
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

    fn take_byte(&mut self) -> Result<u8, DecodeError> {
        let byte = self
            .input
            .get(self.position)
            .copied()
            .ok_or_else(|| DecodeError::new(self.position, DecodeErrorKind::IncompleteInput))?;
        self.position += 1;
        Ok(byte)
    }

    fn framed_payload(
        &mut self,
        length: usize,
        length_offset: usize,
    ) -> Result<&[u8], DecodeError> {
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

    fn error(&self, kind: DecodeErrorKind) -> DecodeError {
        DecodeError::new(self.position, kind)
    }
}

fn parse_i64(bytes: &[u8]) -> Result<i64, DecodeErrorKind> {
    if !is_decimal(bytes) {
        return Err(DecodeErrorKind::InvalidInteger);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| DecodeErrorKind::InvalidInteger)?;
    text.parse::<i64>()
        .map_err(|_| DecodeErrorKind::NumericOverflow)
}

fn parse_length(bytes: &[u8], forbid_null: bool) -> Result<Option<usize>, DecodeErrorKind> {
    if bytes == b"-1" {
        return if forbid_null {
            Err(DecodeErrorKind::InvalidLength)
        } else {
            Ok(None)
        };
    }
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(DecodeErrorKind::InvalidLength);
    }
    let mut value = 0usize;
    for digit in bytes {
        value = value
            .checked_mul(10)
            .and_then(|number| number.checked_add(usize::from(digit - b'0')))
            .ok_or(DecodeErrorKind::NumericOverflow)?;
    }
    Ok(Some(value))
}

pub(crate) fn is_decimal(bytes: &[u8]) -> bool {
    let digits = match bytes.first() {
        Some(b'+' | b'-') => &bytes[1..],
        Some(_) => bytes,
        None => return false,
    };
    !digits.is_empty() && digits.iter().all(u8::is_ascii_digit)
}

fn is_double(bytes: &[u8]) -> bool {
    let mut index = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let integral_start = index;
    while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
        index += 1;
    }
    if index == integral_start {
        return false;
    }

    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fractional_start = index;
        while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
            index += 1;
        }
        if index == fractional_start {
            return false;
        }
    }

    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
            index += 1;
        }
        if index == exponent_start {
            return false;
        }
    }

    index == bytes.len()
}
