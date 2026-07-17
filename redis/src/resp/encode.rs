use super::decode::is_decimal;
use super::{CodecLimits, EncodeError, EncodeErrorKind, PathElement, RespValue};
use std::io::Write;

#[derive(Clone, Debug)]
pub struct Encoder {
    limits: CodecLimits,
}

impl Encoder {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }

    pub fn encode<W: Write>(&self, value: &RespValue, writer: &mut W) -> Result<(), EncodeError> {
        let mut path = Vec::new();
        self.validate(value, 0, &mut path)?;
        emit(value, writer, &mut path)
    }

    pub fn to_bytes(&self, value: &RespValue) -> Result<Vec<u8>, EncodeError> {
        let mut output = Vec::new();
        self.encode(value, &mut output)?;
        Ok(output)
    }

    fn validate(
        &self,
        value: &RespValue,
        depth: usize,
        path: &mut Vec<PathElement>,
    ) -> Result<(), EncodeError> {
        if depth > self.limits.max_depth {
            return Err(encode_error(
                path,
                EncodeErrorKind::DepthLimitExceeded {
                    depth,
                    max: self.limits.max_depth,
                },
            ));
        }
        match value {
            RespValue::SimpleString(data) if contains_newline(data) => {
                Err(encode_error(path, EncodeErrorKind::InvalidSimpleString))
            }
            RespValue::SimpleError(data) if contains_newline(data) => {
                Err(encode_error(path, EncodeErrorKind::InvalidSimpleError))
            }
            RespValue::BulkString(data) | RespValue::BulkError(data) => {
                self.validate_bulk_len(data.len(), path)
            }
            RespValue::VerbatimString { data, .. } => {
                let length = data.len().checked_add(4).ok_or_else(|| {
                    encode_error(
                        path,
                        EncodeErrorKind::BulkLimitExceeded {
                            len: usize::MAX,
                            max: self.limits.max_bulk_len,
                        },
                    )
                })?;
                self.validate_bulk_len(length, path)
            }
            RespValue::BigNumber(data) if !is_decimal(data) => {
                Err(encode_error(path, EncodeErrorKind::InvalidBigNumber))
            }
            RespValue::Array(values) => {
                self.validate_aggregate_len(values.len(), path)?;
                for (index, child) in values.iter().enumerate() {
                    path.push(PathElement::Array(index));
                    let result = self.validate(child, depth + 1, path);
                    path.pop();
                    result?;
                }
                Ok(())
            }
            RespValue::Map(entries) => {
                self.validate_aggregate_len(entries.len(), path)?;
                for (index, (key, value)) in entries.iter().enumerate() {
                    path.push(PathElement::MapKey(index));
                    let key_result = self.validate(key, depth + 1, path);
                    path.pop();
                    key_result?;

                    path.push(PathElement::MapValue(index));
                    let value_result = self.validate(value, depth + 1, path);
                    path.pop();
                    value_result?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn validate_bulk_len(&self, length: usize, path: &[PathElement]) -> Result<(), EncodeError> {
        if length > self.limits.max_bulk_len {
            Err(encode_error(
                path,
                EncodeErrorKind::BulkLimitExceeded {
                    len: length,
                    max: self.limits.max_bulk_len,
                },
            ))
        } else {
            Ok(())
        }
    }

    fn validate_aggregate_len(
        &self,
        length: usize,
        path: &[PathElement],
    ) -> Result<(), EncodeError> {
        if length > self.limits.max_aggregate_len {
            Err(encode_error(
                path,
                EncodeErrorKind::AggregateLimitExceeded {
                    len: length,
                    max: self.limits.max_aggregate_len,
                },
            ))
        } else {
            Ok(())
        }
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}

fn emit<W: Write>(
    value: &RespValue,
    writer: &mut W,
    path: &mut Vec<PathElement>,
) -> Result<(), EncodeError> {
    match value {
        RespValue::SimpleString(data) => emit_line(b'+', data, writer, path),
        RespValue::SimpleError(data) => emit_line(b'-', data, writer, path),
        RespValue::Integer(value) => emit_line(b':', value.to_string().as_bytes(), writer, path),
        RespValue::BulkString(data) => emit_bulk(b'$', data, writer, path),
        RespValue::NullBulkString => write_bytes(writer, b"$-1\r\n", path),
        RespValue::Array(values) => {
            emit_line(b'*', values.len().to_string().as_bytes(), writer, path)?;
            for (index, value) in values.iter().enumerate() {
                path.push(PathElement::Array(index));
                let result = emit(value, writer, path);
                path.pop();
                result?;
            }
            Ok(())
        }
        RespValue::NullArray => write_bytes(writer, b"*-1\r\n", path),
        RespValue::Null => write_bytes(writer, b"_\r\n", path),
        RespValue::Boolean(value) => {
            write_bytes(writer, if *value { b"#t\r\n" } else { b"#f\r\n" }, path)
        }
        RespValue::Double(value) => {
            let encoded = if value.is_nan() {
                "nan".to_owned()
            } else if *value == f64::INFINITY {
                "inf".to_owned()
            } else if *value == f64::NEG_INFINITY {
                "-inf".to_owned()
            } else {
                value.to_string()
            };
            emit_line(b',', encoded.as_bytes(), writer, path)
        }
        RespValue::BigNumber(data) => emit_line(b'(', data, writer, path),
        RespValue::BulkError(data) => emit_bulk(b'!', data, writer, path),
        RespValue::VerbatimString { format, data } => {
            let length = data.len() + 4;
            emit_line(b'=', length.to_string().as_bytes(), writer, path)?;
            write_bytes(writer, format, path)?;
            write_bytes(writer, b":", path)?;
            write_bytes(writer, data, path)?;
            write_bytes(writer, b"\r\n", path)
        }
        RespValue::Map(entries) => {
            emit_line(b'%', entries.len().to_string().as_bytes(), writer, path)?;
            for (index, (key, value)) in entries.iter().enumerate() {
                path.push(PathElement::MapKey(index));
                let key_result = emit(key, writer, path);
                path.pop();
                key_result?;

                path.push(PathElement::MapValue(index));
                let value_result = emit(value, writer, path);
                path.pop();
                value_result?;
            }
            Ok(())
        }
    }
}

fn emit_line<W: Write>(
    prefix: u8,
    data: &[u8],
    writer: &mut W,
    path: &[PathElement],
) -> Result<(), EncodeError> {
    write_bytes(writer, &[prefix], path)?;
    write_bytes(writer, data, path)?;
    write_bytes(writer, b"\r\n", path)
}

fn emit_bulk<W: Write>(
    prefix: u8,
    data: &[u8],
    writer: &mut W,
    path: &[PathElement],
) -> Result<(), EncodeError> {
    emit_line(prefix, data.len().to_string().as_bytes(), writer, path)?;
    write_bytes(writer, data, path)?;
    write_bytes(writer, b"\r\n", path)
}

fn write_bytes<W: Write>(
    writer: &mut W,
    bytes: &[u8],
    path: &[PathElement],
) -> Result<(), EncodeError> {
    writer.write_all(bytes).map_err(|error| EncodeError {
        path: path.to_vec(),
        kind: EncodeErrorKind::Io(error),
    })
}

fn encode_error(path: &[PathElement], kind: EncodeErrorKind) -> EncodeError {
    EncodeError {
        path: path.to_vec(),
        kind,
    }
}

fn contains_newline(bytes: &[u8]) -> bool {
    bytes.iter().any(|byte| matches!(byte, b'\r' | b'\n'))
}
