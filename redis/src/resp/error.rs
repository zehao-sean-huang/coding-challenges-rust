use std::fmt;
use std::io;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeError {
    pub offset: usize,
    pub kind: DecodeErrorKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeErrorKind {
    IncompleteInput,
    UnknownPrefix(u8),
    InvalidCrlf,
    InvalidLength,
    InvalidInteger,
    InvalidDouble,
    InvalidBigNumber,
    NumericOverflow,
    InvalidBoolean,
    InvalidNull,
    InvalidVerbatim,
    BulkLimitExceeded { len: usize, max: usize },
    AggregateLimitExceeded { len: usize, max: usize },
    DepthLimitExceeded { depth: usize, max: usize },
}

impl DecodeError {
    pub(crate) fn new(offset: usize, kind: DecodeErrorKind) -> Self {
        Self { offset, kind }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RESP decode error at byte {}: ", self.offset)?;
        match &self.kind {
            DecodeErrorKind::IncompleteInput => f.write_str("incomplete input"),
            DecodeErrorKind::UnknownPrefix(prefix) => {
                write!(f, "unknown prefix 0x{prefix:02x}")
            }
            DecodeErrorKind::InvalidCrlf => f.write_str("invalid CRLF framing"),
            DecodeErrorKind::InvalidLength => f.write_str("invalid length"),
            DecodeErrorKind::InvalidInteger => f.write_str("invalid integer"),
            DecodeErrorKind::InvalidDouble => f.write_str("invalid double"),
            DecodeErrorKind::InvalidBigNumber => f.write_str("invalid big number"),
            DecodeErrorKind::NumericOverflow => f.write_str("numeric overflow"),
            DecodeErrorKind::InvalidBoolean => f.write_str("invalid boolean"),
            DecodeErrorKind::InvalidNull => f.write_str("invalid null"),
            DecodeErrorKind::InvalidVerbatim => f.write_str("invalid verbatim string"),
            DecodeErrorKind::BulkLimitExceeded { len, max } => {
                write!(f, "bulk length {len} exceeds limit {max}")
            }
            DecodeErrorKind::AggregateLimitExceeded { len, max } => {
                write!(f, "aggregate length {len} exceeds limit {max}")
            }
            DecodeErrorKind::DepthLimitExceeded { depth, max } => {
                write!(f, "nesting depth {depth} exceeds limit {max}")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathElement {
    Array(usize),
    MapKey(usize),
    MapValue(usize),
}

#[derive(Debug)]
pub struct EncodeError {
    pub path: Vec<PathElement>,
    pub kind: EncodeErrorKind,
}

#[derive(Debug)]
pub enum EncodeErrorKind {
    InvalidSimpleString,
    InvalidSimpleError,
    InvalidBigNumber,
    BulkLimitExceeded { len: usize, max: usize },
    AggregateLimitExceeded { len: usize, max: usize },
    DepthLimitExceeded { depth: usize, max: usize },
    Io(io::Error),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RESP encode error at {:?}: ", self.path)?;
        match &self.kind {
            EncodeErrorKind::InvalidSimpleString => f.write_str("simple string contains CR or LF"),
            EncodeErrorKind::InvalidSimpleError => f.write_str("simple error contains CR or LF"),
            EncodeErrorKind::InvalidBigNumber => f.write_str("invalid big number"),
            EncodeErrorKind::BulkLimitExceeded { len, max } => {
                write!(f, "bulk length {len} exceeds limit {max}")
            }
            EncodeErrorKind::AggregateLimitExceeded { len, max } => {
                write!(f, "aggregate length {len} exceeds limit {max}")
            }
            EncodeErrorKind::DepthLimitExceeded { depth, max } => {
                write!(f, "nesting depth {depth} exceeds limit {max}")
            }
            EncodeErrorKind::Io(error) => write!(f, "I/O error: {error}"),
        }
    }
}

impl std::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            EncodeErrorKind::Io(error) => Some(error),
            _ => None,
        }
    }
}
