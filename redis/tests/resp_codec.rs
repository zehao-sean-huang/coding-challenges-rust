use proptest::collection::vec;
use proptest::prelude::*;
use redis::resp::{
    CodecLimits, DecodeErrorKind, Decoder, EncodeErrorKind, Encoder, PathElement, RespValue,
};
use std::io::{self, Write};

fn decoder() -> Decoder {
    Decoder::new(CodecLimits::default())
}

#[test]
fn decodes_every_supported_prefix() {
    let cases: &[(&[u8], RespValue)] = &[
        (b"+OK\r\n", RespValue::SimpleString(b"OK".to_vec())),
        (
            b"-ERR nope\r\n",
            RespValue::SimpleError(b"ERR nope".to_vec()),
        ),
        (b":-42\r\n", RespValue::Integer(-42)),
        (
            b"$4\r\na\r\n\0\r\n",
            RespValue::BulkString(b"a\r\n\0".to_vec()),
        ),
        (b"$-1\r\n", RespValue::NullBulkString),
        (
            b"*2\r\n+one\r\n:2\r\n",
            RespValue::Array(vec![
                RespValue::SimpleString(b"one".to_vec()),
                RespValue::Integer(2),
            ]),
        ),
        (b"*-1\r\n", RespValue::NullArray),
        (b"_\r\n", RespValue::Null),
        (b"#t\r\n", RespValue::Boolean(true)),
        (b",1.25e2\r\n", RespValue::Double(125.0)),
        (
            b"(3492890328409238509324850943850943825024385\r\n",
            RespValue::BigNumber(b"3492890328409238509324850943850943825024385".to_vec()),
        ),
        (b"!4\r\noops\r\n", RespValue::BulkError(b"oops".to_vec())),
        (
            b"=15\r\ntxt:Some string\r\n",
            RespValue::VerbatimString {
                format: *b"txt",
                data: b"Some string".to_vec(),
            },
        ),
        (
            b"%2\r\n+first\r\n:1\r\n:9\r\n+non-string key\r\n",
            RespValue::Map(vec![
                (
                    RespValue::SimpleString(b"first".to_vec()),
                    RespValue::Integer(1),
                ),
                (
                    RespValue::Integer(9),
                    RespValue::SimpleString(b"non-string key".to_vec()),
                ),
            ]),
        ),
    ];

    for (wire, expected) in cases {
        let decoded = decoder().decode(wire).unwrap();
        assert_eq!(&decoded.value, expected, "wire: {wire:?}");
        assert_eq!(decoded.consumed, wire.len(), "wire: {wire:?}");
    }
}

#[test]
fn default_limits_match_the_protocol_budget() {
    assert_eq!(
        CodecLimits::default(),
        CodecLimits {
            max_bulk_len: 536_870_912,
            max_aggregate_len: 1_000_000,
            max_depth: 128,
        }
    );
}

#[test]
fn decodes_numeric_edges_empty_values_and_special_doubles() {
    let cases = [
        (
            b":-9223372036854775808\r\n".as_slice(),
            RespValue::Integer(i64::MIN),
        ),
        (
            b":+9223372036854775807\r\n".as_slice(),
            RespValue::Integer(i64::MAX),
        ),
        (b"+\r\n".as_slice(), RespValue::SimpleString(Vec::new())),
        (b"$0\r\n\r\n".as_slice(), RespValue::BulkString(Vec::new())),
        (b"*0\r\n".as_slice(), RespValue::Array(Vec::new())),
        (b"%0\r\n".as_slice(), RespValue::Map(Vec::new())),
        (b",inf\r\n".as_slice(), RespValue::Double(f64::INFINITY)),
        (
            b",-inf\r\n".as_slice(),
            RespValue::Double(f64::NEG_INFINITY),
        ),
    ];
    for (wire, expected) in cases {
        assert_eq!(decoder().decode(wire).unwrap().value, expected);
    }
    match decoder().decode(b",nan\r\n").unwrap().value {
        RespValue::Double(value) => assert!(value.is_nan()),
        value => panic!("expected double, got {value:?}"),
    }
}

#[test]
fn leaves_pipelined_input_untouched() {
    let input = b"+first\r\n:2\r\n";
    let first = decoder().decode(input).unwrap();
    assert_eq!(first.value, RespValue::SimpleString(b"first".to_vec()));
    assert_eq!(&input[first.consumed..], b":2\r\n");
}

#[test]
fn every_truncated_golden_value_is_incomplete() {
    let fixtures: &[&[u8]] = &[
        b"+OK\r\n",
        b"-ERR\r\n",
        b":42\r\n",
        b"$3\r\na\r\n\r\n",
        b"$-1\r\n",
        b"*2\r\n+one\r\n:2\r\n",
        b"*-1\r\n",
        b"_\r\n",
        b"#f\r\n",
        b",-1.2e-3\r\n",
        b"(-999999999999999999999999\r\n",
        b"!3\r\nERR\r\n",
        b"=7\r\ntxt:abc\r\n",
        b"%1\r\n+key\r\n+value\r\n",
    ];
    for fixture in fixtures {
        for end in 0..fixture.len() {
            let error = decoder().decode(&fixture[..end]).unwrap_err();
            assert_eq!(
                error.kind,
                DecodeErrorKind::IncompleteInput,
                "fixture {fixture:?} truncated at {end}: {error:?}"
            );
        }
        assert_eq!(decoder().decode(fixture).unwrap().consumed, fixture.len());
    }
}

#[test]
fn rejects_malformed_framing_and_values_with_structured_kinds() {
    let cases: &[(&[u8], DecodeErrorKind)] = &[
        (b"?wat\r\n", DecodeErrorKind::UnknownPrefix(b'?')),
        (b"+OK\n", DecodeErrorKind::InvalidCrlf),
        (b"+OK\rX", DecodeErrorKind::InvalidCrlf),
        (b":\r\n", DecodeErrorKind::InvalidInteger),
        (b":1.0\r\n", DecodeErrorKind::InvalidInteger),
        (
            b":9223372036854775808\r\n",
            DecodeErrorKind::NumericOverflow,
        ),
        (b"$-2\r\n", DecodeErrorKind::InvalidLength),
        (b"*-2\r\n", DecodeErrorKind::InvalidLength),
        (b"!-1\r\n", DecodeErrorKind::InvalidLength),
        (b"%-1\r\n", DecodeErrorKind::InvalidLength),
        (
            b"$184467440737095516160\r\n",
            DecodeErrorKind::NumericOverflow,
        ),
        (b"$1\r\naXX", DecodeErrorKind::InvalidCrlf),
        (b"$1\r\na\n", DecodeErrorKind::InvalidCrlf),
        (b"!1\r\na\n", DecodeErrorKind::InvalidCrlf),
        (b"_x\r\n", DecodeErrorKind::InvalidNull),
        (b"#T\r\n", DecodeErrorKind::InvalidBoolean),
        (b"#true\r\n", DecodeErrorKind::InvalidBoolean),
        (b",NaN\r\n", DecodeErrorKind::InvalidDouble),
        (b",.5\r\n", DecodeErrorKind::InvalidDouble),
        (b",1.\r\n", DecodeErrorKind::InvalidDouble),
        (b",1e\r\n", DecodeErrorKind::InvalidDouble),
        (b",1e+\r\n", DecodeErrorKind::InvalidDouble),
        (b",1.2.3\r\n", DecodeErrorKind::InvalidDouble),
        (b",1e9999\r\n", DecodeErrorKind::NumericOverflow),
        (b"(\r\n", DecodeErrorKind::InvalidBigNumber),
        (b"(+12x\r\n", DecodeErrorKind::InvalidBigNumber),
        (b"=3\r\ntxt\r\n", DecodeErrorKind::InvalidVerbatim),
        (b"=4\r\ntxt!\r\n", DecodeErrorKind::InvalidVerbatim),
        (b"=4\r\ntxt:\n", DecodeErrorKind::InvalidCrlf),
    ];
    for (wire, expected_kind) in cases {
        let error = decoder().decode(wire).unwrap_err();
        assert_eq!(&error.kind, expected_kind, "wire: {wire:?}, error: {error}");
        assert!(error.to_string().contains(&error.offset.to_string()));
    }
}

#[test]
fn enforces_decode_payload_aggregate_and_depth_limits() {
    let limited = Decoder::new(CodecLimits {
        max_bulk_len: 3,
        max_aggregate_len: 1,
        max_depth: 1,
    });

    assert!(limited.decode(b"$3\r\nabc\r\n").is_ok());
    assert_eq!(
        limited.decode(b"$4\r\n").unwrap_err().kind,
        DecodeErrorKind::BulkLimitExceeded { len: 4, max: 3 }
    );
    assert!(limited.decode(b"*1\r\n+ok\r\n").is_ok());
    assert_eq!(
        limited.decode(b"*2\r\n").unwrap_err().kind,
        DecodeErrorKind::AggregateLimitExceeded { len: 2, max: 1 }
    );
    assert!(limited.decode(b"*1\r\n*0\r\n").is_ok());
    assert_eq!(
        limited
            .decode(b"*1\r\n*1\r\n+too-deep\r\n")
            .unwrap_err()
            .kind,
        DecodeErrorKind::DepthLimitExceeded { depth: 2, max: 1 }
    );
    assert_eq!(
        limited.decode(b"%2\r\n").unwrap_err().kind,
        DecodeErrorKind::AggregateLimitExceeded { len: 2, max: 1 }
    );
}

#[test]
fn encodes_every_supported_value_and_preserves_map_order_and_duplicates() {
    let encoder = Encoder::default();
    let cases: &[(RespValue, &[u8])] = &[
        (RespValue::SimpleString(b"OK".to_vec()), b"+OK\r\n"),
        (RespValue::SimpleError(b"ERR".to_vec()), b"-ERR\r\n"),
        (RespValue::Integer(i64::MIN), b":-9223372036854775808\r\n"),
        (
            RespValue::BulkString(b"a\r\n\0".to_vec()),
            b"$4\r\na\r\n\0\r\n",
        ),
        (RespValue::NullBulkString, b"$-1\r\n"),
        (
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Boolean(false)]),
            b"*2\r\n:1\r\n#f\r\n",
        ),
        (RespValue::NullArray, b"*-1\r\n"),
        (RespValue::Null, b"_\r\n"),
        (RespValue::Boolean(true), b"#t\r\n"),
        (RespValue::Double(f64::INFINITY), b",inf\r\n"),
        (RespValue::Double(f64::NEG_INFINITY), b",-inf\r\n"),
        (RespValue::Double(f64::NAN), b",nan\r\n"),
        (RespValue::Double(125.5), b",125.5\r\n"),
        (
            RespValue::BigNumber(b"+3492890328409238509324850943850943825024385".to_vec()),
            b"(+3492890328409238509324850943850943825024385\r\n",
        ),
        (RespValue::BulkError(b"oops".to_vec()), b"!4\r\noops\r\n"),
        (
            RespValue::VerbatimString {
                format: *b"txt",
                data: b"Some string".to_vec(),
            },
            b"=15\r\ntxt:Some string\r\n",
        ),
        (
            RespValue::Map(vec![
                (
                    RespValue::Integer(1),
                    RespValue::SimpleString(b"a".to_vec()),
                ),
                (RespValue::Integer(1), RespValue::Array(Vec::new())),
            ]),
            b"%2\r\n:1\r\n+a\r\n:1\r\n*0\r\n",
        ),
    ];
    for (value, expected) in cases {
        assert_eq!(
            encoder.to_bytes(value).unwrap(),
            *expected,
            "value: {value:?}"
        );
    }
}

#[derive(Default)]
struct RecordingWriter {
    bytes: Vec<u8>,
}

impl Write for RecordingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn semantic_encode_errors_write_nothing_and_report_aggregate_paths() {
    let invalid_values = [
        RespValue::SimpleString(b"bad\rvalue".to_vec()),
        RespValue::SimpleError(b"bad\nvalue".to_vec()),
        RespValue::BigNumber(b"12x".to_vec()),
        RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Map(vec![(
                RespValue::SimpleString(b"bad\nkey".to_vec()),
                RespValue::Null,
            )]),
        ]),
    ];

    for value in &invalid_values {
        let mut writer = RecordingWriter::default();
        let error = Encoder::default().encode(value, &mut writer).unwrap_err();
        assert!(writer.bytes.is_empty(), "value wrote bytes before {error}");
    }

    let mut writer = RecordingWriter::default();
    let error = Encoder::default()
        .encode(&invalid_values[3], &mut writer)
        .unwrap_err();
    assert_eq!(
        error.path,
        vec![PathElement::Array(1), PathElement::MapKey(0)]
    );
    assert!(matches!(error.kind, EncodeErrorKind::InvalidSimpleString));
}

#[test]
fn enforces_encode_payload_aggregate_and_depth_limits_before_writing() {
    let encoder = Encoder::new(CodecLimits {
        max_bulk_len: 4,
        max_aggregate_len: 1,
        max_depth: 1,
    });
    let invalid_values = [
        RespValue::BulkString(vec![0; 5]),
        RespValue::BulkError(vec![0; 5]),
        RespValue::VerbatimString {
            format: *b"txt",
            data: b"x".to_vec(),
        },
        RespValue::Array(vec![RespValue::Null, RespValue::Null]),
        RespValue::Map(vec![
            (RespValue::Null, RespValue::Null),
            (RespValue::Null, RespValue::Null),
        ]),
        RespValue::Array(vec![RespValue::Array(vec![RespValue::Null])]),
    ];
    for value in &invalid_values {
        let mut writer = RecordingWriter::default();
        assert!(
            encoder.encode(value, &mut writer).is_err(),
            "value: {value:?}"
        );
        assert!(writer.bytes.is_empty(), "value: {value:?}");
    }
    assert!(encoder.to_bytes(&RespValue::BulkString(vec![0; 4])).is_ok());
    assert!(
        encoder
            .to_bytes(&RespValue::Array(vec![RespValue::Null]))
            .is_ok()
    );
    assert!(
        encoder
            .to_bytes(&RespValue::Array(vec![RespValue::Array(Vec::new())]))
            .is_ok()
    );
}

struct FailingWriter {
    remaining: usize,
}

impl Write for FailingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("injected failure"));
        }
        let written = self.remaining.min(buffer.len());
        self.remaining -= written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn propagates_writer_failures_with_standard_partial_write_behavior() {
    let mut writer = FailingWriter { remaining: 4 };
    let error = Encoder::default()
        .encode(&RespValue::BulkString(b"hello".to_vec()), &mut writer)
        .unwrap_err();
    match error.kind {
        EncodeErrorKind::Io(source) => {
            assert_eq!(source.kind(), io::ErrorKind::Other);
            assert_eq!(source.to_string(), "injected failure");
        }
        kind => panic!("expected I/O error, got {kind:?}"),
    }
}

#[test]
fn rejects_checked_arithmetic_overflow_before_accessing_payloads() {
    let unlimited = Decoder::new(CodecLimits {
        max_bulk_len: usize::MAX,
        max_aggregate_len: usize::MAX,
        max_depth: 1,
    });

    let bulk = format!("${}\r\n", usize::MAX);
    assert_eq!(
        unlimited.decode(bulk.as_bytes()).unwrap_err().kind,
        DecodeErrorKind::NumericOverflow
    );

    let overflowing_entry_count = usize::MAX / 2 + 1;
    let map = format!("%{overflowing_entry_count}\r\n");
    assert_eq!(
        unlimited.decode(map.as_bytes()).unwrap_err().kind,
        DecodeErrorKind::NumericOverflow
    );
}

fn resp_strategy() -> impl Strategy<Value = RespValue> {
    let safe_line = vec(
        any::<u8>().prop_filter("no CR or LF", |byte| !matches!(byte, b'\r' | b'\n')),
        0..16,
    );
    let bytes = vec(any::<u8>(), 0..16);
    let big_number = (
        prop_oneof![Just(Vec::new()), Just(vec![b'+']), Just(vec![b'-'])],
        vec(b'0'..=b'9', 1..20),
    )
        .prop_map(|(mut sign, digits)| {
            sign.extend(digits);
            RespValue::BigNumber(sign)
        });
    let leaf = prop_oneof![
        safe_line.clone().prop_map(RespValue::SimpleString),
        safe_line.prop_map(RespValue::SimpleError),
        any::<i64>().prop_map(RespValue::Integer),
        bytes.clone().prop_map(RespValue::BulkString),
        Just(RespValue::NullBulkString),
        Just(RespValue::NullArray),
        Just(RespValue::Null),
        any::<bool>().prop_map(RespValue::Boolean),
        any::<f64>().prop_map(RespValue::Double),
        big_number,
        bytes.clone().prop_map(RespValue::BulkError),
        (any::<[u8; 3]>(), bytes)
            .prop_map(|(format, data)| { RespValue::VerbatimString { format, data } }),
    ];

    leaf.prop_recursive(4, 96, 8, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..4).prop_map(RespValue::Array),
            vec((inner.clone(), inner), 0..4).prop_map(RespValue::Map),
        ]
    })
}

fn semantically_equal(left: &RespValue, right: &RespValue) -> bool {
    match (left, right) {
        (RespValue::Double(left), RespValue::Double(right)) => {
            (left.is_nan() && right.is_nan()) || left == right
        }
        (RespValue::Array(left), RespValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| semantically_equal(left, right))
        }
        (RespValue::Map(left), RespValue::Map(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(
                    |((left_key, left_value), (right_key, right_value))| {
                        semantically_equal(left_key, right_key)
                            && semantically_equal(left_value, right_value)
                    },
                )
        }
        _ => left == right,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn bounded_values_round_trip(value in resp_strategy()) {
        let bytes = Encoder::default().to_bytes(&value).unwrap();
        let decoded = Decoder::default().decode(&bytes).unwrap();
        prop_assert_eq!(decoded.consumed, bytes.len());
        prop_assert!(semantically_equal(&value, &decoded.value),
            "left: {:?}, right: {:?}", value, decoded.value);
    }
}
