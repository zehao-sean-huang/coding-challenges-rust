use redis::resp::{CodecLimits, DecodeErrorKind, RequestDecoder};

fn decoder() -> RequestDecoder {
    RequestDecoder::new(CodecLimits::default())
}

#[test]
fn borrows_command_payloads_and_preserves_pipeline_tail() {
    let input = b"*3\r\n$3\r\nSET\r\n$2\r\n\0\xff\r\n$0\r\n\r\n*1\r\n$4\r\nPING\r\n";
    let decoded = decoder().decode(input).unwrap();

    assert_eq!(decoded.parts, [b"SET".as_slice(), b"\0\xff", b""]);
    assert_eq!(&input[decoded.consumed..], b"*1\r\n$4\r\nPING\r\n");

    let input_start = input.as_ptr() as usize;
    let input_end = input_start + input.len();
    for part in decoded.parts {
        let part_start = part.as_ptr() as usize;
        assert!(part_start >= input_start);
        assert!(part_start + part.len() <= input_end);
    }
}

#[test]
fn every_truncated_command_is_incomplete() {
    let request = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    for end in 0..request.len() {
        assert_eq!(
            RequestDecoder::default()
                .decode(&request[..end])
                .unwrap_err()
                .kind,
            DecodeErrorKind::IncompleteInput,
            "truncated at {end}"
        );
    }
}

#[test]
fn complete_non_command_values_are_invalid_command_framing() {
    for input in [
        b"+PING\r\n".as_slice(),
        b"*1\r\n+PING\r\n",
        b"*1\r\n$-1\r\n",
        b"*1\r\n*0\r\n",
        b"*-1\r\n",
    ] {
        assert_eq!(
            RequestDecoder::default().decode(input).unwrap_err().kind,
            DecodeErrorKind::InvalidCommandFraming
        );
    }
    assert_eq!(
        RequestDecoder::default()
            .decode(b"*1\r\n+PI")
            .unwrap_err()
            .kind,
        DecodeErrorKind::IncompleteInput
    );
}

#[test]
fn preserves_structured_framing_errors() {
    for (input, expected) in [
        (b"*x\r\n".as_slice(), DecodeErrorKind::InvalidLength),
        (b"*1\r\n$1\r\naXX", DecodeErrorKind::InvalidCrlf),
        (b"*1\r\n$-2\r\n", DecodeErrorKind::InvalidLength),
        (
            b"*184467440737095516160\r\n",
            DecodeErrorKind::NumericOverflow,
        ),
    ] {
        assert_eq!(
            RequestDecoder::default().decode(input).unwrap_err().kind,
            expected
        );
    }
}

#[test]
fn enforces_request_bulk_and_aggregate_limits() {
    let decoder = RequestDecoder::new(CodecLimits {
        max_bulk_len: 3,
        max_aggregate_len: 2,
        max_depth: 1,
    });
    assert_eq!(
        decoder.decode(b"*1\r\n$4\r\n").unwrap_err().kind,
        DecodeErrorKind::BulkLimitExceeded { len: 4, max: 3 }
    );
    assert_eq!(
        decoder.decode(b"*3\r\n").unwrap_err().kind,
        DecodeErrorKind::AggregateLimitExceeded { len: 3, max: 2 }
    );
}
