# Borrowed Request Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parse valid Redis command requests into borrowed payload slices so the server stops copying command names and arguments before dispatch.

**Architecture:** A new public `RequestDecoder` recognizes flat RESP arrays of bulk strings and returns `DecodedRequest<'a> { parts: Vec<&'a [u8]>, consumed }`. The valid path borrows directly from the connection buffer; invalid command shapes fall back to the existing general decoder so incomplete and malformed framing retain current behavior. Logging and dispatch consume borrowed slices, while SET copies only at the database ownership boundary and responses remain owned.

**Tech Stack:** Rust 2024 standard library, the existing RESP codec, `redis-benchmark`, and the existing test suite.

## Global Constraints

- Copy no valid request payload bytes inside the request decoder.
- Permit one `Vec<&[u8]>` descriptor allocation per decoded request.
- Preserve current RESP limits, structured framing errors, fragmentation timing, protocol-error wire bytes, logging, and connection isolation.
- GET must borrow its lookup key; SET must copy key and value exactly once when database ownership begins.
- Keep database storage, response ownership, response encoding, socket I/O, dependencies, and memory limits unchanged.
- Do not compact or mutate the connection input buffer while a decoded request borrows it.

---

### Task 1: Borrow valid command frames

**Files:**
- Create: `src/resp/request.rs`
- Modify: `src/resp/decode.rs`
- Modify: `src/resp/mod.rs`
- Create: `tests/resp_request.rs`

**Interfaces:**
- Produces: `RequestDecoder::new(limits: CodecLimits) -> RequestDecoder`.
- Produces: `RequestDecoder::decode<'a>(&self, input: &'a [u8]) -> Result<DecodedRequest<'a>, DecodeError>`.
- Produces: `DecodedRequest<'a> { pub parts: Vec<&'a [u8]>, pub consumed: usize }`.
- Changes: `decode::parse_length` visibility from private to `pub(super)` for the sibling request parser.

- [x] **Step 1: Write failing borrowed-payload tests**

Create `tests/resp_request.rs` with tests that require binary and empty arguments, exact consumed length, untouched pipelined input, and payload pointers inside the original input allocation.

```rust
use redis::resp::{CodecLimits, RequestDecoder};

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
```

- [x] **Step 2: Run the focused test and verify RED**

Run: `cargo test --test resp_request borrows_command_payloads_and_preserves_pipeline_tail`

Expected: compilation fails because `RequestDecoder` is not exported.

- [x] **Step 3: Implement the valid flat-array parser**

Add `mod request;` plus `pub use request::{DecodedRequest, RequestDecoder};` in `src/resp/mod.rs`. Change `parse_length` to `pub(super)` in `src/resp/decode.rs`.

Implement these types and the valid path in `src/resp/request.rs`:

```rust
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
        let mut cursor = RequestCursor {
            input,
            position: 0,
            limits: self.limits,
        };
        cursor.expect_byte(b'*')?;
        let length_offset = cursor.position;
        let length = parse_length(cursor.line()?, true)
            .map_err(|kind| DecodeError::new(length_offset, kind))?
            .expect("null array forbidden");
        cursor.check_aggregate_limit(length, length_offset)?;

        let mut parts = Vec::with_capacity(length);
        for _ in 0..length {
            cursor.expect_byte(b'$')?;
            let length_offset = cursor.position;
            let length = parse_length(cursor.line()?, true)
                .map_err(|kind| DecodeError::new(length_offset, kind))?
                .expect("null bulk string forbidden");
            cursor.check_bulk_limit(length, length_offset)?;
            parts.push(cursor.framed_payload(length, length_offset)?);
        }
        Ok(DecodedRequest {
            parts,
            consumed: cursor.position,
        })
    }
}

impl Default for RequestDecoder {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}
```

Implement `RequestCursor` with checked `line`, `expect_byte`, `framed_payload`, `check_bulk_limit`, and `check_aggregate_limit` methods matching the existing decoder's offsets and `DecodeErrorKind` values. `framed_payload` returns `&self.input[start..end]` directly and never calls `to_vec()`.

- [x] **Step 4: Run focused tests and verify GREEN**

Run: `cargo test --test resp_request`

Expected: the borrowed payload and pipeline-tail tests pass.

### Task 2: Preserve invalid-shape and limit behavior

**Files:**
- Modify: `src/resp/error.rs`
- Modify: `src/resp/request.rs`
- Modify: `tests/resp_request.rs`

**Interfaces:**
- Adds: `DecodeErrorKind::InvalidCommandFraming`.
- Adds: request-decoder fallback through `Decoder` only for non-command RESP shapes.

- [x] **Step 1: Write failing fragmentation, shape, and limit tests**

Add tests covering every truncation of a valid SET frame, complete invalid shapes, incomplete invalid shapes, malformed lengths/CRLF, and configured limits.

```rust
use redis::resp::{CodecLimits, DecodeErrorKind, RequestDecoder};

#[test]
fn every_truncated_command_is_incomplete() {
    let request = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    for end in 0..request.len() {
        assert_eq!(
            RequestDecoder::default().decode(&request[..end]).unwrap_err().kind,
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
        RequestDecoder::default().decode(b"*1\r\n+PI").unwrap_err().kind,
        DecodeErrorKind::IncompleteInput
    );
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
```

- [x] **Step 2: Run tests and verify RED**

Run: `cargo test --test resp_request`

Expected: invalid-shape assertions fail because the valid-path parser reports prefix/length errors instead of preserving general RESP completion behavior.

- [x] **Step 3: Implement error-path fallback**

Add `InvalidCommandFraming` to `DecodeErrorKind` and render it as `invalid command framing` in `Display`.

Before entering the valid fast path, and whenever an array or member is null or has the wrong prefix, call:

```rust
fn invalid_shape<'a>(&self, input: &'a [u8]) -> Result<DecodedRequest<'a>, DecodeError> {
    match super::Decoder::new(self.limits).decode(input) {
        Ok(decoded) => Err(DecodeError::new(
            decoded.consumed,
            DecodeErrorKind::InvalidCommandFraming,
        )),
        Err(error) => Err(error),
    }
}
```

Keep malformed command-array lengths, bulk lengths, CRLF, overflow, and limits on the specialized path so their existing structured kinds and offsets are retained.

- [x] **Step 4: Run request and generic codec tests**

Run: `cargo test --test resp_request && cargo test --test resp_codec`

Expected: request tests pass, and the general RESP codec remains unchanged except for the additional error-kind variant.

### Task 3: Dispatch and log borrowed command parts

**Files:**
- Modify: `src/command.rs`
- Modify: `src/logging.rs`

**Interfaces:**
- Changes: `command::dispatch(parts: &[&[u8]], database: &Database) -> RespValue`.
- Changes: `logging::request_line(client: &ClientIdentity, parts: &[&[u8]]) -> String`.

- [x] **Step 1: Change tests to require borrowed interfaces**

Replace owned command fixtures with borrowed slices and call dispatch by reference:

```rust
fn dispatch_once(parts: &[&[u8]]) -> RespValue {
    dispatch(parts, &Database::default())
}

#[test]
fn set_copies_only_at_database_boundary() {
    let database = Database::default();
    let mut request_storage = b"keyvalue".to_vec();
    let parts = [b"SET".as_slice(), &request_storage[..3], &request_storage[3..]];
    assert_eq!(dispatch(&parts, &database), RespValue::SimpleString(b"OK".to_vec()));
    request_storage.fill(b'x');
    assert_eq!(
        dispatch(&[b"GET", b"key"], &database),
        RespValue::BulkString(b"value".to_vec())
    );
}
```

Update logging tests to call `request_line(&client, &[b"ECHO".as_slice(), b"hello\nworld"])`.

- [x] **Step 2: Run command and logging tests and verify RED**

Run: `cargo test --bin redis command::tests && cargo test --bin redis logging::tests`

Expected: compilation fails because production signatures still require owned vectors.

- [x] **Step 3: Implement borrowed dispatch and logging**

Change dispatch and request logging to accept `&[&[u8]]`. Keep all comparisons borrowed. Construct owned responses only where required:

```rust
if name.eq_ignore_ascii_case(b"SET") {
    if parts.len() != 3 {
        return wrong_arity("set");
    }
    database.set(parts[1].to_vec(), parts[2].to_vec());
    return RespValue::SimpleString(b"OK".to_vec());
}

if name.eq_ignore_ascii_case(b"GET") {
    return match parts.len() {
        2 => database
            .get(parts[1])
            .map_or(RespValue::NullBulkString, RespValue::BulkString),
        _ => wrong_arity("get"),
    };
}
```

PING and ECHO convert only their response payload to `Vec<u8>`. Unknown commands and arity errors remain byte-for-byte identical.

- [x] **Step 4: Run focused tests and verify GREEN**

Run: `cargo test --bin redis command::tests && cargo test --bin redis logging::tests`

Expected: all command and logging tests pass.

### Task 4: Integrate RequestDecoder into the connection loop

**Files:**
- Modify: `src/server.rs`

**Interfaces:**
- Consumes: `RequestDecoder`, `DecodedRequest<'a>`, borrowed dispatch, and borrowed logging.
- Removes: private `command_parts(RespValue) -> Option<Vec<Vec<u8>>>`.
- Changes: `EventLogger::request(&mut self, parts: &[&[u8]])`.

- [x] **Step 1: Add a server test proving borrowed request storage remains valid through dispatch**

Use the existing `TestIo` path with pipelined binary SET and GET commands, asserting exact responses and enabled request logs. Keep the mixed pipeline/fragmentation test as the compaction lifetime regression.

- [x] **Step 2: Run the focused server test before integration**

Run: `cargo test --bin redis server::tests::handles_pipelined_set_followed_by_get`

Expected: PASS, recording the wire behavior that integration must preserve.

- [x] **Step 3: Replace generic request decoding**

Import `RequestDecoder` instead of `Decoder`, instantiate it once per connection loop, and replace the success arm with:

```rust
Ok(decoded) => {
    consumed = advance_cursor(consumed, decoded.consumed, buffer.len())?;
    if decoded.parts.is_empty() {
        logger.protocol_error("invalid command framing");
        reject_protocol(stream, logger)?;
        return Ok(());
    }
    logger.request(&decoded.parts);
    let response = command::dispatch(&decoded.parts, database);
    let encoded = encoder
        .to_bytes(&response)
        .map_err(|error| io::Error::other(error.to_string()))?;
    write_all_counted(stream, &encoded, logger)?;
    logger.response(&response);
    if consumed == buffer.len() {
        break;
    }
}
```

Special-case `DecodeErrorKind::InvalidCommandFraming` to retain the concise protocol log line. Keep incomplete-input limits, other decode-error logging, cursor checks, and post-pass compaction unchanged. Remove `command_parts`.

- [x] **Step 4: Run all binary tests**

Run: `cargo test --bin redis`

Expected: all parser consumers, server behavior, logging, and database tests pass.

### Task 5: Acceptance and A/B performance measurement

**Files:**
- Verify all modified files.

**Interfaces:**
- Produces: formatted, tested, lint-clean code and paired benchmark evidence against parent commit `bdb73be`.

- [x] **Step 1: Format and verify the entire project**

Run: `cargo fmt --all`, then `cargo fmt --all -- --check`, then `cargo test --all-targets`.

Expected: rustfmt succeeds and every unit/integration test passes.

- [x] **Step 2: Run strict linting**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: Clippy exits successfully with no warnings.

- [x] **Step 3: Build paired release binaries**

Build the current checkout with `cargo build --release`. Export parent commit `bdb73be` to a temporary directory with `git archive`, then build its release binary from the exported `Cargo.toml`.

Expected: both binaries build successfully and the working tree remains unchanged.

- [x] **Step 4: Run alternating non-pipelined and pipelined A/B benchmarks**

For old/new/new/old order, start each binary with `--quiet` and run:

```text
redis-benchmark -t SET,GET -q -n 300000 -c 50
redis-benchmark -t SET,GET -q -n 300000 -c 50 -P 16
```

Expected: record every SET/GET throughput result, compare averages, and report variance. Treat the parser as a performance win only if the improvement is repeatable and exceeds observed run-to-run noise.

#### Observed A/B result

Measured on an isolated loopback port in old/new/new/old order. Each entry is the
average of two runs at 300,000 operations and 50 clients.

| Workload | Parent `bdb73be` | Borrowed parser | Change |
| --- | ---: | ---: | ---: |
| SET, no pipeline | 151,870 req/s | 152,932 req/s | +0.7% |
| GET, no pipeline | 155,162 req/s | 152,912 req/s | -1.5% |
| SET, `-P 16` | 877,463 req/s | 867,081 req/s | -1.2% |
| GET, `-P 16` | 1,018,820 req/s | 1,013,803 req/s | -0.5% |

Conclusion: the result is performance-neutral within observed run-to-run noise;
it does not meet the plan's criterion for claiming a performance win.
