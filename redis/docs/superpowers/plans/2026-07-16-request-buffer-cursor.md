# Request Buffer Cursor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-command input-buffer drains with checked cursor advancement and at most one compaction per socket read.

**Architecture:** The connection loop decodes from an immutable suffix identified by a `consumed` cursor. Small checked helpers validate decoder progress and compact the processed prefix only after the decode pass stops, returning connection-local `InvalidData` errors for broken invariants.

**Tech Stack:** Rust 2024 standard library, existing RESP decoder, and `redis-benchmark`.

## Global Constraints

- Preserve exact RESP bytes, logging behavior, EOF behavior, and connection isolation.
- Never slice, copy, or truncate with an unvalidated cursor.
- Reject zero decoder progress and consumption beyond the supplied suffix with `io::ErrorKind::InvalidData`.
- Apply the incomplete-input limit only to the unconsumed suffix.
- Compact no more than once per successful socket read.
- Keep the existing allocation policy, per-connection limit, owning decoder, and dependencies unchanged.

---

### Task 1: Checked cursor and compaction primitives

**Files:**
- Modify: `src/server.rs`
- Test: unit tests in `src/server.rs`

**Interfaces:**
- Produces: `advance_cursor(cursor: usize, decoded: usize, buffer_len: usize) -> io::Result<usize>`.
- Produces: `compact_buffer(buffer: &mut Vec<u8>, consumed: usize) -> io::Result<()>`.

- [x] **Step 1: Write failing invariant tests**

Add tests requiring valid progress to advance, zero progress and oversized progress to return `InvalidData`, no-progress compaction to leave bytes unchanged, complete consumption to clear the length, a partial suffix to move to index zero, and an oversized cursor to return `InvalidData` without changing bytes.

```rust
#[test]
fn cursor_progress_is_checked() {
    assert_eq!(advance_cursor(4, 3, 10).unwrap(), 7);
    assert_eq!(advance_cursor(4, 0, 10).unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(advance_cursor(4, 7, 10).unwrap_err().kind(), io::ErrorKind::InvalidData);
}

#[test]
fn compaction_preserves_only_the_live_suffix() {
    let mut partial = b"PINGpartial".to_vec();
    compact_buffer(&mut partial, 4).unwrap();
    assert_eq!(partial, b"partial");

    let mut complete = b"PING".to_vec();
    compact_buffer(&mut complete, 4).unwrap();
    assert!(complete.is_empty());

    let mut untouched = b"partial".to_vec();
    compact_buffer(&mut untouched, 0).unwrap();
    assert_eq!(untouched, b"partial");
}

#[test]
fn invalid_compaction_is_connection_local_error() {
    let mut buffer = b"PING".to_vec();
    assert_eq!(compact_buffer(&mut buffer, 5).unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(buffer, b"PING");
}
```

- [x] **Step 2: Run focused tests and verify RED**

Run: `cargo test --bin redis cursor_progress_is_checked`

Expected: compilation fails because `advance_cursor` and `compact_buffer` do not exist.

- [x] **Step 3: Implement the checked helpers**

Use `checked_sub` to validate the cursor against the buffer length. `advance_cursor` rejects zero or excessive decoder progress, then uses `checked_add`. `compact_buffer` validates before mutation, returns immediately for zero progress, uses `clear()` for full consumption, and otherwise uses `copy_within(consumed.., 0)` followed by `truncate(remaining)`.

- [x] **Step 4: Run focused tests and verify GREEN**

Run: `cargo test --bin redis cursor_ && cargo test --bin redis compaction_`

Expected: all cursor and compaction tests pass.

### Task 2: Decode pipelined requests through the cursor

**Files:**
- Modify: `src/server.rs`
- Test: connection tests in `src/server.rs`

**Interfaces:**
- Consumes: checked cursor and compaction helpers from Task 1.
- Changes: `handle_io` retains one `consumed` cursor per decode pass and decodes `buffer.get(consumed..)`.

- [x] **Step 1: Add the mixed pipeline/fragmentation regression test**

Add a socket test that writes two complete requests followed by a partial third request, verifies the first two ordered responses, verifies no third response arrives early, supplies the remainder, then verifies the final response.

- [x] **Step 2: Verify the regression test passes before refactoring**

Run: `cargo test --bin redis pipelined_requests_before_a_fragmented_request_remain_ordered`

Expected: PASS, establishing the behavior that the performance refactor must preserve.

- [x] **Step 3: Replace per-command draining with cursor decoding**

Initialize `consumed = 0` after appending each read. Obtain the decoder input with `buffer.get(consumed..)`, advance through `advance_cursor` after successful decoding, and evaluate the incomplete limit against the suffix length. Stop the decode pass on an empty suffix or incomplete input, then call `compact_buffer` exactly once before the next read.

- [x] **Step 4: Run server tests and verify GREEN**

Run: `cargo test --bin redis server::tests::`

Expected: all server tests pass.

### Task 3: Verification and performance comparison

**Files:**
- Verify: `src/server.rs`

**Interfaces:**
- Produces: formatted, tested, lint-clean code and benchmark evidence against the recorded release baseline.

- [x] **Step 1: Format and run all tests**

Run: `cargo fmt --all && cargo fmt --all -- --check && cargo test --all-targets`

Expected: formatting succeeds and all tests pass.

- [x] **Step 2: Run strict linting**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: Clippy exits successfully with no warnings.

- [x] **Step 3: Benchmark non-pipelined release behavior**

Run the release server with `--quiet`, then run `redis-benchmark -t SET,GET -q -n 100000 -c 50`.

Recorded baseline: SET 145,773 requests/s; GET 160,256 requests/s.

- [x] **Step 4: Benchmark pipelined release behavior**

Against the same server, run `redis-benchmark -t SET,GET -q -n 100000 -c 50 -P 16`.

Recorded baseline: SET 943,396 requests/s; GET 1,010,101 requests/s.

Expected: no material non-pipelined regression and a measurable pipelined improvement. Record actual results without declaring success if normal run-to-run variance obscures the effect.

Observed: non-pipelined SET/GET remained effectively unchanged. Alternating one-million-request A/B samples at pipeline depth 16 averaged approximately +4.5% SET and -0.6% GET, with substantial SET variance. At pipeline depth 256, the cursor build measured +0.9% SET and +0.7% GET. These results do not establish a material throughput improvement.
