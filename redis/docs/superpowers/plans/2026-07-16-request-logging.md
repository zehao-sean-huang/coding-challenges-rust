# Human-Readable Request Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dependency-free, human-readable request and connection logging without changing RESP behavior.

**Architecture:** A new binary-owned `logging` module provides pure formatting helpers and connection accounting. The server supplies socket metadata, updates counters at actual I/O boundaries, and emits prepared log lines to stderr.

**Tech Stack:** Rust 2024 standard library and the existing RESP codec.

## Global Constraints

- Keep runtime dependencies empty.
- Write logs to stderr and never change client-visible RESP bytes.
- Escape binary payloads and cap each rendered value at 120 characters.
- Identify connections with monotonic IDs and peer/local socket addresses.
- Log requests, responses, protocol failures, request counts, byte totals, and duration.

---

### Task 1: Human-readable formatting

**Files:**
- Create: `src/logging.rs`
- Modify: `src/main.rs`

**Interfaces:**
- Produces: `ClientIdentity`, `ConnectionStats`, `request_line`, `response_line`, `protocol_error_line`, and lifecycle-line helpers used by `server`.

- [x] **Step 1: Write failing formatter tests**

Add tests that require `render_bytes` to quote and escape `b"hello\n\0\xff"`, truncate rendered values beyond 120 characters with an ellipsis, render PING and ECHO requests, summarize PONG/error/bulk responses, and include `client-0001`, peer/local addresses, counters, and duration in lifecycle lines.

- [x] **Step 2: Verify the tests fail**

Run: `cargo test --bin redis logging::tests`

Expected: compilation fails because the logging types and functions do not exist.

- [x] **Step 3: Implement minimal pure formatting**

Create `ClientIdentity { id: u64, peer: String, local: String }` and `ConnectionStats { requests: u64, received: u64, sent: u64 }`. Implement binary-safe rendering by escaping ASCII control/non-printable bytes, quotes, and backslashes, enforcing the 120-character rendered limit, and surrounding values with quotes. Implement line helpers with the approved `[redis] client-0001 ...` format.

- [x] **Step 4: Verify focused tests pass**

Run: `cargo test --bin redis logging::tests`

Expected: all logging formatter tests pass.

---

### Task 2: Server instrumentation

**Files:**
- Modify: `src/server.rs`
- Test: unit tests in `src/server.rs`

**Interfaces:**
- Consumes: formatting and accounting types from `src/logging.rs`.
- Produces: stderr lifecycle, request, response, protocol-error, and I/O-error logs while preserving `run(address: &str) -> io::Result<()>`.

- [x] **Step 1: Write failing instrumentation tests**

Use an injected `Vec<u8>` log writer with the existing generic test stream. Assert one PING request produces readable request/response lines and exact received/sent counters. Assert malformed RESP logs a protocol reason while still returning `-ERR Protocol error\r\n` to the client-side writer.

- [x] **Step 2: Verify the tests fail**

Run: `cargo test --bin redis server::tests::logs`

Expected: compilation fails because the connection loop does not accept logging context or a log writer.

- [x] **Step 3: Instrument actual I/O boundaries**

Add a monotonic `AtomicU64` connection ID. On accept, obtain peer and local addresses, emit the connection line, and pass identity/statistics/log writer through the loop. Increment received bytes after reads, requests after valid frames, and sent bytes for actual successful writes. Emit request before dispatch, response after dispatch, protocol reasons at rejection, and a final disconnected or I/O-error line with elapsed time. Ignore stderr write failures.

- [x] **Step 4: Verify server and formatter tests pass**

Run: `cargo test --bin redis`

Expected: all binary tests pass with wire responses unchanged.

---

### Task 3: Acceptance verification

**Files:**
- Verify all modified files.

- [x] **Step 1: Format and verify the entire project**

Run: `cargo fmt --all && cargo test --all-targets`

Expected: all unit and integration tests pass.

- [x] **Step 2: Run strict linting**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: Clippy exits successfully with no warnings.

- [x] **Step 3: Inspect runtime output**

Start the server with `cargo run`, send RESP PING and ECHO requests, and confirm stderr shows startup, client metadata, readable request/response lines, and disconnect statistics while the client receives unchanged RESP replies.
