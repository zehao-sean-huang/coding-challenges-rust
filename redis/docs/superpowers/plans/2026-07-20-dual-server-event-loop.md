# Dual Server Event Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a CLI-selectable single-threaded `mio` event-loop server while retaining the current thread-per-connection server as the default behavioral and performance baseline.

**Architecture:** Move the current implementation intact behind a threaded server module, then build an event-loop engine whose connection objects preserve nonblocking input, output, logging, and lifecycle state. `server::run` selects one engine once at startup; both engines share RESP decoding, command dispatch, the current locked `Database`, response encoding, and log formatting. The event loop uses OS readiness plus a deduplicated FIFO continuation queue to preserve progress when fairness budgets stop before `WouldBlock`.

**Tech Stack:** Rust 2024, standard-library networking for the threaded server, `mio` 1.x with `net` and `os-poll`, the existing RESP codec and command/database modules, `redis-cli`, and `redis-benchmark` 8.8.0.

## Global Constraints

- Preserve `threaded` as the default server mode and preserve its current client-visible and logging behavior.
- Accept `--server threaded` and `--server event-loop`; accept `--quiet` in either position; reject missing, duplicate, and unknown arguments before bind.
- Select the engine once at process startup; do not add runtime mode switching or automatic fallback.
- Add no server trait or request-path trait object; use one enum match in `server::run`.
- Keep the existing `Database` and its `RwLock` unchanged for the first event-loop comparison.
- Keep RESP framing, limits, command semantics, binary safety, protocol-error bytes, and connection-local failure behavior unchanged.
- Preserve response order within each connection.
- Treat `WouldBlock` as normal progress exhaustion, EOF as peer input closure, and all other socket errors as connection-local unless listener or poller state is invalid.
- Register writable interest only while unsent output exists; never leave an idle connection permanently writable.
- Enforce the exact initial budgets: 64 accepts, 256 KiB read, 64 commands, 256 KiB written, 64 KiB target response batch, and 256 KiB maximum retained drained-output capacity.
- Permit one valid response to exceed the 64 KiB batch target; stop adding later responses until it drains.
- Use `--quiet` for every performance comparison; enabled synchronous logging is behaviorally supported but excluded from performance claims.
- Do not remove database locks, change hashers, add new commands, add a worker pool, shard the database, or make the event-loop engine the default.
- Follow TDD: demonstrate each behavioral test failing for the intended reason before implementing its production path.
- Before every performance claim, build once, verify port ownership, alternate engine order, and treat neutral results as neutral.

## File Map

- Create `src/server.rs`: engine selection only.
- Create `src/server/threaded.rs`: current blocking server moved without behavioral changes.
- Create `src/server/connection_logging.rs`: logger trait and enabled/disabled implementations already present in the threaded server.
- Create `src/server/event_loop.rs`: `mio` listener, poller, connection registry, token allocation, readiness copying, and continuation scheduling.
- Create `src/server/event_loop/connection.rs`: generic nonblocking connection state, bounded reads/dispatch/writes, interest selection, protocol close, and backpressure.
- Create `src/server/tests.rs`: shared socket-level contract suite executed against both engines.
- Modify `src/main.rs`: parse and pass `ServerMode`.
- Modify `Cargo.toml` and `Cargo.lock`: add `mio`.
- Do not modify `src/command.rs`, `src/database.rs`, or `src/resp/` unless a failing parity test proves the shared contract is insufficient.

---

### Task 1: Isolate the Existing Threaded Server

**Files:**
- Create: `src/server.rs`
- Create: `src/server/threaded.rs` from the current `src/server.rs`
- Delete old monolithic contents from: `src/server.rs`

**Interfaces:**
- Produces: `threaded::run(address: &str, log_mode: LogMode) -> io::Result<()>`.
- Preserves: crate-level `server::run(address: &str, log_mode: LogMode) -> io::Result<()>` until mode selection is added in Task 6.
- Preserves every current private threaded helper and test without semantic edits.

- [ ] **Step 1: Record the green baseline**

Run:

```bash
cargo test --all-targets
```

Expected: every existing test passes with `test result: ok` and zero failures.

- [ ] **Step 2: Move the blocking implementation verbatim**

Move the current `src/server.rs` contents to `src/server/threaded.rs`. Change only the exported entry point visibility:

```rust
pub(super) fn run(address: &str, log_mode: LogMode) -> io::Result<()> {
    // Existing body remains byte-for-byte equivalent.
}
```

Keep `handle_connection`, `handle_io`, cursor/compaction helpers, loggers, constants, and the complete existing `#[cfg(test)]` module in `threaded.rs`.

- [ ] **Step 3: Add the narrow facade**

Create `src/server.rs`:

```rust
mod threaded;

use crate::logging::LogMode;
use std::io;

pub(crate) fn run(address: &str, log_mode: LogMode) -> io::Result<()> {
    threaded::run(address, log_mode)
}
```

- [ ] **Step 4: Prove the move did not change behavior**

Run:

```bash
cargo fmt --all
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: formatting is clean; all existing tests pass; Clippy reports no warnings.

- [ ] **Step 5: Inspect and commit the mechanical diff**

Run `git diff --stat` and `git diff -- src/server.rs src/server/threaded.rs`. Confirm Git recognizes an implementation move plus the new facade, with no deleted tests or changed wire bytes.

```bash
git add src/server.rs src/server/threaded.rs
git commit -m "Refactor threaded server behind facade"
```

### Task 2: Extract Connection Logging Without Changing the Threaded Path

**Files:**
- Create: `src/server/connection_logging.rs`
- Modify: `src/server.rs`
- Modify: `src/server/threaded.rs`

**Interfaces:**
- Produces: `EventLogger` with `connected`, `received`, `sent`, `request`, `response`, `protocol_error`, `incomplete_buffer`, `decode_error`, and `finished` methods.
- Produces: zero-sized `DisabledLogger`.
- Produces: `EnabledLogger<W: Write>::new(client: ClientIdentity, writer: W)`.
- Produces: `client_identity(peer: io::Result<SocketAddr>, local: io::Result<SocketAddr>) -> ClientIdentity`.
- Consumed later by: `Connection<S, L>` where `L: EventLogger`.

- [ ] **Step 1: Add tests that pin the shared logger interface**

Move the existing logger unit assertions from `threaded.rs` into `connection_logging.rs` and retain these exact properties:

```rust
#[test]
fn disabled_logger_is_zero_sized() {
    assert_eq!(std::mem::size_of::<DisabledLogger>(), 0);
}

#[test]
fn enabled_logger_records_exact_counters() {
    let mut logger = EnabledLogger::new(test_client(), Vec::new());
    logger.received(17);
    logger.request(&[b"PING".as_slice()]);
    logger.response(&RespValue::SimpleString(b"PONG".to_vec()));
    logger.sent(7);

    assert_eq!(
        logger.stats,
        ConnectionStats {
            requests: 1,
            received: 17,
            sent: 7,
        }
    );
}
```

Run `cargo test --bin redis connection_logging`.

Expected: compilation fails because `connection_logging` and its exported logger types do not exist.

- [ ] **Step 2: Extract the existing logger code**

Create `src/server/connection_logging.rs` with the existing implementations, organized around this interface:

```rust
pub(super) trait EventLogger {
    fn connected(&mut self) {}
    fn received(&mut self, _bytes: usize) {}
    fn sent(&mut self, _bytes: usize) {}
    fn request(&mut self, _parts: &[&[u8]]) {}
    fn response(&mut self, _response: &RespValue) {}
    fn protocol_error(&mut self, _reason: &str) {}
    fn incomplete_buffer(&mut self, _buffer_limit: usize) {}
    fn decode_error(&mut self, _error: &DecodeError) {}
    fn finished(&mut self, _result: &io::Result<()>) {}
}

pub(super) struct DisabledLogger;

pub(super) struct EnabledLogger<W> {
    pub(super) client: ClientIdentity,
    pub(super) stats: ConnectionStats,
    pub(super) started: Instant,
    pub(super) writer: W,
}
```

`EnabledLogger` must call the existing functions in `crate::logging`, ignore log-write failures as today, and produce the existing connected/disconnected/I/O-error lines. `client_identity` uses one shared `AtomicU64`, `Ordering::Relaxed`, and the existing `unavailable ({error})` address fallback.

- [ ] **Step 3: Adapt the threaded connection wrapper**

Replace the logger definitions in `threaded.rs` with imports and one generic runner:

```rust
fn handle_connection_with_logger<L: EventLogger>(
    mut stream: TcpStream,
    buffer_limit: usize,
    database: Arc<Database>,
    mut logger: L,
) -> io::Result<()> {
    logger.connected();
    let result = handle_io(&mut stream, buffer_limit, &database, &mut logger);
    logger.finished(&result);
    result
}
```

`handle_connection` still matches `LogMode` once. The enabled branch constructs `EnabledLogger::new(client_identity(stream.peer_addr(), stream.local_addr()), io::stderr())`; the disabled branch constructs `DisabledLogger` and performs no identity, timer, formatting, statistics, or stderr work.

- [ ] **Step 4: Run the logger and full threaded tests**

Run:

```bash
cargo test --bin redis connection_logging
cargo test --bin redis server::threaded
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: exact logger assertions and all prior threaded tests pass; Clippy is clean.

- [ ] **Step 5: Commit the behavior-preserving extraction**

```bash
git add src/server.rs src/server/threaded.rs src/server/connection_logging.rs
git commit -m "Extract shared connection logging"
```

### Task 3: Build the Nonblocking Connection State Machine

**Files:**
- Create: `src/server/event_loop.rs`
- Create: `src/server/event_loop/connection.rs`
- Modify: `src/server.rs`

**Interfaces:**
- Produces: `Connection<S, L>` where `S: Read + Write` and `L: EventLogger`.
- Produces: `Readiness { readable: bool, writable: bool }`.
- Produces: `DesiredInterest::{Readable, Writable}`.
- Produces: `ServiceResult { close: bool, requeue: bool, interest: Option<DesiredInterest> }`.
- Produces: `Connection::new(socket, logger, buffer_limit)` so production uses the existing limit and tests can inject a smaller limit.
- Produces: `Connection::service(&mut self, readiness, database, decoder, encoder) -> io::Result<ServiceResult>`.
- Keeps socket registration out of `connection.rs`; Task 5 translates `DesiredInterest` into `mio::Interest`.

- [ ] **Step 1: Write failing fragmented and pipelined request tests**

Create a scripted nonblocking I/O fixture in `connection.rs` tests. It returns configured byte chunks and then `WouldBlock`, and captures writes:

```rust
enum ReadAction {
    Bytes(Vec<u8>),
    WouldBlock,
    Eof,
}

struct ScriptedIo {
    reads: VecDeque<ReadAction>,
    written: Vec<u8>,
    max_write: usize,
    block_writes: bool,
}

impl ScriptedIo {
    fn reading(reads: impl IntoIterator<Item = ReadAction>) -> Self {
        Self {
            reads: reads.into_iter().collect(),
            written: Vec::new(),
            max_write: usize::MAX,
            block_writes: false,
        }
    }
}

fn test_connection(io: ScriptedIo) -> Connection<ScriptedIo, DisabledLogger> {
    Connection::new(io, DisabledLogger, 1024)
}

fn service_readable(
    connection: &mut Connection<ScriptedIo, DisabledLogger>,
) -> io::Result<ServiceResult> {
    connection.service(
        Readiness {
            readable: true,
            writable: false,
        },
        &Database::default(),
        &RequestDecoder::default(),
        &Encoder::default(),
    )
}
```

Add:

```rust
#[test]
fn fragmented_pipeline_resumes_without_losing_order() {
    let io = ScriptedIo::reading([
        ReadAction::Bytes(b"*1\r\n$4\r\nPI".to_vec()),
        ReadAction::WouldBlock,
        ReadAction::Bytes(
            b"NG\r\n*2\r\n$4\r\nECHO\r\n$3\r\none\r\n".to_vec(),
        ),
        ReadAction::WouldBlock,
    ]);
    let mut connection = test_connection(io);

    let first = service_readable(&mut connection).unwrap();
    assert_eq!(first.interest, Some(DesiredInterest::Readable));
    assert!(connection.socket.written.is_empty());

    let second = service_readable(&mut connection).unwrap();
    assert_eq!(connection.socket.written, b"+PONG\r\n$3\r\none\r\n");
    assert!(!second.close);
}
```

Run `cargo test --bin redis server::event_loop::connection::tests::fragmented_pipeline_resumes_without_losing_order`.

Expected: compilation fails because `Connection` and `service` do not exist.

- [ ] **Step 2: Implement the connection fields and bounded read path**

Use these exact constants:

```rust
pub(super) const ACCEPT_BUDGET: usize = 64;
const READ_BUDGET: usize = 256 * 1024;
const COMMAND_BUDGET: usize = 64;
const WRITE_BUDGET: usize = 256 * 1024;
const OUTPUT_BATCH_TARGET: usize = 64 * 1024;
const MAX_RETAINED_OUTPUT: usize = 256 * 1024;
const NORMAL_OUTPUT_CAPACITY: usize = 64 * 1024;
const READ_CHUNK: usize = 8192;
const PROTOCOL_ERROR: &[u8] = b"-ERR Protocol error\r\n";
pub(super) const MAX_INCOMPLETE_BUFFER: usize = 536_870_912 + 64;
```

Re-export the production limit to the parent server module from `event_loop.rs`:

```rust
pub(super) use connection::MAX_INCOMPLETE_BUFFER;
```

Define:

```rust
pub(super) struct Connection<S, L> {
    pub(super) socket: S,
    input: Vec<u8>,
    consumed: usize,
    output: Vec<u8>,
    written: usize,
    peer_closed: bool,
    close_after_flush: bool,
    queued: bool,
    desired_interest: Option<DesiredInterest>,
    buffer_limit: usize,
    pub(super) logger: L,
}
```

`read_available` loops on `Read::read`, retries `Interrupted`, stops successfully on `WouldBlock`, marks `peer_closed` on `Ok(0)`, and sets `requeue` if 256 KiB is consumed before reaching `WouldBlock`. It must not read while output is pending or `close_after_flush` is set.

- [ ] **Step 3: Implement bounded decode, dispatch, and direct encoding**

For at most 64 commands and while output is below 64 KiB:

```rust
let decoded = decoder.decode(&self.input[self.consumed..]);
```

On success, validate nonempty command parts, advance with checked arithmetic, call `logger.request`, call `command::dispatch`, encode directly into `self.output` with `encoder.encode(&response, &mut self.output)`, call `logger.response`, and then drop all request borrows before compacting input.

On `IncompleteInput`, leave the suffix buffered unless it exceeds `self.buffer_limit`; production constructs connections with `MAX_INCOMPLETE_BUFFER`, while tests may inject a smaller value. On complete invalid framing or another decode error, log the exact reason, append `PROTOCOL_ERROR`, set `close_after_flush`, and stop reads and dispatch. If EOF leaves only an incomplete suffix, discard it silently.

- [ ] **Step 4: Implement immediate bounded writes and lifecycle output**

After dispatch, attempt to write queued output even when the readiness event was only readable. Retry `Interrupted`; preserve `written` on a partial write; treat `WouldBlock` as `DesiredInterest::Writable`; return `WriteZero` for `Ok(0)`.

When output drains:

```rust
self.output.clear();
self.written = 0;
if self.output.capacity() > MAX_RETAINED_OUTPUT {
    self.output = Vec::with_capacity(NORMAL_OUTPUT_CAPACITY);
}
```

Return `close = true` only when output is empty and either `close_after_flush` is set or peer EOF has left no complete buffered work. Otherwise return readable interest when no output exists and writable interest when output remains.

- [ ] **Step 5: Prove basic connection behavior**

Add and run exact tests for:

- fragmented PING followed by pipelined ECHO;
- pipelined SET followed by GET;
- EOF after a complete request flushes its response;
- EOF with an incomplete request closes silently;
- malformed framing queues exactly `-ERR Protocol error\r\n` and closes after flush;
- one connection error does not mutate a separate connection or database.

Run:

```bash
cargo test --bin redis server::event_loop::connection
```

Expected: every connection-state test passes.

- [ ] **Step 6: Commit the connection core**

```bash
git add src/server.rs src/server/event_loop.rs src/server/event_loop/connection.rs
git commit -m "Add nonblocking connection state machine"
```

### Task 4: Enforce Partial-I/O, Backpressure, and Fairness Invariants

**Files:**
- Modify: `src/server/event_loop/connection.rs`

**Interfaces:**
- Produces: `Connection::mark_queued() -> bool`, returning true only on the first enqueue.
- Produces: `Connection::clear_queued()`.
- Produces: stable `desired_interest()` for Task 5 registration.
- Tightens: `ServiceResult::requeue` for exhausted read, command, or write budgets and for user-space work exposed after output drains.

- [ ] **Step 1: Write failing partial-write and interest tests**

Add a writer that accepts three bytes, returns `WouldBlock`, and later resumes. Assert:

```rust
assert_eq!(first.interest, Some(DesiredInterest::Writable));
assert_eq!(&connection.output[connection.written..], b"NG\r\n");
assert!(!first.close);

connection.socket.block_writes = false;
let second = connection.service(
    Readiness { readable: false, writable: true },
    &database,
    &decoder,
    &encoder,
).unwrap();
assert_eq!(connection.socket.written, b"+PONG\r\n");
assert_eq!(second.interest, Some(DesiredInterest::Readable));
```

Run the focused test. Expected: RED because pending output and interest transitions are not yet complete.

- [ ] **Step 2: Write failing work-budget tests**

Build 65 pipelined PING requests. After one service turn, assert exactly 64 responses were dispatched, `requeue` is true, and no input is read again while output is blocked. After the continuation turn, assert the 65th response is appended in order.

Add equivalent tests that exhaust 256 KiB read and write budgets without reaching `WouldBlock` and assert `requeue == true`.

- [ ] **Step 3: Implement deduplicated continuation state**

```rust
pub(super) fn mark_queued(&mut self) -> bool {
    if self.queued {
        false
    } else {
        self.queued = true;
        true
    }
}

pub(super) fn clear_queued(&mut self) {
    self.queued = false;
}
```

Set `requeue` whenever a budget stops progress before the operation reaches `WouldBlock`, or when output drains and complete user-space input remains. Do not requeue an incomplete request waiting for more network bytes.

- [ ] **Step 4: Implement slow-reader backpressure**

While `output[written..]` is nonempty:

- return writable-only interest;
- perform no reads;
- dispatch no new commands after the output batch target has been reached; and
- preserve already-buffered input.

When output drains, restore readable interest and requeue the connection if buffered input might contain work. Add a test proving the unread pipeline resumes and response order is preserved.

- [ ] **Step 5: Implement output-capacity release**

Construct a response buffer above 256 KiB, drain it, and assert the replacement capacity is at least 64 KiB and no more than 256 KiB; `Vec::with_capacity` does not promise an allocator-exact capacity. A single response larger than 64 KiB must still be accepted and written; the 64 KiB value is a batching target, not a response limit.

- [ ] **Step 6: Run all connection tests and commit**

```bash
cargo fmt --all
cargo test --bin redis server::event_loop::connection
cargo clippy --bin redis -- -D warnings
git add src/server/event_loop/connection.rs
git commit -m "Bound event loop connection work"
```

Expected: all connection tests pass and Clippy is clean.

### Task 5: Add the `mio` Poller, Registry, and Continuation Queue

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/server/event_loop.rs`
- Modify: `src/server/event_loop/connection.rs`

**Interfaces:**
- Produces: `event_loop::run(address: &str, log_mode: LogMode) -> io::Result<()>`.
- Produces: `EventLoop<L, F>::bind(address, logger_factory, buffer_limit) -> io::Result<EventLoop<L, F>>`.
- Produces: `EventLoop::local_addr() -> io::Result<SocketAddr>` for startup logging and tests using port zero.
- Produces: `EventLoop::drive_once(timeout: Option<Duration>) -> io::Result<()>` for production and deterministic tests.
- Produces: `EventLoop::run(&mut self) -> io::Result<()>`, which repeatedly calls `drive_once(None)`.
- Produces: monotonically increasing `Token` values, with `Token(0)` reserved for the listener.
- Consumes: `Connection<mio::net::TcpStream, L>` and all Task 4 progress/interest interfaces.

- [ ] **Step 1: Add `mio` and write a failing real-socket test**

Add:

```toml
[dependencies]
mio = { version = "1", features = ["net", "os-poll"] }
```

Write a test that binds `EventLoop` to `127.0.0.1:0`, connects a standard `TcpStream`, sends RESP PING, repeatedly calls `drive_once(Some(Duration::from_millis(50)))`, and reads exactly `+PONG\r\n` without using `sleep`.

Run `cargo test --bin redis event_loop_accepts_and_serves_ping`.

Expected: compilation fails because `EventLoop::bind` and `drive_once` do not exist.

- [ ] **Step 2: Implement poller and non-reused token registry**

Use these driver types:

```rust
const LISTENER: Token = Token(0);
const MAX_CONTINUATIONS_PER_POLL: usize = 256;

enum WorkItem {
    Listener,
    Connection(Token),
}

struct ReadyEvent {
    token: Token,
    readable: bool,
    writable: bool,
}

struct EventLoop<L, F> {
    poll: Poll,
    events: Events,
    ready_events: Vec<ReadyEvent>,
    listener: mio::net::TcpListener,
    connections: HashMap<Token, Connection<mio::net::TcpStream, L>>,
    continuations: VecDeque<WorkItem>,
    listener_queued: bool,
    next_token: usize,
    database: Database,
    decoder: RequestDecoder,
    encoder: Encoder,
    buffer_limit: usize,
    logger_factory: F,
}
```

Initialize both Mio and copied-event buffers with capacity 1024. Copy readiness flags into the reusable `ready_events` vector before mutating other loop fields. Treat `is_read_closed()` as readable so the connection observes EOF, and treat `is_write_closed()` or `is_error()` as writable so pending output observes the actual socket error. Do not close solely from a readiness flag. Allocate tokens with `checked_add`; never reuse removed values. Exhaustion returns `io::Error::other("connection token space exhausted")` from the selected server. A missing token from an already-copied readiness event is ignored.

Add a unit test that removes one connection, proves the next accepted connection gets a larger token, injects a copied event for the removed token, and verifies the new connection is untouched. Set `next_token` to `usize::MAX` in a separate test and assert the exact token-exhaustion error without attempting wraparound.

- [ ] **Step 3: Implement bounded accept and registration**

Accept up to `ACCEPT_BUDGET`. Retry `Interrupted`; stop on `WouldBlock`; return other listener errors. For each socket:

1. allocate a nonzero token;
2. build the mode-specific logger without doing enabled-only work in quiet mode;
3. construct `Connection::new(socket, logger, self.buffer_limit)`;
4. register `Interest::READABLE`; and
5. call `connection.logger.connected()` and insert it into the registry only after successful registration.

If 64 accepts complete before `WouldBlock`, enqueue `WorkItem::Listener` once and continue accepting from the internal queue.

- [ ] **Step 4: Implement readiness service and interest changes**

Translate a copied event to `Readiness`, call `Connection::service`, then:

- remove and finish the connection when `close` is true;
- enqueue it once when `requeue` is true;
- `reregister` as readable or writable only when desired interest changes; and
- close only that connection if service or registration fails.

The driver must call `logger.finished(&result)` exactly once when removing a connection. Listener and poller failures return from the selected server.

- [ ] **Step 5: Implement fair polling and internal continuations**

Each `drive_once` call:

1. polls with the caller's timeout when the continuation queue is empty, otherwise with `Duration::ZERO`;
2. services every copied OS readiness event once;
3. services at most 256 queued continuations FIFO;
4. clears each queued flag before service so the connection may enqueue itself again; and
5. returns without blocking while runnable continuations remain.

Add tests proving a 65-command client cannot prevent a second ready client from receiving PONG, and duplicate OS readiness plus continuation signals produce only one queued connection entry.

- [ ] **Step 6: Add enabled and disabled event-loop startup paths**

`event_loop::run` matches `LogMode` once:

```rust
pub(super) fn run(address: &str, log_mode: LogMode) -> io::Result<()> {
    match log_mode {
        LogMode::Enabled => run_with_logger(address, true, |stream| {
            EnabledLogger::new(
                client_identity(stream.peer_addr(), stream.local_addr()),
                io::stderr(),
            )
        }),
        LogMode::Disabled => run_with_logger(address, false, |_stream| DisabledLogger),
    }
}
```

Define the generic bridge explicitly:

```rust
fn run_with_logger<L, F>(address: &str, announce: bool, logger_factory: F) -> io::Result<()>
where
    L: EventLogger,
    F: FnMut(&mio::net::TcpStream) -> L,
{
    let mut event_loop = EventLoop::bind(address, logger_factory, MAX_INCOMPLETE_BUFFER)?;
    if announce {
        eprintln!("[redis] listening address={}", event_loop.local_addr()?);
    }
    event_loop.run()
}
```

The enabled loop prints the existing listener line once after a successful bind. The disabled instantiation performs no client identity, timer, statistics, formatting, listener announcement, or stderr work.

- [ ] **Step 7: Run poller tests and commit**

```bash
cargo fmt --all
cargo test --bin redis server::event_loop
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
git add Cargo.toml Cargo.lock src/server/event_loop.rs src/server/event_loop/connection.rs
git commit -m "Add mio event loop server"
```

Expected: real-socket, fairness, token, connection, and existing threaded tests pass; Clippy is clean.

### Task 6: Expose Server Mode and Add One Shared Contract Suite

**Files:**
- Modify: `src/server.rs`
- Modify: `src/server/threaded.rs`
- Modify: `src/server/event_loop.rs`
- Modify: `src/main.rs`
- Create: `src/server/tests.rs`

**Interfaces:**
- Produces: `ServerMode::{Threaded, EventLoop}`.
- Changes: `server::run(address: &str, log_mode: LogMode, server_mode: ServerMode) -> io::Result<()>`.
- Produces test-only: `threaded::run_test_listener(listener, log_mode, expected_connections, buffer_limit) -> io::Result<()>`.
- Produces test-only: `event_loop::run_test_listener(listener, log_mode, expected_connections, buffer_limit, deadline) -> io::Result<()>`.

- [ ] **Step 1: Write failing CLI contract tests**

Add exact assertions:

```rust
#[test]
fn parses_server_modes_and_option_order() {
    assert_eq!(
        Config::parse(std::iter::empty::<&str>()).unwrap().server_mode,
        ServerMode::Threaded,
    );
    assert_eq!(
        Config::parse(["--server", "event-loop"]).unwrap().server_mode,
        ServerMode::EventLoop,
    );
    assert_eq!(
        Config::parse(["--server", "threaded", "--quiet"]).unwrap(),
        Config {
            log_mode: LogMode::Disabled,
            server_mode: ServerMode::Threaded,
        },
    );
    assert_eq!(
        Config::parse(["--quiet", "--server", "event-loop"]).unwrap(),
        Config {
            log_mode: LogMode::Disabled,
            server_mode: ServerMode::EventLoop,
        },
    );
}
```

Assert exact failures for duplicate `--quiet`, duplicate `--server`, missing server value, unknown server name, and unknown option. Every failure ends with:

```text
Usage: redis [--quiet] [--server <threaded|event-loop>]
```

Run `cargo test --bin redis tests::parses_server_modes_and_option_order`.

Expected: RED because `Config` has no `server_mode`.

- [ ] **Step 2: Implement `ServerMode` and startup dispatch**

In `server.rs`:

```rust
mod connection_logging;
mod event_loop;
mod threaded;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServerMode {
    Threaded,
    EventLoop,
}

pub(crate) fn run(
    address: &str,
    log_mode: LogMode,
    server_mode: ServerMode,
) -> io::Result<()> {
    match server_mode {
        ServerMode::Threaded => threaded::run(address, log_mode),
        ServerMode::EventLoop => event_loop::run(address, log_mode),
    }
}
```

Update `main` to call `server::run(DEFAULT_ADDRESS, config.log_mode, config.server_mode)`.

- [ ] **Step 3: Implement order-independent argument parsing**

Iterate arguments once while tracking `quiet_seen` and `server_seen`. Do not consume an option-looking token as a server value. Produce these exact first lines before the common usage suffix:

```text
duplicate argument '--quiet'
duplicate argument '--server'
missing value for '--server'
unknown server 'worker-pool'
unknown argument '--verbose'
```

Keep default `LogMode::Enabled` and `ServerMode::Threaded`.

- [ ] **Step 4: Create a shared socket-level behavior harness**

Create `src/server/tests.rs`. Bind one `std::net::TcpListener` to `127.0.0.1:0`, then hand it to a test-only engine runner selected by `ServerMode`. The harness normally passes `MAX_INCOMPLETE_BUFFER`; the input-limit scenario passes `8`. The threaded runner accepts exactly the declared number of clients and joins their existing connection handlers with that injected limit. The event-loop runner converts the listener to nonblocking mode, constructs every connection with the same injected limit, drives with bounded poll timeouts, and exits when the declared clients have been accepted and all have closed; reaching the explicit deadline returns `TimedOut` rather than hanging.

Use one helper:

```rust
fn for_each_server_mode(test: impl Fn(ServerMode)) {
    for mode in [ServerMode::Threaded, ServerMode::EventLoop] {
        test(mode);
    }
}
```

- [ ] **Step 5: Run the same contract scenarios against both engines**

Implement shared tests with exact RESP byte assertions for:

```text
fragmented PING
pipelined PING and binary ECHO
pipelined binary SET then GET
value visibility across two connections
unknown command and wrong arity
all currently rejected non-command RESP shapes
incomplete input above an injected limit
EOF with no request and EOF with an incomplete request
bad-client protocol rejection while a good client still receives PONG
```

Run the basic PING scenario once with `LogMode::Disabled` for each mode and assert the response remains `+PONG\r\n`. The zero-sized disabled-logger test from Task 2 and the captured real-binary output checks in Task 7 prove that this path performs no logging writes.

Do not delete architecture-specific threaded or event-loop tests after adding the shared suite.

- [ ] **Step 6: Run CLI, parity, and full verification**

```bash
cargo fmt --all
cargo test --bin redis tests
cargo test --bin redis server::tests
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: both modes pass every shared byte-level scenario; all existing tests pass; Clippy is clean.

- [ ] **Step 7: Commit the user-visible dual mode**

```bash
git add src/main.rs src/server.rs src/server/threaded.rs src/server/event_loop.rs src/server/tests.rs
git commit -m "Add selectable threaded and event loop servers"
```

### Task 7: Verify Real Binaries and Inspect the Final Diff

**Files:**
- Verify only; modify production or test files only if a failing check exposes a real defect.

**Interfaces:**
- Verifies both production invocations and the complete repository contract.
- Produces no benchmark claim yet.

- [ ] **Step 1: Run repository-wide checks from a clean process state**

Verify nothing owns the Redis port, then run:

```bash
cargo fmt --all --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

Expected: all commands exit zero.

- [ ] **Step 2: Smoke-test the threaded release binary**

Start in terminal A and retain its exact PID:

```bash
target/release/redis --quiet --server threaded >/tmp/redis-threaded.stdout 2>/tmp/redis-threaded.stderr &
threaded_server_pid=$!
```

In terminal B, verify `lsof -nP -iTCP:6379 -sTCP:LISTEN` identifies that exact binary, then run:

```bash
redis-cli -p 6379 SET smoke-key smoke-value
redis-cli -p 6379 GET smoke-key
```

Expected responses: `OK` and `smoke-value`. Stop and inspect only the recorded process:

```bash
kill -INT "$threaded_server_pid"
wait "$threaded_server_pid"
wc -c /tmp/redis-threaded.stdout /tmp/redis-threaded.stderr
```

The signal may make `wait` nonzero. Both byte counts must be zero.

- [ ] **Step 3: Smoke-test the event-loop release binary**

Repeat Step 2 with distinct output files and PID:

```bash
target/release/redis --quiet --server event-loop >/tmp/redis-event-loop.stdout 2>/tmp/redis-event-loop.stderr &
event_loop_server_pid=$!
```

After SET/GET, stop it with `kill -INT "$event_loop_server_pid"`, wait for that PID, and verify both `/tmp/redis-event-loop.stdout` and `/tmp/redis-event-loop.stderr` have zero bytes.

- [ ] **Step 4: Exercise failure boundaries live**

Against event-loop mode, send malformed RESP and verify exactly one error response followed by connection closure:

```bash
printf '*0\r\n' | nc 127.0.0.1 6379
```

Expected output: `-ERR Protocol error` and then `nc` exits. Without restarting the server, `redis-cli -p 6379 PING` returns `PONG`.

Start a five-second slow reader in another terminal; it writes a large PING pipeline and intentionally never reads responses:

```bash
ruby -rsocket -e 's = TCPSocket.new("127.0.0.1", 6379); request = "*1\r\n$4\r\nPING\r\n"; s.write_nonblock(request * 100_000, exception: false); sleep 5'
```

While it is connected, run `redis-cli -t 1 -p 6379 PING`. Expected: `PONG`; the automated fairness and backpressure tests remain the authoritative deterministic proof.

- [ ] **Step 5: Inspect scope and commit only genuine fixes**

Run:

```bash
git status --short
git diff --check
git diff --stat
git diff
```

Confirm there is no database-lock removal, hasher change, new command, async logger, worker pool, key sharding, debug output, generated trace, or benchmark artifact. If verification required a fix, commit only that focused fix after rerunning the failed check.

### Task 8: Run Alternating Performance Comparisons

**Files:**
- No source changes.
- Record commands, raw results, medians, environment, and interpretation in the implementation handoff.

**Interfaces:**
- Compares the two modes from the same `target/release/redis` binary.
- Produces throughput, p50, p99, sampled server CPU use, and an explicit neutral/improved/regressed conclusion.

- [ ] **Step 1: Fix the benchmark environment**

Record:

```bash
git rev-parse HEAD
rustc -Vv
redis-benchmark --version
redis-server --version
uname -a
```

Build once with `cargo build --release`. Before each run, verify port 6379 is free, start exactly one selected mode with `--quiet`, and use `lsof` to prove the release binary owns the port.

- [ ] **Step 2: Run five alternating rounds**

Alternate engine order by round:

```text
round 1: threaded, event-loop
round 2: event-loop, threaded
round 3: threaded, event-loop
round 4: event-loop, threaded
round 5: threaded, event-loop
```

For every engine in every round, run SET followed by GET for this matrix with `-n 1000000`:

```text
-c 1  -P 1   shared key
-c 50 -P 1   shared key
-c 50 -P 16  shared key
-c 50 -P 1   -r 100000
-c 50 -P 16  -r 100000
```

The exact shared-key commands are:

```bash
redis-benchmark -t SET -n 1000000 -c 1 -P 1
redis-benchmark -t GET -n 1000000 -c 1 -P 1
redis-benchmark -t SET -n 1000000 -c 50 -P 1
redis-benchmark -t GET -n 1000000 -c 50 -P 1
redis-benchmark -t SET -n 1000000 -c 50 -P 16
redis-benchmark -t GET -n 1000000 -c 50 -P 16
```

The exact randomized-key commands are:

```bash
redis-benchmark -t SET -n 1000000 -c 50 -P 1 -r 100000
redis-benchmark -t GET -n 1000000 -c 50 -P 1 -r 100000
redis-benchmark -t SET -n 1000000 -c 50 -P 16 -r 100000
redis-benchmark -t GET -n 1000000 -c 50 -P 16 -r 100000
```

Use non-quiet `redis-benchmark` output for percentile data. Do not compare a run if port ownership changed or the server printed output in quiet mode.

- [ ] **Step 3: Measure server CPU during sustained load**

For each mode, start this sustained workload in a separate terminal:

```bash
redis-benchmark -t SET,GET -n 20000000 -c 50 -P 16
```

Resolve and validate the listener PID, then sample it ten times at one-second intervals:

```bash
server_pid="$(lsof -t -iTCP:6379 -sTCP:LISTEN)"
ps -p "$server_pid" -o command=
top -l 10 -s 1 -pid "$server_pid" -stats pid,cpu,time,threads
```

The `ps` output must identify the expected `target/release/redis` process before sampling. Record average server `%CPU`, thread count, and throughput. Label throughput-per-CPU calculations as approximate because `%CPU` is sampled rather than integrated.

- [ ] **Step 4: Analyze without overclaiming**

For each matrix cell, report the median of five throughput, p50, and p99 observations. Report the full observed range. Call an improvement only when it is repeatable across alternating order and materially larger than the run-to-run range. State separately:

- non-pipelined latency/throughput behavior;
- pipelined CPU-bound behavior;
- shared-key versus randomized-key behavior;
- total throughput versus approximate throughput per CPU; and
- whether the event-loop core reaches its one-core capacity ceiling.

If the result is neutral or negative, keep the event-loop mode experimental and report the evidence without changing the default or tuning budgets opportunistically.

- [ ] **Step 5: Final implementation handoff**

Report:

1. files and behavior changed;
2. ownership, ordering, backpressure, and failure decisions;
3. exact formatting, tests, Clippy, smoke, and benchmark commands run;
4. measured results with uncertainty;
5. remaining limitations, especially synchronous enabled logging and the one-core command ceiling; and
6. the next measured optimization, only if the profile or benchmark supports one.
