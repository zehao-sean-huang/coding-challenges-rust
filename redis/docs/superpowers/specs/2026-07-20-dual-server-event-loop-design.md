# Dual Server Event Loop Design

## Goal

Add a second, single-threaded nonblocking server implementation while preserving the existing thread-per-connection server as the default baseline. A startup flag selects one implementation for the lifetime of the process. Both implementations must expose the same Redis protocol and command behavior so they can be compared from the same release binary.

This is an experiment in connection scheduling and I/O ownership. The first event-loop implementation deliberately reuses the current database, request decoder, command dispatcher, response encoder, limits, and log formatting. Its reusable output buffer is intrinsic to nonblocking partial writes and response batching. Specialized command/response representations, database allocation changes, lock removal, alternate hashers, and multicore sharding remain separate measurable changes.

## Command-line contract

The binary accepts these forms:

```text
redis
redis --quiet
redis --server threaded
redis --server event-loop
redis --quiet --server threaded
redis --quiet --server event-loop
```

`--quiet` and `--server` may appear in either order. Each may appear at most once. `--server` requires exactly one value: `threaded` or `event-loop`. Missing values, duplicate options, unknown options, and unknown server names fail before binding the listener, print a concise error plus usage, and return a nonzero status.

The default server mode is `threaded`; this preserves existing behavior. Server mode is immutable after startup. The usage line is:

```text
Usage: redis [--quiet] [--server <threaded|event-loop>]
```

## Module boundaries

The server modules are organized as:

```text
src/server.rs                         startup dispatch and shared server mode
src/server/threaded.rs                existing blocking implementation
src/server/event_loop.rs              mio polling, listener, and connection registry
src/server/event_loop/connection.rs   per-connection nonblocking state transitions
```

`server::run(address, log_mode, server_mode)` performs one enum match at startup and delegates to the selected implementation. There is no server trait, trait object, or dynamic dispatch in the request path.

The existing blocking implementation moves into `threaded.rs` with no intended behavioral change. Its connection threads, request loop, buffer handling, logging, error isolation, and `Arc<Database>` ownership remain intact.

The event-loop implementation owns one `Database` directly but initially retains the `RwLock` inside it. It does not wrap the database in `Arc`, and the lock is uncontended. Retaining the current database type keeps `command::dispatch` unchanged and keeps the first benchmark focused on networking, scheduling, and the buffering required by nonblocking I/O. Removing the event-loop lock requires a later design and independent measurement.

The RESP codec, request decoder, command dispatcher, protocol limits, database semantics, and log formatting stay shared. Connection state, readiness handling, buffering, fairness, and cleanup are implementation-specific and are not forced through a common abstraction.

## Event-loop architecture

The event-loop mode adds `mio` with its networking and OS-poll features. One OS thread owns:

- the nonblocking listener;
- `mio::Poll` and its event buffer;
- the connection registry;
- every connection's socket and buffers;
- the database; and
- command execution.

Token zero is reserved for the listener. Connections receive monotonically increasing nonzero tokens and live in a `HashMap<Token, Connection>`. Tokens are not reused, preventing an already-delivered stale readiness event from referring to a new connection. Token allocation uses checked arithmetic; exhaustion returns a server-level error and terminates the selected server rather than wrapping or ambiguously reusing a token.

Each connection stores at least:

```text
socket
input buffer and consumed cursor
output buffer and written cursor
read/write readiness interest
peer-closed state
close-after-flush state
internal-continuation state
client identity and logging statistics when logging is enabled
```

The poller produces only external readiness and lifecycle signals: listener readable, connection readable, connection writable, and connection error or closure. RESP decoding, command dispatch, database access, and response encoding execute synchronously while servicing a readable connection; they are not separate asynchronous tasks.

The loop is the only code allowed to block intentionally, through `Poll::poll` when no internal continuation is runnable. Listener accept, socket read, and socket write run in nonblocking mode. `io::ErrorKind::WouldBlock` means the operation made all currently possible progress and the connection must wait for another readiness notification; it is not logged or treated as a connection failure.

## Connection progress and ordering

On listener readiness, the server accepts connections until `WouldBlock` or the per-turn accept budget is exhausted. Each accepted socket is registered as readable and receives independent input, output, and logging state.

On readable readiness, a connection:

1. reads within its per-turn byte budget until `WouldBlock`, EOF, or an error;
2. decodes complete requests already present in its input buffer;
3. dispatches requests synchronously against the event-loop database;
4. appends encoded responses in request order; and
5. attempts to flush the response batch without waiting.

Decoded request slices may borrow the input buffer only through synchronous command dispatch and response construction. The input buffer is not read into, compacted, or otherwise mutated while those borrows exist.

Responses for one connection remain ordered exactly as requests were decoded. The shared `Encoder` writes each response directly into the connection's reusable output buffer; the event loop does not create an intermediate encoded `Vec` and then copy it into the pending output. A partial write preserves the output buffer and written cursor. The connection registers writable interest only while unsent output exists. Once output drains, the buffer is cleared without discarding normal batch capacity and writable interest is removed so a normally writable socket cannot cause a busy loop.

If a response batch is pending, the connection does not dispatch additional commands after reaching the output batch target. This bounds queued output without rejecting a single valid response larger than the target. Input already received remains buffered and is resumed after output drains.

EOF is distinct from `WouldBlock`. EOF marks the peer as closed for input. Complete buffered commands are still processed and their responses are flushed when possible; an incomplete trailing request is discarded, matching the blocking server's current EOF behavior. The connection closes after pending output drains or a terminal write error occurs.

Protocol errors enqueue the existing protocol-error response, set `close_after_flush`, stop further reads and dispatch, and close after the error response is written. Other decode and connection I/O errors remain connection-local; they must not terminate the listener or corrupt another connection's state.

## Fairness and internal continuations

Readiness is a hint rather than a unit of work. A ready client may have an arbitrarily deep pipeline, and a fairness budget may stop work before the socket reaches `WouldBlock`. Because a new OS readiness edge is not guaranteed for work already buffered in user space, the server maintains an internal FIFO continuation queue.

A connection is queued for continuation when it exhausts a work budget while it can still make immediate progress. A per-connection flag prevents duplicate queue entries. The server drains a bounded number of continuations between poll calls and uses a zero poll timeout while runnable continuations remain; it blocks in `Poll::poll` only when the continuation queue is empty.

Initial compile-time budgets are:

```text
64 accepted connections per listener turn
256 KiB read per connection turn
64 commands dispatched per connection turn
256 KiB written per connection turn
64 KiB target response batch per connection
256 KiB maximum retained output-buffer capacity after a batch drains
```

A single response may exceed the 64 KiB batch target. The target stops additional command dispatch; it is not a response-size limit. These constants are part of the initial implementation and may be tuned only with benchmark and fairness evidence.

## Backpressure and memory bounds

The existing incomplete-request limit remains unchanged and applies to the live unconsumed input. The event-loop server does not continue reading a connection while its response batch is backpressured.

At most one bounded command batch is encoded ahead of the socket. Once output reaches the response batch target, command dispatch pauses and the server attempts to flush. If the socket returns `WouldBlock`, readable interest is disabled and only writable progress is requested. When output drains, readable interest is restored and the connection is internally queued so already-buffered input cannot be stranded.

This policy bounds pipeline-generated output by the batch target plus one maximum valid response. It preserves support for a single response up to the existing protocol limit and prevents a slow reader from causing unbounded response accumulation. After a batch drains, an output buffer whose capacity exceeds 256 KiB is replaced with a fresh 64 KiB buffer so one large response does not pin its full allocation for the connection's lifetime. No new user-configurable memory limits are introduced.

## Logging behavior

Both implementations preserve the current log contents and `--quiet` semantics. Quiet mode must remain a genuine no-op path without request formatting, client statistics, or writes.

Enabled logging remains synchronous for this experiment and can therefore delay the single event loop if stderr blocks. Performance comparisons must use `--quiet`. Asynchronous or lossy logging would require an explicit delivery and overload policy and is outside this change.

## Test architecture

The external contract is protected by one black-box behavior suite executed against both server modes. It covers:

- PING, ECHO, SET, and GET;
- binary-safe keys and values;
- missing keys and overwrites;
- fragmented requests;
- pipelined requests and response ordering;
- wrong arity and unknown commands;
- malformed protocol input and connection-local rejection;
- EOF with complete and incomplete buffered input;
- input limits;
- quiet logging; and
- isolation between concurrent clients.

The threaded implementation retains its existing blocking-I/O, cursor, compaction, connection, and logging tests.

Event-loop-specific tests cover:

- accepting until `WouldBlock`;
- reading until `WouldBlock` and distinguishing EOF;
- partial writes and written-cursor resumption;
- adding and removing writable interest;
- close-after-flush protocol errors;
- response ordering across fragmented pipelines;
- pausing reads under output backpressure;
- resuming buffered input after output drains;
- releasing unusually large output-buffer capacity after a batch drains;
- command, byte, and accept budgets;
- internal continuation deduplication and fairness;
- cleanup after connection errors; and
- stale tokens never resolving to a new connection.

The event loop exposes a bounded internal poll/drive operation for deterministic tests. Production `run` calls it continuously; tests drive it with explicit timeouts and state predicates rather than timing sleeps. No production shutdown CLI is added.

Acceptance requires:

```text
cargo fmt --all
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

A real-binary smoke test starts each mode separately, verifies which process owns the port, exercises SET and GET, confirms response parity, and confirms quiet mode emits no stdout or stderr.

## Performance evaluation

Both modes are built once into the same release binary. Benchmarks alternate mode order and verify the selected process owns the target port before every run. The minimum matrix measures SET and GET separately with:

```text
1 and 50 clients
pipeline depths 1 and 16
default shared key and randomized keyspace
```

The report includes throughput, p50, and p99 latency, plus process CPU consumption where available. It distinguishes total throughput from throughput per CPU-second. Multiple alternating runs are required; single-run differences and changes within observed noise are not called improvements.

The event-loop implementation is successful as an experimental mode when it has behavioral parity, passes all verification, and yields reproducible measurements. The default does not change as part of this work. No minimum benchmark win is assumed, and a neutral or negative result is reported as such.

## Failure behavior and operability

A listener bind, poller, registry, or token-allocation failure terminates the selected server and follows the existing top-level quiet-mode error policy. Accept errors that indicate a listener-level failure terminate the server; per-connection read, write, decode, or registration failures close only that connection after any explicitly queued protocol response.

The event loop must not panic on stale readiness, missing connection tokens, checked-arithmetic failure, malformed input, cursor inconsistency, or partial I/O. Unexpected readiness for a missing token is ignored and may be logged only when logging is enabled.

The threaded mode remains available as a runtime fallback and behavioral reference. There is no automatic fallback from a failed event-loop startup to threaded mode because that would make the selected architecture ambiguous during testing and operation.

## Scope and future direction

This change does not:

- remove `RwLock` from the event-loop database;
- optimize request-part, response, or database allocations;
- change hashers;
- add asynchronous logging;
- add configurable addresses or ports;
- add graceful-shutdown CLI behavior;
- add new Redis commands or protocol forms;
- add a worker pool;
- execute commands on multiple cores;
- shard the keyspace; or
- change the default from `threaded`.

If the single event loop demonstrates a material per-core improvement but reaches an insufficient one-core capacity ceiling, multicore scaling becomes a separate design. The preferred direction is multiple event loops that exclusively own keyspace shards, with explicit routing and cross-shard semantics, rather than restoring thread-per-connection access to one globally locked database.

Keeping both modes permanently is not promised. Removal of the threaded baseline requires behavioral confidence, understood failure and backpressure behavior, and repeatable benchmark evidence from the event-loop mode.
