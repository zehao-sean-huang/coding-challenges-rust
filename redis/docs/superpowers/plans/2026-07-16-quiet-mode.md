# Quiet Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--quiet` startup option whose request path performs no log formatting, accounting, timing, or stderr writes.

**Architecture:** `main` parses arguments into a small configuration and passes a `LogMode` to the server. The server branches once per connection between generic enabled and disabled logger implementations, allowing Rust to monomorphize the disabled logger's no-op event methods out of the hot path.

**Tech Stack:** Rust 2024 standard library and the existing RESP server.

## Global Constraints

- Keep runtime dependencies empty.
- Preserve current logging when no argument is supplied.
- `--quiet` suppresses startup, lifecycle, request, response, protocol-error, and I/O-error logs.
- Quiet connections do not create logging-only client metadata, statistics, timers, rendered strings, or stderr handles.
- Unknown arguments fail before listener binding with usage text and a nonzero exit status.
- Do not change RESP decoding, dispatch, database access, encoding, or client-visible bytes.

---

### Task 1: Parse `--quiet` and plumb logging mode to the server

**Files:**
- Modify: `src/main.rs`
- Modify: `src/logging.rs`
- Modify: `src/server.rs`

**Interfaces:**
- Produces: `logging::LogMode::{Enabled, Disabled}`.
- Produces: `Config::parse<I, S>(arguments: I) -> Result<Config, String>` for iterators of `AsRef<OsStr>` values.
- Changes: `server::run(address: &str, log_mode: LogMode) -> io::Result<()>`.

- [x] **Step 1: Write failing argument parser tests**

Add tests in `src/main.rs` requiring no arguments to select `LogMode::Enabled`, `--quiet` to select `LogMode::Disabled`, and an unknown argument to return `unknown argument '--verbose'\nUsage: redis [--quiet]`.

```rust
#[test]
fn logging_is_enabled_by_default() {
    assert_eq!(
        Config::parse(std::iter::empty::<&str>()).unwrap().log_mode,
        LogMode::Enabled
    );
}

#[test]
fn quiet_disables_logging() {
    assert_eq!(
        Config::parse(["--quiet"]).unwrap().log_mode,
        LogMode::Disabled
    );
}

#[test]
fn unknown_argument_is_rejected_with_usage() {
    assert_eq!(
        Config::parse(["--verbose"]).unwrap_err(),
        "unknown argument '--verbose'\nUsage: redis [--quiet]"
    );
}
```

- [x] **Step 2: Run focused tests and verify RED**

Run: `cargo test --bin redis tests::`

Expected: compilation fails because `Config`, `LogMode`, and the new `server::run` parameter do not exist.

- [x] **Step 3: Implement minimal parsing and plumbing**

Define a copyable `LogMode` in `src/logging.rs`. In `src/main.rs`, parse `env::args_os().skip(1)`, allow exactly zero arguments or one `--quiet`, print argument/server errors to stderr, and return `ExitCode::FAILURE` on error. Change the server entrypoint to accept `LogMode` and guard the startup line:

```rust
if log_mode == LogMode::Enabled {
    eprintln!("[redis] listening address={bound_address}");
}
```

Copy `log_mode` into each spawned connection closure.

- [x] **Step 4: Run focused tests and verify GREEN**

Run: `cargo test --bin redis tests::`

Expected: all `main.rs` tests pass.

### Task 2: Compile logging work out of quiet connections

**Files:**
- Modify: `src/server.rs`
- Test: unit tests in `src/server.rs`

**Interfaces:**
- Produces: private `EventLogger` trait with default no-op event methods.
- Produces: private zero-sized `DisabledLogger`.
- Produces: private `EnabledLogger<W: Write>` owning `ClientIdentity`, `ConnectionStats`, `Instant`, and its writer.
- Changes: the generic connection loop to `handle_io<T: Read + Write, L: EventLogger>(stream: &mut T, buffer_limit: usize, database: &Database, logger: &mut L) -> io::Result<()>`.

- [x] **Step 1: Write a failing quiet-path test**

Add a `DisabledLogger` test proving a PING request returns the exact response while the logger remains zero-sized:

```rust
#[test]
fn disabled_logger_is_zero_sized_and_preserves_responses() {
    assert_eq!(std::mem::size_of::<DisabledLogger>(), 0);
    let mut io = TestIo {
        reader: Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec()),
        written: Vec::new(),
    };
    handle_io(
        &mut io,
        TEST_LIMIT,
        &Database::default(),
        &mut DisabledLogger,
    )
    .unwrap();
    assert_eq!(io.written, b"+PONG\r\n");
}
```

Retain enabled-path assertions for request/response content and exact received/sent counters by constructing `EnabledLogger<Vec<u8>>` and inspecting its writer and statistics after the call.

- [x] **Step 2: Run the focused test and verify RED**

Run: `cargo test --bin redis server::tests::disabled_logger_is_zero_sized_and_preserves_responses`

Expected: compilation fails because `DisabledLogger` and the logger-generic `handle_io` do not exist.

- [x] **Step 3: Implement static logger dispatch**

Define no-op methods on `EventLogger` for received/sent byte accounting and request/response/protocol events. Override them in `EnabledLogger<W>` to update `ConnectionStats`, call the existing pure formatting helpers, and write lines. Add an enabled-only completion method that emits disconnected or I/O-error output.

In `handle_connection`, branch once on `LogMode`. The enabled branch constructs socket metadata, a timer, statistics, and stderr-backed `EnabledLogger`; the disabled branch constructs only `DisabledLogger`. Both invoke the same generic connection loop, producing separate monomorphized machine-code paths.

Move accounting calls to logger events at the existing successful read/write boundaries. Route protocol, request, and response events through the logger. Do not construct formatted strings outside `EnabledLogger` methods.

- [x] **Step 4: Run binary tests and verify GREEN**

Run: `cargo test --bin redis`

Expected: every server, logging, command, database, and argument test passes.

### Task 3: Acceptance verification

**Files:**
- Verify: `src/main.rs`
- Verify: `src/logging.rs`
- Verify: `src/server.rs`

**Interfaces:**
- Consumes: completed quiet-mode implementation.
- Produces: formatted, tested, lint-clean code and runtime evidence.

- [x] **Step 1: Format and run all tests**

Run: `cargo fmt --all && cargo test --all-targets`

Expected: formatting succeeds and all tests pass.

- [x] **Step 2: Run strict linting**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: Clippy exits successfully with no warnings.

- [x] **Step 3: Smoke-test the quiet binary**

Build the binary, start `target/debug/redis --quiet` with stderr captured in a temporary directory, send `*1\r\n$4\r\nPING\r\n`, stop the process, and inspect the capture.

Expected: the client receives `+PONG\r\n` and the stderr capture is empty.
