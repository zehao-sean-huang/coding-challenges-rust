# Quiet Mode Design

## Goal

Add a `--quiet` startup option that removes all server logging overhead from the request path so performance benchmarks measure command processing rather than human-readable diagnostics.

## Command-line behavior

Running `redis` without arguments preserves the current logging behavior. Running `redis --quiet` suppresses every server log, including startup, connection lifecycle, request, response, protocol-error, and I/O-error lines.

The binary parses arguments with the standard library and adds no dependency. Unknown arguments fail startup with a concise error and usage text. No short option, environment variable, log level, or configuration file is added.

## Architecture

`main` parses its process arguments into an application configuration containing the logging mode, then passes that mode to `server::run`.

The server owns a logger with enabled and disabled variants. Event methods such as connection, request, response, protocol error, and disconnect first inspect the variant. Only the enabled variant creates client metadata, statistics, timers, rendered strings, or a stderr handle. The disabled variant performs no formatting and no writes. This keeps logging policy out of command dispatch while avoiding the cost of redirecting fully formatted logs to an output sink.

The logging mode is copied into each connection thread. RESP decoding, command dispatch, database access, response encoding, and client-visible bytes remain unchanged.

## Error handling

As today, failures while writing logs do not affect client connections. Quiet mode also suppresses I/O-error diagnostics. Argument errors occur before binding the listener and return a nonzero process status.

## Tests

Unit tests cover default argument parsing, `--quiet`, and rejection of unknown arguments. Logger tests prove disabled event methods produce no output and leave logging-only accounting absent. Existing server tests continue to cover the enabled path and exact RESP behavior.

Acceptance requires `cargo fmt --all`, `cargo test --all-targets`, and strict all-target Clippy. A manual smoke check starts the server with `--quiet`, sends a request, and confirms stderr stays empty while the RESP response is unchanged.

## Scope

This change does not add configurable addresses, help/version output, partial log categories, dynamic runtime toggling, asynchronous logging, or a benchmarking harness.
