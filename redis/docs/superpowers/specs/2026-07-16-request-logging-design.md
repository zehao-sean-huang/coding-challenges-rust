# Human-Readable Request Logging Design

## Goal

Make the TCP server's activity visible to a learner without changing RESP behavior or adding dependencies.

## Log content

The server writes one human-readable line at a time to stderr. Startup logs show the bound address. Each accepted connection receives a monotonic process-local identity such as `client-0001` and logs its peer and local socket addresses.

For every valid request, the server logs the command and arguments. Printable bytes remain readable; quotes, backslashes, control bytes, and non-ASCII bytes are escaped. Rendered values are capped at 120 characters and end in an ellipsis when truncated. Responses are summarized as simple strings, errors, or bulk strings with their byte length and escaped payload.

Protocol errors identify the client and include a concise reason. Connection completion logs include the request count, received and sent byte totals, and elapsed duration. I/O failures use the same metadata and terminate only that connection.

## Architecture

Add a binary-owned `logging` module next to `command` and `server`. It owns client identities, connection statistics, byte rendering, request/response summaries, and individual log-line formatting. Its formatting functions are pure and unit-testable.

The listener logs startup. Each connection thread creates its identity from `peer_addr` and `local_addr`, emits a connection line, and passes the identity plus mutable statistics into the existing connection loop. Reads and successful writes update byte counters. The loop logs each decoded request before dispatch and its response after dispatch. A final lifecycle line is emitted whether the loop ends normally or with an I/O error.

## Error handling

Logging is observational: it never alters bytes sent to clients. stderr write failures are ignored so logging cannot terminate a client connection. Protocol-error responses retain their exact existing wire representation. Payloads are escaped before logging and no UTF-8 assumption is made.

## Tests

Unit tests verify client identity formatting, binary escaping, the 120-character truncation boundary, request and response summaries, and connection-stat formatting. Existing connection tests continue to verify fragmentation, pipelining, persistence, concurrency, EOF, protocol errors, and write failures. Final acceptance remains `cargo test --all-targets` and strict all-target Clippy.

## Scope

No logging framework, configuration flags, log levels, structured JSON, timestamps, authentication redaction, file output, or log rotation are added.
