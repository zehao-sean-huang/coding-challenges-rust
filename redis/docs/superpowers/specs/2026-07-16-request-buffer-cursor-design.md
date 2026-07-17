# Request Buffer Cursor Design

## Goal

Process pipelined requests without shifting the unread tail after every decoded command, while preserving fragmentation behavior, protocol limits, and server availability.

## Buffer algorithm

Each read batch maintains a `consumed` cursor into the connection's existing `Vec<u8>`. The decoder receives only `buffer[consumed..]`. A successful decode advances the cursor without mutating the buffer, and request dispatch plus response writing finish before the next decode.

Compaction occurs once when the decode pass stops. If `consumed` is zero, the buffer is unchanged. If `consumed == buffer.len()`, `clear()` resets its length without copying. If an incomplete tail remains, `copy_within(consumed.., 0)` moves that tail once and `truncate()` removes the processed prefix.

The incomplete-request limit applies only to `buffer.len() - consumed`. Already-processed pipeline bytes do not count toward the live partial request.

## Safety invariants

Before decoding, the cursor must identify a valid suffix of the buffer. Each successful decoder result must consume at least one byte and no more than the supplied suffix. Cursor advancement uses checked arithmetic, and compaction validates the cursor before any range operation.

An invalid cursor or decoder consumption count returns `io::ErrorKind::InvalidData`. The affected connection thread terminates; the listener and other clients continue. No invariant violation is allowed to become an indexing panic or infinite decode loop.

Buffer mutation occurs only after decoded request data and its response are fully owned and processed. This is compatible with the current owning RESP decoder. A future borrowing decoder must drop all request borrows before compaction.

## Availability constraints

This change does not alter the configured incomplete-buffer limit or allocation policy. `Vec::clear()` and `truncate()` retain capacity, so a connection that accumulates a large request retains that allocation until it disconnects. The existing per-connection limit and thread-per-connection architecture remain separate availability concerns.

EOF with an incomplete request, protocol-error responses, wire bytes, logging behavior, and connection isolation remain unchanged.

## Tests and measurement

Unit tests cover cursor validation for zero progress and consumption beyond the remaining suffix, full-consumption clearing, no-op compaction, and preservation of an incomplete tail. A connection test supplies multiple complete pipelined requests followed by a fragmented request and verifies all responses remain ordered.

Acceptance requires rustfmt, all-target tests, strict Clippy, and the same release benchmark used for the baseline. The primary performance comparison is `redis-benchmark -t SET,GET -q -n 100000 -c 50 -P 16`; the non-pipelined benchmark is rerun to check for regressions.

## Scope

No ring buffer, zero-copy RESP values, database change, socket change, dependency, or revised memory limit is introduced.
