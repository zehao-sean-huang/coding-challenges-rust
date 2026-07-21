use super::super::connection_logging::EventLogger;
use crate::command;
use crate::database::Database;
use redis::resp::{DecodeErrorKind, Encoder, RequestDecoder};
use std::io::{self, Read, Write};

const READ_BUDGET: usize = 256 * 1024;
const COMMAND_BUDGET: usize = 64;
const WRITE_BUDGET: usize = 256 * 1024;
const OUTPUT_BATCH_TARGET: usize = 64 * 1024;
const MAX_RETAINED_OUTPUT: usize = 256 * 1024;
const NORMAL_OUTPUT_CAPACITY: usize = 64 * 1024;
const READ_CHUNK: usize = 8192;
const PROTOCOL_ERROR: &[u8] = b"-ERR Protocol error\r\n";
pub(in crate::server) const MAX_INCOMPLETE_BUFFER: usize = 536_870_912 + 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Readiness {
    pub(super) readable: bool,
    pub(super) writable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DesiredInterest {
    Readable,
    Writable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ServiceResult {
    pub(super) close: bool,
    pub(super) requeue: bool,
    pub(super) interest: Option<DesiredInterest>,
}

pub(super) struct Connection<S, L> {
    pub(super) socket: S,
    input: Vec<u8>,
    consumed: usize,
    output: Vec<u8>,
    written: usize,
    peer_closed: bool,
    close_after_flush: bool,
    read_continuation_pending: bool,
    write_continuation_pending: bool,
    queued: bool,
    desired_interest: Option<DesiredInterest>,
    buffer_limit: usize,
    pub(super) logger: L,
}

impl<S: Read + Write, L: EventLogger> Connection<S, L> {
    pub(super) fn new(socket: S, logger: L, buffer_limit: usize) -> Self {
        Self {
            socket,
            input: Vec::new(),
            consumed: 0,
            output: Vec::with_capacity(NORMAL_OUTPUT_CAPACITY),
            written: 0,
            peer_closed: false,
            close_after_flush: false,
            read_continuation_pending: false,
            write_continuation_pending: false,
            queued: false,
            desired_interest: Some(DesiredInterest::Readable),
            buffer_limit,
            logger,
        }
    }

    pub(super) fn desired_interest(&self) -> Option<DesiredInterest> {
        self.desired_interest
    }

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

    pub(super) fn service(
        &mut self,
        readiness: Readiness,
        database: &Database,
        decoder: &RequestDecoder,
        encoder: &Encoder,
    ) -> io::Result<ServiceResult> {
        let output_pending_before = self.output_pending()?;
        let mut requeue = false;

        if output_pending_before {
            if readiness.writable || self.write_continuation_pending {
                requeue |= self.write_available()?;
                if !self.output_pending()? && !self.close_after_flush {
                    requeue |= self.read_continuation_pending;
                    requeue |= self.has_user_space_work(decoder)?;
                }
            }
        } else {
            requeue |= self.read_available(readiness.readable)?;
            requeue |= self.dispatch_available(database, decoder, encoder)?;
            if self.output_pending()? {
                requeue |= self.write_available()?;
                if !self.output_pending()? && !self.close_after_flush {
                    requeue |= self.has_user_space_work(decoder)?;
                }
            }
        }

        let output_pending = self.output_pending()?;
        let close = !output_pending
            && (self.close_after_flush || (self.peer_closed && self.input.is_empty()));
        self.desired_interest = if close {
            None
        } else if output_pending {
            Some(DesiredInterest::Writable)
        } else {
            Some(DesiredInterest::Readable)
        };

        Ok(ServiceResult {
            close,
            requeue,
            interest: self.desired_interest,
        })
    }

    fn read_available(&mut self, readable: bool) -> io::Result<bool> {
        if (!readable && !self.read_continuation_pending)
            || self.peer_closed
            || self.close_after_flush
            || self.output_pending()?
        {
            return Ok(false);
        }

        let mut work = 0;
        let mut chunk = [0; READ_CHUNK];
        loop {
            if work == READ_BUDGET {
                self.read_continuation_pending = true;
                return Ok(true);
            }
            let available = READ_BUDGET.checked_sub(work).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "read work budget underflowed")
            })?;
            let chunk_len = available.min(chunk.len());
            match self.socket.read(&mut chunk[..chunk_len]) {
                Ok(0) => {
                    self.read_continuation_pending = false;
                    self.peer_closed = true;
                    return Ok(false);
                }
                Ok(read) => {
                    if read > chunk_len {
                        self.read_continuation_pending = false;
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "reader returned more bytes than requested",
                        ));
                    }
                    self.logger.received(read);
                    self.input.extend_from_slice(&chunk[..read]);
                    work = work.checked_add(read).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "read work counter overflowed")
                    })?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.read_continuation_pending = false;
                    return Ok(false);
                }
                Err(error) => {
                    self.read_continuation_pending = false;
                    return Err(error);
                }
            }
        }
    }

    fn dispatch_available(
        &mut self,
        database: &Database,
        decoder: &RequestDecoder,
        encoder: &Encoder,
    ) -> io::Result<bool> {
        if self.close_after_flush {
            return Ok(false);
        }

        let mut commands = 0;
        let mut discard_incomplete_eof = false;
        while commands < COMMAND_BUDGET && self.output.len() < OUTPUT_BATCH_TARGET {
            let remaining = self.input.get(self.consumed..).ok_or_else(cursor_error)?;
            if remaining.is_empty() {
                break;
            }

            match decoder.decode(remaining) {
                Ok(decoded) if decoded.parts.is_empty() => {
                    self.logger.protocol_error("invalid command framing");
                    self.reject_protocol();
                    break;
                }
                Ok(decoded) => {
                    let next = advance_cursor(self.consumed, decoded.consumed, self.input.len())?;
                    self.logger.request(&decoded.parts);
                    let response = command::dispatch(&decoded.parts, database);
                    encoder
                        .encode(&response, &mut self.output)
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    self.logger.response(&response);
                    self.consumed = next;
                    commands += 1;
                }
                Err(error) if error.kind == DecodeErrorKind::IncompleteInput => {
                    if self.peer_closed {
                        discard_incomplete_eof = true;
                    } else if remaining.len() > self.buffer_limit {
                        self.logger.incomplete_buffer(self.buffer_limit);
                        self.reject_protocol();
                    }
                    break;
                }
                Err(error) if error.kind == DecodeErrorKind::InvalidCommandFraming => {
                    self.logger.protocol_error("invalid command framing");
                    self.reject_protocol();
                    break;
                }
                Err(error) => {
                    self.logger.decode_error(&error);
                    self.reject_protocol();
                    break;
                }
            }
        }

        if self.close_after_flush {
            self.input.clear();
            self.consumed = 0;
        } else {
            self.compact_input()?;
            if discard_incomplete_eof {
                self.input.clear();
            }
        }
        Ok(commands == COMMAND_BUDGET && self.has_user_space_work(decoder)?)
    }

    fn write_available(&mut self) -> io::Result<bool> {
        match self.write_available_inner() {
            Ok(requeue) => {
                self.write_continuation_pending = requeue;
                Ok(requeue)
            }
            Err(error) => {
                self.write_continuation_pending = false;
                Err(error)
            }
        }
    }

    fn write_available_inner(&mut self) -> io::Result<bool> {
        let mut work = 0;
        while self.output_pending()? && work < WRITE_BUDGET {
            let remaining_budget = WRITE_BUDGET.checked_sub(work).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "write work budget underflowed")
            })?;
            let remaining = self.output.get(self.written..).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response buffer cursor exceeds buffer length",
                )
            })?;
            let offered = remaining.len().min(remaining_budget);
            match self.socket.write(&remaining[..offered]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(written) => {
                    if written > offered {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "writer accepted more bytes than offered",
                        ));
                    }
                    self.written = self.written.checked_add(written).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "response buffer cursor overflowed",
                        )
                    })?;
                    work = work.checked_add(written).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "write work counter overflowed")
                    })?;
                    self.logger.sent(written);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }

        if self.written == self.output.len() {
            self.output.clear();
            self.written = 0;
            if self.output.capacity() > MAX_RETAINED_OUTPUT {
                self.output = Vec::with_capacity(NORMAL_OUTPUT_CAPACITY);
            }
        }
        Ok(work == WRITE_BUDGET && self.output_pending()?)
    }

    fn has_user_space_work(&self, decoder: &RequestDecoder) -> io::Result<bool> {
        let remaining = self.input.get(self.consumed..).ok_or_else(cursor_error)?;
        if remaining.is_empty() {
            return Ok(false);
        }

        match decoder.decode(remaining) {
            Err(error) if error.kind == DecodeErrorKind::IncompleteInput => {
                Ok(self.peer_closed || remaining.len() > self.buffer_limit)
            }
            _ => Ok(true),
        }
    }

    fn compact_input(&mut self) -> io::Result<()> {
        let remaining = self
            .input
            .len()
            .checked_sub(self.consumed)
            .ok_or_else(cursor_error)?;
        if self.consumed == 0 {
            return Ok(());
        }
        if remaining == 0 {
            self.input.clear();
        } else {
            self.input.copy_within(self.consumed.., 0);
            self.input.truncate(remaining);
        }
        self.consumed = 0;
        Ok(())
    }

    fn output_pending(&self) -> io::Result<bool> {
        self.output
            .len()
            .checked_sub(self.written)
            .map(|remaining| remaining != 0)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response buffer cursor exceeds buffer length",
                )
            })
    }

    fn reject_protocol(&mut self) {
        self.output.extend_from_slice(PROTOCOL_ERROR);
        self.close_after_flush = true;
    }
}

fn advance_cursor(cursor: usize, decoded: usize, buffer_len: usize) -> io::Result<usize> {
    let remaining = buffer_len.checked_sub(cursor).ok_or_else(cursor_error)?;
    if decoded == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP decoder made no progress",
        ));
    }
    if decoded > remaining {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP decoder consumed beyond its input",
        ));
    }
    cursor.checked_add(decoded).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "request buffer cursor overflowed",
        )
    })
}

fn cursor_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "request buffer cursor exceeds buffer length",
    )
}

#[cfg(test)]
mod tests {
    use super::{
        Connection, DesiredInterest, MAX_RETAINED_OUTPUT, NORMAL_OUTPUT_CAPACITY,
        OUTPUT_BATCH_TARGET, READ_BUDGET, Readiness, WRITE_BUDGET,
    };
    use crate::database::Database;
    use crate::server::connection_logging::DisabledLogger;
    use redis::resp::{Encoder, RequestDecoder};
    use std::collections::VecDeque;
    use std::io::{self, Read, Write};

    const TEST_LIMIT: usize = 1024;

    enum ReadAction {
        Bytes(Vec<u8>),
        WouldBlock,
        Eof,
    }

    enum WriteAction {
        Accept(usize),
        WouldBlock,
    }

    struct ScriptedIo {
        reads: VecDeque<ReadAction>,
        written: Vec<u8>,
        max_write: usize,
        block_writes: bool,
        writes: VecDeque<WriteAction>,
        read_calls: usize,
    }

    impl ScriptedIo {
        fn new(reads: impl IntoIterator<Item = ReadAction>) -> Self {
            Self {
                reads: reads.into_iter().collect(),
                written: Vec::new(),
                max_write: usize::MAX,
                block_writes: false,
                writes: VecDeque::new(),
                read_calls: 0,
            }
        }
    }

    fn service_readable(
        connection: &mut Connection<ScriptedIo, DisabledLogger>,
        database: &Database,
    ) -> io::Result<super::ServiceResult> {
        connection.service(
            Readiness {
                readable: true,
                writable: false,
            },
            database,
            &RequestDecoder::default(),
            &Encoder::default(),
        )
    }

    impl Read for ScriptedIo {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.read_calls += 1;
            match self.reads.pop_front().unwrap_or(ReadAction::WouldBlock) {
                ReadAction::Bytes(bytes) => {
                    let copied = bytes.len().min(buffer.len());
                    buffer[..copied].copy_from_slice(&bytes[..copied]);
                    if copied < bytes.len() {
                        self.reads
                            .push_front(ReadAction::Bytes(bytes[copied..].to_vec()));
                    }
                    Ok(copied)
                }
                ReadAction::WouldBlock => Err(io::ErrorKind::WouldBlock.into()),
                ReadAction::Eof => Ok(0),
            }
        }
    }

    impl Write for ScriptedIo {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            if let Some(action) = self.writes.pop_front() {
                return match action {
                    WriteAction::Accept(limit) => {
                        let written = buffer.len().min(limit);
                        self.written.extend_from_slice(&buffer[..written]);
                        Ok(written)
                    }
                    WriteAction::WouldBlock => Err(io::ErrorKind::WouldBlock.into()),
                };
            }
            if self.block_writes {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let written = buffer.len().min(self.max_write);
            self.written.extend_from_slice(&buffer[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn partial_write_preserves_remaining_output_and_interest() {
        let mut socket = ScriptedIo::new([
            ReadAction::Bytes(b"*1\r\n$4\r\nPING\r\n".to_vec()),
            ReadAction::WouldBlock,
        ]);
        socket.writes = [WriteAction::Accept(3), WriteAction::WouldBlock]
            .into_iter()
            .collect();
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        let database = Database::default();
        let decoder = RequestDecoder::default();
        let encoder = Encoder::default();

        let first = connection
            .service(
                Readiness {
                    readable: true,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();

        assert_eq!(first.interest, Some(DesiredInterest::Writable));
        assert_eq!(connection.desired_interest(), first.interest);
        assert_eq!(&connection.output[connection.written..], b"NG\r\n");
        assert!(!first.close);

        let second = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();

        assert_eq!(connection.socket.written, b"+PONG\r\n");
        assert_eq!(second.interest, Some(DesiredInterest::Readable));
        assert_eq!(connection.desired_interest(), second.interest);
    }

    #[test]
    fn continuation_enqueue_is_deduplicated_until_cleared() {
        let mut connection = Connection::new(ScriptedIo::new([]), DisabledLogger, TEST_LIMIT);

        assert!(connection.mark_queued());
        assert!(!connection.mark_queued());
        connection.clear_queued();
        assert!(connection.mark_queued());
    }

    #[test]
    fn command_budget_preserves_blocked_pipeline_for_later_continuation() {
        let ping = b"*1\r\n$4\r\nPING\r\n";
        let pipeline = ping.repeat(65);
        let mut socket = ScriptedIo::new([
            ReadAction::Bytes(pipeline),
            ReadAction::WouldBlock,
            ReadAction::Bytes(ping.to_vec()),
        ]);
        socket.block_writes = true;
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT * 2);
        let database = Database::default();
        let decoder = RequestDecoder::default();
        let encoder = Encoder::default();

        let first = connection
            .service(
                Readiness {
                    readable: true,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        let reads_after_first = connection.socket.read_calls;

        assert!(first.requeue);
        assert_eq!(first.interest, Some(DesiredInterest::Writable));
        assert_eq!(connection.output.len(), 64 * b"+PONG\r\n".len());
        assert!(connection.socket.written.is_empty());

        let blocked_continuation = connection
            .service(
                Readiness {
                    readable: true,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(!blocked_continuation.requeue);
        assert_eq!(connection.socket.read_calls, reads_after_first);
        assert_eq!(connection.output.len(), 64 * b"+PONG\r\n".len());

        connection.socket.block_writes = false;
        let drained = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(drained.requeue);
        assert_eq!(drained.interest, Some(DesiredInterest::Readable));
        assert_eq!(connection.socket.written, b"+PONG\r\n".repeat(64));

        let resumed = connection
            .service(
                Readiness {
                    readable: false,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(!resumed.close);
        assert_eq!(resumed.interest, Some(DesiredInterest::Readable));
        assert_eq!(connection.socket.written, b"+PONG\r\n".repeat(65));
    }

    #[test]
    fn exact_read_budget_requeues_before_would_block() {
        let prefix = b"*2\r\n$4\r\nECHO\r\n$262144\r\n";
        let mut incomplete = Vec::with_capacity(READ_BUDGET);
        incomplete.extend_from_slice(prefix);
        incomplete.resize(READ_BUDGET, b'x');
        let socket = ScriptedIo::new([ReadAction::Bytes(incomplete), ReadAction::WouldBlock]);
        let mut connection = Connection::new(socket, DisabledLogger, READ_BUDGET * 2);

        let result = service_readable(&mut connection, &Database::default()).unwrap();

        assert!(result.requeue);
        assert_eq!(
            connection.socket.read_calls,
            READ_BUDGET / super::READ_CHUNK
        );
        assert!(connection.socket.written.is_empty());
        assert_eq!(result.interest, Some(DesiredInterest::Readable));
    }

    #[test]
    fn blocked_output_preserves_exact_read_budget_continuation() {
        let value_len = READ_BUDGET - 31;
        let mut exact_request =
            format!("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n${value_len}\r\n").into_bytes();
        exact_request.resize(exact_request.len() + value_len, b'x');
        exact_request.extend_from_slice(b"\r\n");
        assert_eq!(exact_request.len(), READ_BUDGET);

        let ping = b"*1\r\n$4\r\nPING\r\n";
        let mut socket = ScriptedIo::new([
            ReadAction::Bytes(exact_request),
            ReadAction::Bytes(ping.to_vec()),
            ReadAction::WouldBlock,
        ]);
        socket.block_writes = true;
        let mut connection = Connection::new(socket, DisabledLogger, READ_BUDGET * 2);
        let database = Database::default();
        let decoder = RequestDecoder::default();
        let encoder = Encoder::default();

        let first = connection
            .service(
                Readiness {
                    readable: true,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(first.requeue);
        assert_eq!(first.interest, Some(DesiredInterest::Writable));
        assert!(connection.socket.written.is_empty());

        connection.socket.block_writes = false;
        let drained = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(drained.requeue);
        assert_eq!(drained.interest, Some(DesiredInterest::Readable));
        assert_eq!(connection.socket.written, b"+OK\r\n");

        let resumed = connection
            .service(
                Readiness {
                    readable: false,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(!resumed.requeue);
        assert_eq!(resumed.interest, Some(DesiredInterest::Readable));
        assert_eq!(connection.socket.written, b"+OK\r\n+PONG\r\n");
    }

    #[test]
    fn exact_write_budget_requeues_before_would_block() {
        let socket = ScriptedIo::new([]);
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        connection.output = vec![b'x'; WRITE_BUDGET + 1];

        let result = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &Database::default(),
                &RequestDecoder::default(),
                &Encoder::default(),
            )
            .unwrap();

        assert!(result.requeue);
        assert_eq!(connection.socket.written.len(), WRITE_BUDGET);
        assert_eq!(&connection.output[connection.written..], b"x");
        assert_eq!(result.interest, Some(DesiredInterest::Writable));
    }

    #[test]
    fn write_budget_continuation_resumes_without_writable_readiness() {
        let socket = ScriptedIo::new([]);
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        connection.output = vec![b'x'; WRITE_BUDGET + 1];
        connection.close_after_flush = true;
        let database = Database::default();
        let decoder = RequestDecoder::default();
        let encoder = Encoder::default();

        let first = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(first.requeue);
        assert!(!first.close);
        assert_eq!(first.interest, Some(DesiredInterest::Writable));
        assert_eq!(connection.socket.written.len(), WRITE_BUDGET);

        let resumed = connection
            .service(
                Readiness {
                    readable: false,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert_eq!(connection.socket.written.len(), WRITE_BUDGET + 1);
        assert!(!resumed.requeue);
        assert!(resumed.close);
        assert_eq!(resumed.interest, None);
    }

    #[test]
    fn incomplete_input_after_would_block_does_not_requeue() {
        let socket = ScriptedIo::new([
            ReadAction::Bytes(b"*1\r\n$4\r\nPI".to_vec()),
            ReadAction::WouldBlock,
        ]);
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);

        let result = service_readable(&mut connection, &Database::default()).unwrap();

        assert!(!result.requeue);
        assert_eq!(result.interest, Some(DesiredInterest::Readable));
    }

    #[test]
    fn oversized_output_releases_excess_capacity_after_drain() {
        let socket = ScriptedIo::new([]);
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        connection.output = vec![b'x'; MAX_RETAINED_OUTPUT + 1];

        let first = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &Database::default(),
                &RequestDecoder::default(),
                &Encoder::default(),
            )
            .unwrap();
        assert!(first.requeue);

        let drained = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &Database::default(),
                &RequestDecoder::default(),
                &Encoder::default(),
            )
            .unwrap();

        assert_eq!(connection.output.len(), 0);
        assert!(connection.output.capacity() >= NORMAL_OUTPUT_CAPACITY);
        assert!(connection.output.capacity() <= MAX_RETAINED_OUTPUT);
        assert_eq!(drained.interest, Some(DesiredInterest::Readable));
    }

    #[test]
    fn response_larger_than_batch_target_is_written() {
        let payload = vec![b'x'; OUTPUT_BATCH_TARGET + 1];
        let mut request = format!("*2\r\n$4\r\nECHO\r\n${}\r\n", payload.len()).into_bytes();
        request.extend_from_slice(&payload);
        request.extend_from_slice(b"\r\n");
        let socket = ScriptedIo::new([ReadAction::Bytes(request), ReadAction::WouldBlock]);
        let mut connection = Connection::new(socket, DisabledLogger, READ_BUDGET);

        let result = service_readable(&mut connection, &Database::default()).unwrap();

        let mut expected = format!("${}\r\n", payload.len()).into_bytes();
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(connection.socket.written, expected);
        assert_eq!(result.interest, Some(DesiredInterest::Readable));
    }

    #[test]
    fn fragmented_pipeline_resumes_without_losing_order() {
        let socket = ScriptedIo::new([
            ReadAction::Bytes(b"*1\r\n$4\r\nPI".to_vec()),
            ReadAction::WouldBlock,
            ReadAction::Bytes(b"NG\r\n*2\r\n$4\r\nECHO\r\n$3\r\none\r\n".to_vec()),
            ReadAction::WouldBlock,
        ]);
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        let database = Database::default();
        let decoder = RequestDecoder::default();
        let encoder = Encoder::default();

        let first = connection
            .service(
                Readiness {
                    readable: true,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(!first.close);
        assert!(!first.requeue);
        assert_eq!(first.interest, Some(DesiredInterest::Readable));
        assert!(connection.socket.written.is_empty());

        let second = connection
            .service(
                Readiness {
                    readable: true,
                    writable: false,
                },
                &database,
                &decoder,
                &encoder,
            )
            .unwrap();
        assert!(!second.close);
        assert!(!second.requeue);
        assert_eq!(second.interest, Some(DesiredInterest::Readable));
        assert_eq!(connection.socket.written, b"+PONG\r\n$3\r\none\r\n");
    }

    #[test]
    fn pipelined_set_then_get_observes_the_write() {
        let socket = ScriptedIo::new([
            ReadAction::Bytes(
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
                    .to_vec(),
            ),
            ReadAction::WouldBlock,
        ]);
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        let result = service_readable(&mut connection, &Database::default()).unwrap();

        assert_eq!(connection.socket.written, b"+OK\r\n$5\r\nvalue\r\n");
        assert!(!result.close);
        assert_eq!(result.interest, Some(DesiredInterest::Readable));
    }

    #[test]
    fn eof_after_complete_request_flushes_response_before_close() {
        let mut socket = ScriptedIo::new([
            ReadAction::Bytes(b"*1\r\n$4\r\nPING\r\n".to_vec()),
            ReadAction::Eof,
        ]);
        socket.max_write = 2;
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        let result = service_readable(&mut connection, &Database::default()).unwrap();

        assert_eq!(connection.socket.written, b"+PONG\r\n");
        assert!(result.close);
        assert_eq!(result.interest, None);
    }

    #[test]
    fn eof_with_incomplete_request_closes_silently() {
        let socket = ScriptedIo::new([
            ReadAction::Bytes(b"*1\r\n$4\r\nPI".to_vec()),
            ReadAction::Eof,
        ]);
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        let result = service_readable(&mut connection, &Database::default()).unwrap();

        assert!(connection.socket.written.is_empty());
        assert!(result.close);
        assert_eq!(result.interest, None);
    }

    #[test]
    fn malformed_framing_queues_exact_protocol_error_and_closes_after_flush() {
        let mut socket =
            ScriptedIo::new([ReadAction::Bytes(b"?\r\n".to_vec()), ReadAction::WouldBlock]);
        socket.block_writes = true;
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        let database = Database::default();

        let blocked = service_readable(&mut connection, &database).unwrap();
        assert!(connection.socket.written.is_empty());
        assert!(!blocked.close);
        assert_eq!(blocked.interest, Some(DesiredInterest::Writable));

        connection.socket.block_writes = false;
        let flushed = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &database,
                &RequestDecoder::default(),
                &Encoder::default(),
            )
            .unwrap();
        assert_eq!(connection.socket.written, b"-ERR Protocol error\r\n");
        assert!(flushed.close);
        assert_eq!(flushed.interest, None);
    }

    #[test]
    fn pending_output_waits_for_writable_readiness() {
        let mut socket = ScriptedIo::new([
            ReadAction::Bytes(b"*1\r\n$4\r\nPING\r\n".to_vec()),
            ReadAction::WouldBlock,
        ]);
        socket.block_writes = true;
        let mut connection = Connection::new(socket, DisabledLogger, TEST_LIMIT);
        let database = Database::default();

        let blocked = service_readable(&mut connection, &database).unwrap();
        assert_eq!(blocked.interest, Some(DesiredInterest::Writable));
        connection.socket.block_writes = false;

        let readable_only = service_readable(&mut connection, &database).unwrap();
        assert!(connection.socket.written.is_empty());
        assert!(!readable_only.requeue);
        assert_eq!(readable_only.interest, Some(DesiredInterest::Writable));

        let writable = connection
            .service(
                Readiness {
                    readable: false,
                    writable: true,
                },
                &database,
                &RequestDecoder::default(),
                &Encoder::default(),
            )
            .unwrap();
        assert_eq!(connection.socket.written, b"+PONG\r\n");
        assert_eq!(writable.interest, Some(DesiredInterest::Readable));
    }

    #[test]
    fn connection_protocol_error_does_not_mutate_another_connection_or_database() {
        let database = Database::default();
        database.set(b"stable".to_vec(), b"value".to_vec());
        let bad_socket = ScriptedIo::new([ReadAction::Bytes(b"*0\r\n".to_vec()), ReadAction::Eof]);
        let mut bad = Connection::new(bad_socket, DisabledLogger, TEST_LIMIT);
        let bad_result = service_readable(&mut bad, &database).unwrap();

        assert_eq!(bad.socket.written, b"-ERR Protocol error\r\n");
        assert!(bad_result.close);
        assert_eq!(database.get(b"stable"), Some(b"value".to_vec()));

        let good_socket = ScriptedIo::new([
            ReadAction::Bytes(b"*2\r\n$3\r\nGET\r\n$6\r\nstable\r\n".to_vec()),
            ReadAction::WouldBlock,
        ]);
        let mut good = Connection::new(good_socket, DisabledLogger, TEST_LIMIT);
        let good_result = service_readable(&mut good, &database).unwrap();

        assert_eq!(good.socket.written, b"$5\r\nvalue\r\n");
        assert!(!good_result.close);
        assert_eq!(good_result.interest, Some(DesiredInterest::Readable));
    }
}
