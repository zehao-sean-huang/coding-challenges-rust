use crate::command;
use crate::database::Database;
use crate::logging::{
    ClientIdentity, ConnectionStats, LogMode, connected_line, disconnected_line, io_error_line,
    protocol_error_line, request_line, response_line,
};
use redis::resp::{DecodeError, DecodeErrorKind, Decoder, Encoder, RespValue};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

const PROTOCOL_ERROR: &[u8] = b"-ERR Protocol error\r\n";
const MAX_INCOMPLETE_BUFFER: usize = 536_870_912 + 64;
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn run(address: &str, log_mode: LogMode) -> io::Result<()> {
    let listener = TcpListener::bind(address)?;
    let database = Arc::new(Database::default());
    let bound_address = listener.local_addr()?;
    if log_mode == LogMode::Enabled {
        eprintln!("[redis] listening address={bound_address}");
    }
    for stream in listener.incoming() {
        let stream = stream?;
        let database = database.clone();
        drop(thread::spawn(move || {
            let _ = handle_connection(stream, MAX_INCOMPLETE_BUFFER, database, log_mode);
        }));
    }
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    buffer_limit: usize,
    database: Arc<Database>,
    log_mode: LogMode,
) -> io::Result<()> {
    match log_mode {
        LogMode::Enabled => {
            let client = ClientIdentity {
                id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
                peer: socket_description(stream.peer_addr()),
                local: socket_description(stream.local_addr()),
            };
            let mut logger = EnabledLogger::new(client, io::stderr());
            logger.connected();
            let result = handle_io(&mut stream, buffer_limit, &database, &mut logger);
            logger.finished(&result);
            result
        }
        LogMode::Disabled => handle_io(&mut stream, buffer_limit, &database, &mut DisabledLogger),
    }
}

trait EventLogger {
    #[inline(always)]
    fn received(&mut self, _bytes: usize) {}

    #[inline(always)]
    fn sent(&mut self, _bytes: usize) {}

    #[inline(always)]
    fn request(&mut self, _parts: &[Vec<u8>]) {}

    #[inline(always)]
    fn response(&mut self, _response: &RespValue) {}

    #[inline(always)]
    fn protocol_error(&mut self, _reason: &str) {}

    #[inline(always)]
    fn incomplete_buffer(&mut self, _buffer_limit: usize) {}

    #[inline(always)]
    fn decode_error(&mut self, _error: &DecodeError) {}
}

struct DisabledLogger;

struct EnabledLogger<W> {
    client: ClientIdentity,
    stats: ConnectionStats,
    started: Instant,
    writer: W,
}

impl<W: Write> EnabledLogger<W> {
    fn new(client: ClientIdentity, writer: W) -> Self {
        Self {
            client,
            stats: ConnectionStats::default(),
            started: Instant::now(),
            writer,
        }
    }

    fn connected(&mut self) {
        log_line(&mut self.writer, &connected_line(&self.client));
    }

    fn finished(&mut self, result: &io::Result<()>) {
        let duration = self.started.elapsed();
        let line = match result {
            Ok(()) => disconnected_line(&self.client, &self.stats, duration),
            Err(error) => io_error_line(&self.client, &self.stats, duration, &error.to_string()),
        };
        log_line(&mut self.writer, &line);
    }
}

impl EventLogger for DisabledLogger {}

impl<W: Write> EventLogger for EnabledLogger<W> {
    fn received(&mut self, bytes: usize) {
        self.stats.received += bytes as u64;
    }

    fn sent(&mut self, bytes: usize) {
        self.stats.sent += bytes as u64;
    }

    fn request(&mut self, parts: &[Vec<u8>]) {
        self.stats.requests += 1;
        log_line(&mut self.writer, &request_line(&self.client, parts));
    }

    fn response(&mut self, response: &RespValue) {
        log_line(&mut self.writer, &response_line(&self.client, response));
    }

    fn protocol_error(&mut self, reason: &str) {
        log_line(&mut self.writer, &protocol_error_line(&self.client, reason));
    }

    fn incomplete_buffer(&mut self, buffer_limit: usize) {
        self.protocol_error(&format!(
            "incomplete request exceeds {buffer_limit}-byte buffer limit"
        ));
    }

    fn decode_error(&mut self, error: &DecodeError) {
        self.protocol_error(&error.to_string());
    }
}

fn handle_io<T: Read + Write, L: EventLogger>(
    stream: &mut T,
    buffer_limit: usize,
    database: &Database,
    logger: &mut L,
) -> io::Result<()> {
    let decoder = Decoder::default();
    let encoder = Encoder::default();
    let mut buffer = Vec::new();
    let mut chunk = [0; 8192];

    loop {
        let bytes_read = stream.read(&mut chunk)?;
        if bytes_read == 0 {
            return Ok(());
        }
        logger.received(bytes_read);
        buffer.extend_from_slice(&chunk[..bytes_read]);

        loop {
            match decoder.decode(&buffer) {
                Ok(decoded) => {
                    let Some(parts) = command_parts(decoded.value) else {
                        logger.protocol_error("invalid command framing");
                        reject_protocol(stream, logger)?;
                        return Ok(());
                    };
                    logger.request(&parts);
                    let response = command::dispatch(parts, database);
                    let encoded = encoder
                        .to_bytes(&response)
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    write_all_counted(stream, &encoded, logger)?;
                    logger.response(&response);
                    buffer.drain(..decoded.consumed);
                    if buffer.is_empty() {
                        break;
                    }
                }
                Err(error) if error.kind == DecodeErrorKind::IncompleteInput => {
                    if buffer.len() > buffer_limit {
                        logger.incomplete_buffer(buffer_limit);
                        reject_protocol(stream, logger)?;
                        return Ok(());
                    }
                    break;
                }
                Err(error) => {
                    logger.decode_error(&error);
                    reject_protocol(stream, logger)?;
                    return Ok(());
                }
            }
        }
    }
}

fn reject_protocol<T: Write, L: EventLogger>(stream: &mut T, logger: &mut L) -> io::Result<()> {
    write_all_counted(stream, PROTOCOL_ERROR, logger)
}

fn write_all_counted<T: Write, L: EventLogger>(
    writer: &mut T,
    mut bytes: &[u8],
    logger: &mut L,
) -> io::Result<()> {
    while !bytes.is_empty() {
        match writer.write(bytes) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                logger.sent(written);
                bytes = &bytes[written..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn log_line<W: Write>(writer: &mut W, line: &str) {
    let _ = writeln!(writer, "{line}");
}

fn socket_description(address: io::Result<std::net::SocketAddr>) -> String {
    address.map_or_else(
        |error| format!("unavailable ({error})"),
        |address| address.to_string(),
    )
}

fn command_parts(value: RespValue) -> Option<Vec<Vec<u8>>> {
    let RespValue::Array(values) = value else {
        return None;
    };
    if values.is_empty() {
        return None;
    }
    values
        .into_iter()
        .map(|value| match value {
            RespValue::BulkString(data) => Some(data),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{DisabledLogger, EnabledLogger, handle_connection, handle_io};
    use crate::database::Database;
    use crate::logging::{ClientIdentity, ConnectionStats, LogMode};
    use std::io::{self, Cursor, Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::Arc;
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    const TEST_LIMIT: usize = 1024;

    fn test_client() -> ClientIdentity {
        ClientIdentity {
            id: 7,
            peer: "127.0.0.1:50000".to_owned(),
            local: "127.0.0.1:6379".to_owned(),
        }
    }

    fn connection(limit: usize) -> (TcpStream, JoinHandle<io::Result<()>>) {
        connection_with_database(limit, Arc::new(Database::default()))
    }

    fn connection_with_database(
        limit: usize,
        database: Arc<Database>,
    ) -> (TcpStream, JoinHandle<io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).unwrap();
        let (server, _) = listener.accept().unwrap();
        let handle =
            thread::spawn(move || handle_connection(server, limit, database, LogMode::Enabled));
        (client, handle)
    }

    fn read_exact(stream: &mut TcpStream, expected: &[u8]) {
        let mut actual = vec![0; expected.len()];
        stream.read_exact(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }

    fn finish(mut client: TcpStream, handle: JoinHandle<io::Result<()>>) {
        client.shutdown(Shutdown::Write).unwrap();
        let mut trailing = Vec::new();
        client.read_to_end(&mut trailing).unwrap();
        assert!(trailing.is_empty());
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn handles_fragmented_requests() {
        let (mut client, handle) = connection(TEST_LIMIT);
        client.write_all(b"*1\r\n$4\r\nPI").unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let mut byte = [0];
        let error = client.read(&mut byte).unwrap_err();
        assert!(matches!(
            error.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        ));
        client.set_read_timeout(None).unwrap();
        client.write_all(b"NG\r\n").unwrap();
        read_exact(&mut client, b"+PONG\r\n");
        finish(client, handle);
    }

    #[test]
    fn handles_pipelined_requests_in_order() {
        let (mut client, handle) = connection(TEST_LIMIT);
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$3\r\none\r\n")
            .unwrap();
        read_exact(&mut client, b"+PONG\r\n$3\r\none\r\n");
        finish(client, handle);
    }

    #[test]
    fn handles_pipelined_set_followed_by_get() {
        let (mut client, handle) = connection(TEST_LIMIT);
        client
            .write_all(
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            )
            .unwrap();
        read_exact(&mut client, b"+OK\r\n$5\r\nvalue\r\n");
        finish(client, handle);
    }

    #[test]
    fn values_survive_the_connection_that_created_them() {
        let database = Arc::new(Database::default());
        let (mut writer, writer_handle) = connection_with_database(TEST_LIMIT, database.clone());
        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .unwrap();
        read_exact(&mut writer, b"+OK\r\n");
        finish(writer, writer_handle);

        let (mut reader, reader_handle) = connection_with_database(TEST_LIMIT, database);
        reader
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .unwrap();
        read_exact(&mut reader, b"$5\r\nvalue\r\n");
        finish(reader, reader_handle);
    }

    #[test]
    fn concurrent_client_observes_value_written_by_another() {
        let database = Arc::new(Database::default());
        let (mut writer, writer_handle) = connection_with_database(TEST_LIMIT, database.clone());
        let (mut reader, reader_handle) = connection_with_database(TEST_LIMIT, database);

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .unwrap();
        read_exact(&mut writer, b"+OK\r\n");
        reader
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .unwrap();
        read_exact(&mut reader, b"$5\r\nvalue\r\n");

        finish(writer, writer_handle);
        finish(reader, reader_handle);
    }

    #[test]
    fn handles_sequential_requests_on_a_persistent_connection() {
        let (mut client, handle) = connection(TEST_LIMIT);
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        read_exact(&mut client, b"+PONG\r\n");
        client.write_all(b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n").unwrap();
        read_exact(&mut client, b"$0\r\n\r\n");
        finish(client, handle);
    }

    #[test]
    fn concurrent_clients_are_independent() {
        let (mut bad_client, bad_handle) = connection(TEST_LIMIT);
        let (mut good_client, good_handle) = connection(TEST_LIMIT);

        bad_client.write_all(b"not RESP").unwrap();
        bad_client.shutdown(Shutdown::Write).unwrap();
        good_client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();

        read_exact(&mut bad_client, b"-ERR Protocol error\r\n");
        let mut end = Vec::new();
        bad_client.read_to_end(&mut end).unwrap();
        assert!(end.is_empty());
        bad_handle.join().unwrap().unwrap();

        read_exact(&mut good_client, b"+PONG\r\n");
        finish(good_client, good_handle);
    }

    #[test]
    fn malformed_resp_and_invalid_command_shapes_are_protocol_errors() {
        for request in [
            b"?\r\n".as_slice(),
            b"*0\r\n",
            b"+PING\r\n",
            b"*1\r\n+PING\r\n",
            b"*1\r\n$-1\r\n",
            b"*-1\r\n",
        ] {
            let (mut client, handle) = connection(TEST_LIMIT);
            client.write_all(request).unwrap();
            client.shutdown(Shutdown::Write).unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).unwrap();
            assert_eq!(response, b"-ERR Protocol error\r\n", "request: {request:?}");
            handle.join().unwrap().unwrap();
        }
    }

    #[test]
    fn rejects_incomplete_input_over_the_injected_buffer_limit() {
        let (mut client, handle) = connection(8);
        client.write_all(b"*1\r\n$20\r\nabc").unwrap();
        client.shutdown(Shutdown::Write).unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).unwrap();
        assert_eq!(response, b"-ERR Protocol error\r\n");
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn eof_without_a_request_closes_silently() {
        let (client, handle) = connection(TEST_LIMIT);
        finish(client, handle);
    }

    struct WriteFailure {
        reader: Cursor<Vec<u8>>,
    }

    struct TestIo {
        reader: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl Read for TestIo {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.reader.read(buffer)
        }
    }

    impl Write for TestIo {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.written.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn logs_request_response_and_exact_io_counters() {
        let request = b"*1\r\n$4\r\nPING\r\n";
        let mut io = TestIo {
            reader: Cursor::new(request.to_vec()),
            written: Vec::new(),
        };
        let mut logger = EnabledLogger::new(test_client(), Vec::new());

        handle_io(&mut io, TEST_LIMIT, &Database::default(), &mut logger).unwrap();

        assert_eq!(io.written, b"+PONG\r\n");
        assert_eq!(
            logger.stats,
            ConnectionStats {
                requests: 1,
                received: request.len() as u64,
                sent: 7,
            }
        );
        let logs = String::from_utf8(logger.writer).unwrap();
        assert!(logs.contains("[redis] client-0007 request PING\n"));
        assert!(logs.contains("[redis] client-0007 response PONG\n"));
    }

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

    #[test]
    fn logs_null_bulk_string_response_explicitly() {
        let request = b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n";
        let mut io = TestIo {
            reader: Cursor::new(request.to_vec()),
            written: Vec::new(),
        };
        let mut logger = EnabledLogger::new(test_client(), Vec::new());

        handle_io(&mut io, TEST_LIMIT, &Database::default(), &mut logger).unwrap();

        assert_eq!(io.written, b"$-1\r\n");
        let logs = String::from_utf8(logger.writer).unwrap();
        assert!(logs.contains("[redis] client-0007 response null bulk-string\n"));
    }

    #[test]
    fn logs_protocol_reason_and_counts_the_error_response() {
        let mut io = TestIo {
            reader: Cursor::new(b"?\r\n".to_vec()),
            written: Vec::new(),
        };
        let mut logger = EnabledLogger::new(test_client(), Vec::new());

        handle_io(&mut io, TEST_LIMIT, &Database::default(), &mut logger).unwrap();

        assert_eq!(io.written, b"-ERR Protocol error\r\n");
        assert_eq!(logger.stats.requests, 0);
        assert_eq!(logger.stats.received, 3);
        assert_eq!(logger.stats.sent, io.written.len() as u64);
        let logs = String::from_utf8(logger.writer).unwrap();
        assert!(logs.contains("[redis] client-0007 protocol-error"));
        assert!(logs.contains("unknown prefix 0x3f"));
    }

    impl Read for WriteFailure {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.reader.read(buffer)
        }
    }

    impl Write for WriteFailure {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "injected"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn response_write_failure_terminates_the_affected_connection() {
        let mut io = WriteFailure {
            reader: Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec()),
        };
        let error = handle_io(
            &mut io,
            TEST_LIMIT,
            &Database::default(),
            &mut DisabledLogger,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

        let (mut client, handle) = connection(TEST_LIMIT);
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        read_exact(&mut client, b"+PONG\r\n");
        finish(client, handle);
    }
}
