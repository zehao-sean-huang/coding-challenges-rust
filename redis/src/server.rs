use crate::command;
use crate::logging::{
    ClientIdentity, ConnectionStats, connected_line, disconnected_line, io_error_line,
    protocol_error_line, request_line, response_line,
};
use redis::resp::{DecodeErrorKind, Decoder, Encoder, RespValue};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

const PROTOCOL_ERROR: &[u8] = b"-ERR Protocol error\r\n";
const MAX_INCOMPLETE_BUFFER: usize = 536_870_912 + 64;
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn run(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address)?;
    let bound_address = listener.local_addr()?;
    eprintln!("[redis] listening address={bound_address}");
    for stream in listener.incoming() {
        let stream = stream?;
        drop(thread::spawn(move || {
            let _ = handle_connection(stream, MAX_INCOMPLETE_BUFFER);
        }));
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream, buffer_limit: usize) -> io::Result<()> {
    let client = ClientIdentity {
        id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
        peer: socket_description(stream.peer_addr()),
        local: socket_description(stream.local_addr()),
    };
    let started = Instant::now();
    let mut stats = ConnectionStats::default();
    let mut log = io::stderr();
    log_line(&mut log, &connected_line(&client));

    let result = handle_io_logged(&mut stream, buffer_limit, &client, &mut stats, &mut log);
    let duration = started.elapsed();
    match &result {
        Ok(()) => log_line(&mut log, &disconnected_line(&client, &stats, duration)),
        Err(error) => log_line(
            &mut log,
            &io_error_line(&client, &stats, duration, &error.to_string()),
        ),
    }
    result
}

#[cfg(test)]
fn handle_io<T: Read + Write>(mut stream: T, buffer_limit: usize) -> io::Result<()> {
    let client = ClientIdentity {
        id: 0,
        peer: "test-peer".to_owned(),
        local: "test-local".to_owned(),
    };
    let mut stats = ConnectionStats::default();
    handle_io_logged(
        &mut stream,
        buffer_limit,
        &client,
        &mut stats,
        &mut io::sink(),
    )
}

fn handle_io_logged<T: Read + Write, W: Write>(
    stream: &mut T,
    buffer_limit: usize,
    client: &ClientIdentity,
    stats: &mut ConnectionStats,
    log: &mut W,
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
        stats.received += bytes_read as u64;
        buffer.extend_from_slice(&chunk[..bytes_read]);

        loop {
            match decoder.decode(&buffer) {
                Ok(decoded) => {
                    let Some(parts) = command_parts(decoded.value) else {
                        reject_protocol(stream, stats, log, client, "invalid command framing")?;
                        return Ok(());
                    };
                    log_line(log, &request_line(client, &parts));
                    stats.requests += 1;
                    let response = command::dispatch(parts);
                    let encoded = encoder
                        .to_bytes(&response)
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    write_all_counted(stream, &encoded, stats)?;
                    log_line(log, &response_line(client, &response));
                    buffer.drain(..decoded.consumed);
                    if buffer.is_empty() {
                        break;
                    }
                }
                Err(error) if error.kind == DecodeErrorKind::IncompleteInput => {
                    if buffer.len() > buffer_limit {
                        reject_protocol(
                            stream,
                            stats,
                            log,
                            client,
                            &format!("incomplete request exceeds {buffer_limit}-byte buffer limit"),
                        )?;
                        return Ok(());
                    }
                    break;
                }
                Err(error) => {
                    reject_protocol(stream, stats, log, client, &error.to_string())?;
                    return Ok(());
                }
            }
        }
    }
}

fn reject_protocol<T: Write, W: Write>(
    stream: &mut T,
    stats: &mut ConnectionStats,
    log: &mut W,
    client: &ClientIdentity,
    reason: &str,
) -> io::Result<()> {
    log_line(log, &protocol_error_line(client, reason));
    write_all_counted(stream, PROTOCOL_ERROR, stats)
}

fn write_all_counted<T: Write>(
    writer: &mut T,
    mut bytes: &[u8],
    stats: &mut ConnectionStats,
) -> io::Result<()> {
    while !bytes.is_empty() {
        match writer.write(bytes) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                stats.sent += written as u64;
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
    use super::{handle_connection, handle_io, handle_io_logged};
    use crate::logging::{ClientIdentity, ConnectionStats};
    use std::io::{self, Cursor, Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).unwrap();
        let (server, _) = listener.accept().unwrap();
        let handle = thread::spawn(move || handle_connection(server, limit));
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
        let mut stats = ConnectionStats::default();
        let mut logs = Vec::new();

        handle_io_logged(&mut io, TEST_LIMIT, &test_client(), &mut stats, &mut logs).unwrap();

        assert_eq!(io.written, b"+PONG\r\n");
        assert_eq!(
            stats,
            ConnectionStats {
                requests: 1,
                received: request.len() as u64,
                sent: 7,
            }
        );
        let logs = String::from_utf8(logs).unwrap();
        assert!(logs.contains("[redis] client-0007 request PING\n"));
        assert!(logs.contains("[redis] client-0007 response PONG\n"));
    }

    #[test]
    fn logs_protocol_reason_and_counts_the_error_response() {
        let mut io = TestIo {
            reader: Cursor::new(b"?\r\n".to_vec()),
            written: Vec::new(),
        };
        let mut stats = ConnectionStats::default();
        let mut logs = Vec::new();

        handle_io_logged(&mut io, TEST_LIMIT, &test_client(), &mut stats, &mut logs).unwrap();

        assert_eq!(io.written, b"-ERR Protocol error\r\n");
        assert_eq!(stats.requests, 0);
        assert_eq!(stats.received, 3);
        assert_eq!(stats.sent, io.written.len() as u64);
        let logs = String::from_utf8(logs).unwrap();
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
        let io = WriteFailure {
            reader: Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec()),
        };
        let error = handle_io(io, TEST_LIMIT).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

        let (mut client, handle) = connection(TEST_LIMIT);
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        read_exact(&mut client, b"+PONG\r\n");
        finish(client, handle);
    }
}
