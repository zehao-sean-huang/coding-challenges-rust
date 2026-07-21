use super::{ServerMode, event_loop, run, threaded};
use crate::logging::LogMode;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
const SERVER_TIMEOUT: Duration = Duration::from_secs(5);

fn for_each_server_mode(test: impl Fn(ServerMode)) {
    for mode in [ServerMode::Threaded, ServerMode::EventLoop] {
        test(mode);
    }
}

struct TestServer {
    address: SocketAddr,
    handle: JoinHandle<io::Result<()>>,
}

fn start_server(
    mode: ServerMode,
    log_mode: LogMode,
    expected_connections: usize,
    buffer_limit: usize,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || match mode {
        ServerMode::Threaded => {
            threaded::run_test_listener(listener, log_mode, expected_connections, buffer_limit)
        }
        ServerMode::EventLoop => event_loop::run_test_listener(
            listener,
            log_mode,
            expected_connections,
            buffer_limit,
            Instant::now() + SERVER_TIMEOUT,
        ),
    });
    TestServer { address, handle }
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, CLIENT_TIMEOUT).unwrap();
    stream.set_read_timeout(Some(CLIENT_TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(CLIENT_TIMEOUT)).unwrap();
    stream
}

fn read_exact_response(stream: &mut TcpStream, expected: &[u8]) {
    let mut actual = vec![0; expected.len()];
    stream.read_exact(&mut actual).unwrap();
    assert_eq!(actual, expected);
}

fn finish(mut clients: Vec<TcpStream>, server: TestServer) {
    for client in &clients {
        client.shutdown(Shutdown::Write).unwrap();
    }
    for client in &mut clients {
        let mut trailing = Vec::new();
        client.read_to_end(&mut trailing).unwrap();
        assert!(trailing.is_empty());
    }
    server.handle.join().unwrap().unwrap();
}

fn read_to_end_after_eof(mut client: TcpStream) -> Vec<u8> {
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    response
}

#[test]
fn facade_accepts_the_selected_server_mode() {
    let _run: fn(&str, LogMode, ServerMode) -> io::Result<()> = run;
}

#[test]
fn disabled_logging_preserves_ping_bytes() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        read_exact_response(&mut client, b"+PONG\r\n");
        finish(vec![client], server);
    });
}

#[test]
fn complete_request_before_write_shutdown_flushes_response_before_close() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();

        let response = read_to_end_after_eof(client);

        assert_eq!(response, b"+PONG\r\n", "mode: {mode:?}");
        server.handle.join().unwrap().unwrap();
    });
}

#[test]
fn fragmented_ping_has_no_early_response_and_returns_exact_pong() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client.write_all(b"*1\r\n$4\r\nPI").unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let mut byte = [0];
        let error = client.read(&mut byte).unwrap_err();
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ),
            "mode: {mode:?}, error: {error}"
        );
        client.set_read_timeout(Some(CLIENT_TIMEOUT)).unwrap();
        client.write_all(b"NG\r\n").unwrap();
        read_exact_response(&mut client, b"+PONG\r\n");
        finish(vec![client], server);
    });
}

#[test]
fn pipelined_ping_and_binary_echo_return_exact_bytes_in_order() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$4\r\n\0\xff\r\n\r\n")
            .unwrap();
        read_exact_response(&mut client, b"+PONG\r\n$4\r\n\0\xff\r\n\r\n");
        finish(vec![client], server);
    });
}

#[test]
fn pipelined_binary_set_then_get_returns_exact_bytes() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client
            .write_all(
                b"*3\r\n$3\r\nSET\r\n$2\r\n\0\xff\r\n$4\r\n\xff\0\r\n\r\n*2\r\n$3\r\nGET\r\n$2\r\n\0\xff\r\n",
            )
            .unwrap();
        read_exact_response(&mut client, b"+OK\r\n$4\r\n\xff\0\r\n\r\n");
        finish(vec![client], server);
    });
}

#[test]
fn get_missing_key_returns_exact_null_bulk_string() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n")
            .unwrap();
        read_exact_response(&mut client, b"$-1\r\n");
        finish(vec![client], server);
    });
}

#[test]
fn set_overwrite_then_get_returns_exact_final_value() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client
            .write_all(
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nfirst\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nsecond\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            )
            .unwrap();
        read_exact_response(&mut client, b"+OK\r\n+OK\r\n$6\r\nsecond\r\n");
        finish(vec![client], server);
    });
}

#[test]
fn value_written_by_one_connection_is_visible_to_another() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            2,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut writer = connect(server.address);
        let mut reader = connect(server.address);
        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .unwrap();
        read_exact_response(&mut writer, b"+OK\r\n");
        reader
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .unwrap();
        read_exact_response(&mut reader, b"$5\r\nvalue\r\n");
        finish(vec![writer, reader], server);
    });
}

#[test]
fn unknown_command_and_wrong_arity_return_exact_errors() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client
            .write_all(b"*1\r\n$4\r\nNOPE\r\n*1\r\n$4\r\nECHO\r\n")
            .unwrap();
        read_exact_response(
            &mut client,
            b"-ERR unknown command\r\n-ERR wrong number of arguments for 'echo' command\r\n",
        );
        finish(vec![client], server);
    });
}

#[test]
fn every_rejected_non_command_resp_shape_returns_a_protocol_error() {
    for request in [
        b"?\r\n".as_slice(),
        b"*0\r\n",
        b"+PING\r\n",
        b"*1\r\n+PING\r\n",
        b"*1\r\n$-1\r\n",
        b"*-1\r\n",
    ] {
        for_each_server_mode(|mode| {
            let server = start_server(
                mode,
                LogMode::Disabled,
                1,
                event_loop::MAX_INCOMPLETE_BUFFER,
            );
            let mut client = connect(server.address);
            client.write_all(request).unwrap();
            let response = read_to_end_after_eof(client);
            assert_eq!(response, b"-ERR Protocol error\r\n", "mode: {mode:?}");
            server.handle.join().unwrap().unwrap();
        });
    }
}

#[test]
fn incomplete_input_above_injected_limit_returns_a_protocol_error() {
    for_each_server_mode(|mode| {
        let server = start_server(mode, LogMode::Disabled, 1, 8);
        let mut client = connect(server.address);
        client.write_all(b"*1\r\n$20\r\nabc").unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).unwrap();
        assert_eq!(response, b"-ERR Protocol error\r\n", "mode: {mode:?}");
        server.handle.join().unwrap().unwrap();
    });
}

#[test]
fn eof_without_a_request_closes_silently() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let client = connect(server.address);
        assert!(read_to_end_after_eof(client).is_empty(), "mode: {mode:?}");
        server.handle.join().unwrap().unwrap();
    });
}

#[test]
fn eof_with_an_incomplete_request_closes_silently() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            1,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut client = connect(server.address);
        client.write_all(b"*1\r\n$4\r\nPI").unwrap();
        assert!(read_to_end_after_eof(client).is_empty(), "mode: {mode:?}");
        server.handle.join().unwrap().unwrap();
    });
}

#[test]
fn bad_client_protocol_rejection_does_not_break_a_good_client() {
    for_each_server_mode(|mode| {
        let server = start_server(
            mode,
            LogMode::Disabled,
            2,
            event_loop::MAX_INCOMPLETE_BUFFER,
        );
        let mut bad_client = connect(server.address);
        let mut good_client = connect(server.address);
        bad_client.write_all(b"not RESP").unwrap();
        bad_client.shutdown(Shutdown::Write).unwrap();
        good_client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();

        let mut bad_response = Vec::new();
        bad_client.read_to_end(&mut bad_response).unwrap();
        assert_eq!(bad_response, b"-ERR Protocol error\r\n");
        read_exact_response(&mut good_client, b"+PONG\r\n");
        finish(vec![good_client], server);
    });
}

#[test]
fn event_loop_test_runner_times_out_at_its_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let error = event_loop::run_test_listener(
        listener,
        LogMode::Disabled,
        1,
        event_loop::MAX_INCOMPLETE_BUFFER,
        Instant::now(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
}
