use crate::logging::{
    ClientIdentity, ConnectionStats, connected_line, disconnected_line, io_error_line,
    protocol_error_line, request_line, response_line,
};
use redis::resp::{DecodeError, RespValue};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

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

impl<W: Write> EnabledLogger<W> {
    pub(super) fn new(client: ClientIdentity, writer: W) -> Self {
        Self {
            client,
            stats: ConnectionStats::default(),
            started: Instant::now(),
            writer,
        }
    }
}

impl EventLogger for DisabledLogger {}

impl<W: Write> EventLogger for EnabledLogger<W> {
    fn connected(&mut self) {
        log_line(&mut self.writer, &connected_line(&self.client));
    }

    fn received(&mut self, bytes: usize) {
        self.stats.received += bytes as u64;
    }

    fn sent(&mut self, bytes: usize) {
        self.stats.sent += bytes as u64;
    }

    fn request(&mut self, parts: &[&[u8]]) {
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

    fn finished(&mut self, result: &io::Result<()>) {
        let duration = self.started.elapsed();
        let line = match result {
            Ok(()) => disconnected_line(&self.client, &self.stats, duration),
            Err(error) => io_error_line(&self.client, &self.stats, duration, &error.to_string()),
        };
        log_line(&mut self.writer, &line);
    }
}

pub(super) fn client_identity(
    peer: io::Result<SocketAddr>,
    local: io::Result<SocketAddr>,
) -> ClientIdentity {
    ClientIdentity {
        id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
        peer: socket_description(peer),
        local: socket_description(local),
    }
}

fn log_line<W: Write>(writer: &mut W, line: &str) {
    let _ = writeln!(writer, "{line}");
}

fn socket_description(address: io::Result<SocketAddr>) -> String {
    address.map_or_else(
        |error| format!("unavailable ({error})"),
        |address| address.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::{DisabledLogger, EnabledLogger, EventLogger};
    use crate::logging::{ClientIdentity, ConnectionStats};
    use redis::resp::RespValue;

    fn test_client() -> ClientIdentity {
        ClientIdentity {
            id: 7,
            peer: "127.0.0.1:50000".to_owned(),
            local: "127.0.0.1:6379".to_owned(),
        }
    }

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
}
