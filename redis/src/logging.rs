use redis::resp::RespValue;
use std::time::Duration;

const MAX_RENDERED_CONTENT: usize = 120;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClientIdentity {
    pub(crate) id: u64,
    pub(crate) peer: String,
    pub(crate) local: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ConnectionStats {
    pub(crate) requests: u64,
    pub(crate) received: u64,
    pub(crate) sent: u64,
}

pub(crate) fn render_bytes(bytes: &[u8]) -> String {
    let mut content = String::new();
    for &byte in bytes {
        let escaped = match byte {
            b'\n' => "\\n".to_owned(),
            b'\r' => "\\r".to_owned(),
            b'\t' => "\\t".to_owned(),
            b'\\' => "\\\\".to_owned(),
            b'"' => "\\\"".to_owned(),
            b' '..=b'~' => char::from(byte).to_string(),
            _ => format!("\\x{byte:02x}"),
        };
        if content.len() + escaped.len() > MAX_RENDERED_CONTENT {
            content.truncate(MAX_RENDERED_CONTENT - 3);
            content.push_str("...");
            break;
        }
        content.push_str(&escaped);
    }
    format!("\"{content}\"")
}

pub(crate) fn connected_line(client: &ClientIdentity) -> String {
    format!(
        "[redis] {} connected peer={} local={}",
        client_label(client),
        client.peer,
        client.local
    )
}

pub(crate) fn request_line(client: &ClientIdentity, parts: &[Vec<u8>]) -> String {
    let command = parts
        .first()
        .map_or_else(|| "<empty>".to_owned(), |name| render_command(name));
    let arguments = parts
        .iter()
        .skip(1)
        .map(|argument| render_bytes(argument))
        .collect::<Vec<_>>()
        .join(" ");
    if arguments.is_empty() {
        format!("[redis] {} request {command}", client_label(client))
    } else {
        format!(
            "[redis] {} request {command} {arguments}",
            client_label(client)
        )
    }
}

pub(crate) fn response_line(client: &ClientIdentity, response: &RespValue) -> String {
    let summary = match response {
        RespValue::SimpleString(value) => render_unquoted(value),
        RespValue::SimpleError(value) => format!("error {}", render_bytes(value)),
        RespValue::BulkString(value) => {
            format!("bulk-string {} bytes {}", value.len(), render_bytes(value))
        }
        _ => "RESP value".to_owned(),
    };
    format!("[redis] {} response {summary}", client_label(client))
}

pub(crate) fn protocol_error_line(client: &ClientIdentity, reason: &str) -> String {
    format!("[redis] {} protocol-error {reason}", client_label(client))
}

pub(crate) fn disconnected_line(
    client: &ClientIdentity,
    stats: &ConnectionStats,
    duration: Duration,
) -> String {
    format!(
        "[redis] {} disconnected requests={} received={}B sent={}B duration={:.2}s",
        client_label(client),
        stats.requests,
        stats.received,
        stats.sent,
        duration.as_secs_f64()
    )
}

pub(crate) fn io_error_line(
    client: &ClientIdentity,
    stats: &ConnectionStats,
    duration: Duration,
    error: &str,
) -> String {
    format!(
        "[redis] {} io-error {error} requests={} received={}B sent={}B duration={:.2}s",
        client_label(client),
        stats.requests,
        stats.received,
        stats.sent,
        duration.as_secs_f64()
    )
}

fn client_label(client: &ClientIdentity) -> String {
    format!("client-{:04}", client.id)
}

fn render_command(bytes: &[u8]) -> String {
    if bytes
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        String::from_utf8(bytes.to_vec()).expect("ASCII command is valid UTF-8")
    } else {
        render_bytes(bytes)
    }
}

fn render_unquoted(bytes: &[u8]) -> String {
    let rendered = render_bytes(bytes);
    rendered[1..rendered.len() - 1].to_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        ClientIdentity, ConnectionStats, connected_line, disconnected_line, io_error_line,
        protocol_error_line, render_bytes, request_line, response_line,
    };
    use redis::resp::RespValue;
    use std::time::Duration;

    fn client() -> ClientIdentity {
        ClientIdentity {
            id: 1,
            peer: "127.0.0.1:52341".to_owned(),
            local: "127.0.0.1:6379".to_owned(),
        }
    }

    #[test]
    fn renders_binary_data_readably() {
        assert_eq!(render_bytes(b"hello\n\0\xff"), r#""hello\n\x00\xff""#);
        assert_eq!(render_bytes(b"say \"hi\" \\"), r#""say \"hi\" \\""#);
    }

    #[test]
    fn truncates_rendered_values_at_120_content_characters() {
        let rendered = render_bytes(&[b'a'; 121]);
        assert_eq!(rendered, format!("\"{}...\"", "a".repeat(117)));
        assert_eq!(rendered.chars().count(), 122);
    }

    #[test]
    fn formats_requests_and_responses_for_people() {
        let client = client();
        assert_eq!(
            request_line(&client, &[b"PING".to_vec()]),
            "[redis] client-0001 request PING"
        );
        assert_eq!(
            request_line(&client, &[b"ECHO".to_vec(), b"hello\nworld".to_vec()]),
            r#"[redis] client-0001 request ECHO "hello\nworld""#
        );
        assert_eq!(
            response_line(&client, &RespValue::SimpleString(b"PONG".to_vec())),
            "[redis] client-0001 response PONG"
        );
        assert_eq!(
            response_line(&client, &RespValue::BulkString(b"hello".to_vec())),
            r#"[redis] client-0001 response bulk-string 5 bytes "hello""#
        );
        assert_eq!(
            response_line(
                &client,
                &RespValue::SimpleError(b"ERR unknown command".to_vec())
            ),
            r#"[redis] client-0001 response error "ERR unknown command""#
        );
    }

    #[test]
    fn formats_connection_metadata_protocol_errors_and_statistics() {
        let client = client();
        assert_eq!(
            connected_line(&client),
            "[redis] client-0001 connected peer=127.0.0.1:52341 local=127.0.0.1:6379"
        );
        assert_eq!(
            protocol_error_line(&client, "invalid command framing"),
            "[redis] client-0001 protocol-error invalid command framing"
        );
        let stats = ConnectionStats {
            requests: 2,
            received: 61,
            sent: 32,
        };
        assert_eq!(
            disconnected_line(&client, &stats, Duration::from_millis(1240)),
            "[redis] client-0001 disconnected requests=2 received=61B sent=32B duration=1.24s"
        );
        assert_eq!(
            io_error_line(&client, &stats, Duration::from_millis(1240), "broken pipe"),
            "[redis] client-0001 io-error broken pipe requests=2 received=61B sent=32B duration=1.24s"
        );
    }
}
