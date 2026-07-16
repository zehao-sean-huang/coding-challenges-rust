use redis::resp::RespValue;

pub(crate) fn dispatch(parts: Vec<Vec<u8>>) -> RespValue {
    let Some(name) = parts.first() else {
        return RespValue::SimpleError(b"ERR unknown command".to_vec());
    };

    if name.eq_ignore_ascii_case(b"PING") {
        return match parts.len() {
            1 => RespValue::SimpleString(b"PONG".to_vec()),
            2 => RespValue::BulkString(parts.into_iter().nth(1).expect("checked command arity")),
            _ => wrong_arity("ping"),
        };
    }

    if name.eq_ignore_ascii_case(b"ECHO") {
        return match parts.len() {
            2 => RespValue::BulkString(parts.into_iter().nth(1).expect("checked command arity")),
            _ => wrong_arity("echo"),
        };
    }

    RespValue::SimpleError(b"ERR unknown command".to_vec())
}

fn wrong_arity(command: &str) -> RespValue {
    RespValue::SimpleError(
        format!("ERR wrong number of arguments for '{command}' command").into_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use redis::resp::RespValue;

    fn command(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|part| part.to_vec()).collect()
    }

    #[test]
    fn ping_without_message_returns_pong_for_any_ascii_case() {
        for name in [b"PING".as_slice(), b"ping", b"PiNg"] {
            assert_eq!(
                dispatch(command(&[name])),
                RespValue::SimpleString(b"PONG".to_vec())
            );
        }
    }

    #[test]
    fn ping_with_message_returns_binary_safe_bulk_string() {
        for message in [b"hello".as_slice(), b"", b"\0\xff\r\n"] {
            assert_eq!(
                dispatch(command(&[b"PING", message])),
                RespValue::BulkString(message.to_vec())
            );
        }
    }

    #[test]
    fn echo_returns_binary_safe_bulk_string_for_any_ascii_case() {
        for name in [b"ECHO".as_slice(), b"echo", b"EcHo"] {
            for message in [b"hello".as_slice(), b"", b"\0\xff\r\n"] {
                assert_eq!(
                    dispatch(command(&[name, message])),
                    RespValue::BulkString(message.to_vec())
                );
            }
        }
    }

    #[test]
    fn wrong_arities_return_exact_errors() {
        for (parts, expected) in [
            (
                command(&[b"PING", b"one", b"two"]),
                b"ERR wrong number of arguments for 'ping' command".as_slice(),
            ),
            (
                command(&[b"ECHO"]),
                b"ERR wrong number of arguments for 'echo' command".as_slice(),
            ),
            (
                command(&[b"echo", b"one", b"two"]),
                b"ERR wrong number of arguments for 'echo' command".as_slice(),
            ),
        ] {
            assert_eq!(dispatch(parts), RespValue::SimpleError(expected.to_vec()));
        }
    }

    #[test]
    fn unknown_commands_return_exact_error() {
        assert_eq!(
            dispatch(command(&[b"GET", b"key"])),
            RespValue::SimpleError(b"ERR unknown command".to_vec())
        );
    }
}
