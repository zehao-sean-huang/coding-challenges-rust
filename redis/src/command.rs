use crate::database::Database;
use redis::resp::RespValue;

pub(crate) fn dispatch(parts: Vec<Vec<u8>>, database: &Database) -> RespValue {
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

    if name.eq_ignore_ascii_case(b"SET") {
        if parts.len() != 3 {
            return wrong_arity("set");
        }
        let mut arguments = parts.into_iter().skip(1);
        let key = arguments.next().expect("checked command arity");
        let value = arguments.next().expect("checked command arity");
        database.set(key, value);
        return RespValue::SimpleString(b"OK".to_vec());
    }

    if name.eq_ignore_ascii_case(b"GET") {
        return match parts.len() {
            2 => database
                .get(&parts[1])
                .map_or(RespValue::NullBulkString, RespValue::BulkString),
            _ => wrong_arity("get"),
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
    use crate::database::Database;
    use redis::resp::RespValue;

    fn command(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|part| part.to_vec()).collect()
    }

    fn dispatch_once(parts: Vec<Vec<u8>>) -> RespValue {
        dispatch(parts, &Database::default())
    }

    #[test]
    fn ping_without_message_returns_pong_for_any_ascii_case() {
        for name in [b"PING".as_slice(), b"ping", b"PiNg"] {
            assert_eq!(
                dispatch_once(command(&[name])),
                RespValue::SimpleString(b"PONG".to_vec())
            );
        }
    }

    #[test]
    fn ping_with_message_returns_binary_safe_bulk_string() {
        for message in [b"hello".as_slice(), b"", b"\0\xff\r\n"] {
            assert_eq!(
                dispatch_once(command(&[b"PING", message])),
                RespValue::BulkString(message.to_vec())
            );
        }
    }

    #[test]
    fn echo_returns_binary_safe_bulk_string_for_any_ascii_case() {
        for name in [b"ECHO".as_slice(), b"echo", b"EcHo"] {
            for message in [b"hello".as_slice(), b"", b"\0\xff\r\n"] {
                assert_eq!(
                    dispatch_once(command(&[name, message])),
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
            assert_eq!(
                dispatch_once(parts),
                RespValue::SimpleError(expected.to_vec())
            );
        }
    }

    #[test]
    fn unknown_commands_return_exact_error() {
        assert_eq!(
            dispatch_once(command(&[b"NOPE", b"key"])),
            RespValue::SimpleError(b"ERR unknown command".to_vec())
        );
    }

    #[test]
    fn set_creates_and_overwrites_values_for_get() {
        let database = Database::default();

        assert_eq!(
            dispatch(command(&[b"SET", b"key", b"first"]), &database),
            RespValue::SimpleString(b"OK".to_vec())
        );
        assert_eq!(
            dispatch(command(&[b"GET", b"key"]), &database),
            RespValue::BulkString(b"first".to_vec())
        );

        assert_eq!(
            dispatch(command(&[b"SET", b"key", b"second"]), &database),
            RespValue::SimpleString(b"OK".to_vec())
        );
        assert_eq!(
            dispatch(command(&[b"GET", b"key"]), &database),
            RespValue::BulkString(b"second".to_vec())
        );
    }

    #[test]
    fn get_of_missing_key_returns_null_bulk_string() {
        assert_eq!(
            dispatch_once(command(&[b"GET", b"missing"])),
            RespValue::NullBulkString
        );
    }

    #[test]
    fn set_and_get_are_binary_safe_and_ascii_case_insensitive() {
        let database = Database::default();

        assert_eq!(
            dispatch(command(&[b"sEt", b"\0\xff", b"\xff\0\r\n"]), &database),
            RespValue::SimpleString(b"OK".to_vec())
        );
        assert_eq!(
            dispatch(command(&[b"GeT", b"\0\xff"]), &database),
            RespValue::BulkString(b"\xff\0\r\n".to_vec())
        );
        assert_eq!(
            dispatch(command(&[b"SET", b"", b""]), &database),
            RespValue::SimpleString(b"OK".to_vec())
        );
        assert_eq!(
            dispatch(command(&[b"GET", b""]), &database),
            RespValue::BulkString(Vec::new())
        );
    }

    #[test]
    fn set_and_get_wrong_arities_return_exact_errors() {
        for (parts, expected) in [
            (
                command(&[b"SET", b"key"]),
                b"ERR wrong number of arguments for 'set' command".as_slice(),
            ),
            (
                command(&[b"SET", b"key", b"value", b"NX"]),
                b"ERR wrong number of arguments for 'set' command".as_slice(),
            ),
            (
                command(&[b"GET"]),
                b"ERR wrong number of arguments for 'get' command".as_slice(),
            ),
            (
                command(&[b"GET", b"key", b"extra"]),
                b"ERR wrong number of arguments for 'get' command".as_slice(),
            ),
        ] {
            assert_eq!(
                dispatch_once(parts),
                RespValue::SimpleError(expected.to_vec())
            );
        }
    }
}
