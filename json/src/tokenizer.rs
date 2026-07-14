#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Token<'input> {
    LeftBrace,
    RightBrace,
    Colon,
    Comma,
    String(&'input str),
}

#[derive(Debug, PartialEq)]
pub(crate) enum TokenizeError {
    UnexpectedCharacter {
        position: usize,
        byte: u8,
    },
    ExpectedByte {
        position: usize,
        expected: u8,
        found: Option<u8>,
    },
    UnsupportedEscape {
        position: usize,
    },
    ControlCharacter {
        position: usize,
    },
    UnterminatedString {
        position: usize,
    },
}

fn expect_byte(bytes: &[u8], position: &mut usize, expected: u8) -> Result<(), TokenizeError> {
    match bytes.get(*position).copied() {
        Some(found) if found == expected => {
            *position += 1;
            Ok(())
        }
        found => Err(TokenizeError::ExpectedByte {
            position: *position,
            expected,
            found,
        }),
    }
}

fn tokenize_string<'input>(
    content: &'input str,
    position: &mut usize,
) -> Result<Token<'input>, TokenizeError> {
    let bytes = content.as_bytes();
    let opening_quote = *position;

    expect_byte(bytes, position, b'"')?;
    let start = *position;

    while let Some(byte) = bytes.get(*position).copied() {
        match byte {
            b'"' => {
                let end = *position;
                expect_byte(bytes, position, b'"')?;
                return Ok(Token::String(&content[start..end]));
            }
            b'\\' => {
                return Err(TokenizeError::UnsupportedEscape {
                    position: *position,
                });
            }
            0x00..=0x1f => {
                return Err(TokenizeError::ControlCharacter {
                    position: *position,
                });
            }
            0x80..=0xff => {
                return Err(TokenizeError::UnexpectedCharacter {
                    position: *position,
                    byte,
                });
            }
            _ => *position += 1,
        }
    }

    Err(TokenizeError::UnterminatedString {
        position: opening_quote,
    })
}

pub(crate) fn tokenize_json(content: &str) -> Result<Vec<Token<'_>>, TokenizeError> {
    let mut result = vec![];
    let bytes = content.as_bytes();
    let mut position = 0;

    while position < bytes.len() {
        match bytes[position] {
            b'{' => result.push(Token::LeftBrace),
            b'}' => result.push(Token::RightBrace),
            b':' => result.push(Token::Colon),
            b',' => result.push(Token::Comma),
            b' ' | b'\n' | b'\r' | b'\t' => {}
            b'"' => {
                result.push(tokenize_string(content, &mut position)?);
                continue;
            }
            byte => return Err(TokenizeError::UnexpectedCharacter { position, byte }),
        }

        position += 1;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_one_key_value_pair() {
        let input = "{ \"key\": \"value\" }";

        assert_eq!(
            tokenize_json(input),
            Ok(vec![
                Token::LeftBrace,
                Token::String("key"),
                Token::Colon,
                Token::String("value"),
                Token::RightBrace,
            ])
        );
    }

    #[test]
    fn preserves_whitespace_inside_strings() {
        let input = "{\"first key\": \"first value\"}";

        assert_eq!(
            tokenize_json(input),
            Ok(vec![
                Token::LeftBrace,
                Token::String("first key"),
                Token::Colon,
                Token::String("first value"),
                Token::RightBrace,
            ])
        );
    }

    #[test]
    fn rejects_unsupported_escapes() {
        let input = r#"{"key": "escaped\nvalue"}"#;

        assert_eq!(
            tokenize_json(input),
            Err(TokenizeError::UnsupportedEscape { position: 16 })
        );
    }

    #[test]
    fn rejects_unterminated_strings() {
        let input = r#"{"key": "value}"#;

        assert_eq!(
            tokenize_json(input),
            Err(TokenizeError::UnterminatedString { position: 8 })
        );
    }
}
