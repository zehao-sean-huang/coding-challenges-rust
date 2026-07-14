#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Token<'input> {
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,
    Colon,
    Comma,
    String(&'input str),
    Number(&'input str),
    Boolean(bool),
    Null,
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
    InvalidUnicodeEscape {
        position: usize,
    },
    InvalidNumber {
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
                let escape_start = *position;
                *position += 1;

                match bytes.get(*position).copied() {
                    Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {
                        *position += 1;
                    }
                    Some(b'u') => {
                        *position += 1;

                        for _ in 0..4 {
                            match bytes.get(*position).copied() {
                                Some(byte) if byte.is_ascii_hexdigit() => *position += 1,
                                _ => {
                                    return Err(TokenizeError::InvalidUnicodeEscape {
                                        position: *position,
                                    });
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(TokenizeError::UnsupportedEscape {
                            position: escape_start,
                        });
                    }
                }
            }
            0x00..=0x1f => {
                return Err(TokenizeError::ControlCharacter {
                    position: *position,
                });
            }
            _ => *position += 1,
        }
    }

    Err(TokenizeError::UnterminatedString {
        position: opening_quote,
    })
}

fn tokenize_keyword<'input>(
    content: &'input str,
    position: &mut usize,
    keyword: &[u8],
    token: Token<'input>,
) -> Result<Token<'input>, TokenizeError> {
    let bytes = content.as_bytes();

    for expected in keyword {
        expect_byte(bytes, position, *expected)?;
    }

    Ok(token)
}

fn tokenize_number<'input>(
    content: &'input str,
    position: &mut usize,
) -> Result<Token<'input>, TokenizeError> {
    let bytes = content.as_bytes();
    let start = *position;

    if bytes.get(*position) == Some(&b'-') {
        *position += 1;
    }

    match bytes.get(*position).copied() {
        Some(b'0') => {
            *position += 1;

            if bytes.get(*position).is_some_and(u8::is_ascii_digit) {
                return Err(TokenizeError::InvalidNumber {
                    position: *position,
                });
            }
        }
        Some(b'1'..=b'9') => {
            while bytes.get(*position).is_some_and(u8::is_ascii_digit) {
                *position += 1;
            }
        }
        _ => {
            return Err(TokenizeError::InvalidNumber {
                position: *position,
            });
        }
    }

    if bytes.get(*position) == Some(&b'.') {
        *position += 1;
        let fraction_start = *position;

        while bytes.get(*position).is_some_and(u8::is_ascii_digit) {
            *position += 1;
        }

        if *position == fraction_start {
            return Err(TokenizeError::InvalidNumber {
                position: fraction_start,
            });
        }
    }

    if matches!(bytes.get(*position), Some(b'e' | b'E')) {
        *position += 1;

        if matches!(bytes.get(*position), Some(b'+' | b'-')) {
            *position += 1;
        }

        let exponent_start = *position;

        while bytes.get(*position).is_some_and(u8::is_ascii_digit) {
            *position += 1;
        }

        if *position == exponent_start {
            return Err(TokenizeError::InvalidNumber {
                position: exponent_start,
            });
        }
    }

    Ok(Token::Number(&content[start..*position]))
}

pub(crate) fn tokenize_json(content: &str) -> Result<Vec<Token<'_>>, TokenizeError> {
    let mut result = vec![];
    let bytes = content.as_bytes();
    let mut position = 0;

    while position < bytes.len() {
        match bytes[position] {
            b'{' => result.push(Token::LeftBrace),
            b'}' => result.push(Token::RightBrace),
            b'[' => result.push(Token::LeftBracket),
            b']' => result.push(Token::RightBracket),
            b':' => result.push(Token::Colon),
            b',' => result.push(Token::Comma),
            b' ' | b'\n' | b'\r' | b'\t' => {}
            b'"' => {
                result.push(tokenize_string(content, &mut position)?);
                continue;
            }
            b'-' | b'0'..=b'9' => {
                result.push(tokenize_number(content, &mut position)?);
                continue;
            }
            b't' => {
                result.push(tokenize_keyword(
                    content,
                    &mut position,
                    b"true",
                    Token::Boolean(true),
                )?);
                continue;
            }
            b'f' => {
                result.push(tokenize_keyword(
                    content,
                    &mut position,
                    b"false",
                    Token::Boolean(false),
                )?);
                continue;
            }
            b'n' => {
                result.push(tokenize_keyword(
                    content,
                    &mut position,
                    b"null",
                    Token::Null,
                )?);
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
    fn accepts_supported_escapes() {
        let input =
            r#"{"key":"quote: \" slash: \/ backslash: \\ controls: \b\f\n\r\t unicode: \uCAFE"}"#;

        assert!(tokenize_json(input).is_ok());
    }

    #[test]
    fn rejects_unsupported_escapes() {
        let input = r#"{"key": "escaped\nvalue"}"#;

        assert!(tokenize_json(input).is_ok());

        let input = r#"{"key": "escaped\xvalue"}"#;

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

    #[test]
    fn tokenizes_supported_value_types() {
        let input = r#"{"a":true,"b":false,"c":null,"d":101,"e":1223.23}"#;

        assert_eq!(
            tokenize_json(input),
            Ok(vec![
                Token::LeftBrace,
                Token::String("a"),
                Token::Colon,
                Token::Boolean(true),
                Token::Comma,
                Token::String("b"),
                Token::Colon,
                Token::Boolean(false),
                Token::Comma,
                Token::String("c"),
                Token::Colon,
                Token::Null,
                Token::Comma,
                Token::String("d"),
                Token::Colon,
                Token::Number("101"),
                Token::Comma,
                Token::String("e"),
                Token::Colon,
                Token::Number("1223.23"),
                Token::RightBrace,
            ])
        );
    }

    #[test]
    fn tokenizes_full_json_number_syntax() {
        let input = r#"{"number":-12.5e+2}"#;

        assert!(tokenize_json(input)
            .is_ok_and(|tokens| { tokens.contains(&Token::Number("-12.5e+2")) }));
    }

    #[test]
    fn rejects_invalid_numbers() {
        assert_eq!(
            tokenize_json(r#"{"number":01}"#),
            Err(TokenizeError::InvalidNumber { position: 11 })
        );
        assert_eq!(
            tokenize_json(r#"{"number":1.}"#),
            Err(TokenizeError::InvalidNumber { position: 12 })
        );
    }

    #[test]
    fn tokenizes_array_and_object_delimiters() {
        let input = r#"{"array":[{},[null]]}"#;

        assert_eq!(
            tokenize_json(input),
            Ok(vec![
                Token::LeftBrace,
                Token::String("array"),
                Token::Colon,
                Token::LeftBracket,
                Token::LeftBrace,
                Token::RightBrace,
                Token::Comma,
                Token::LeftBracket,
                Token::Null,
                Token::RightBracket,
                Token::RightBracket,
                Token::RightBrace,
            ])
        );
    }
}
