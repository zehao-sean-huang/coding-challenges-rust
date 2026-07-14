use crate::tokenizer::Token;

const MAX_NESTING_DEPTH: usize = 19;

#[derive(Debug, PartialEq)]
pub(crate) enum JsonValue<'input> {
    String(&'input str),
    Number(&'input str),
    Boolean(bool),
    Null,
    Object(JsonObject<'input>),
    Array(Vec<JsonValue<'input>>),
}

#[derive(Debug, PartialEq)]
pub(crate) struct JsonPair<'input> {
    pub(crate) key: &'input str,
    pub(crate) value: JsonValue<'input>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct JsonObject<'input> {
    pub(crate) pairs: Vec<JsonPair<'input>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ExpectedToken {
    LeftBrace,
    LeftBracket,
    Document,
    String,
    Value,
    Colon,
    Comma,
    RightBrace,
    RightBracket,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ParseError<'input> {
    UnexpectedEnd {
        position: usize,
        expected: ExpectedToken,
    },
    UnexpectedToken {
        position: usize,
        expected: ExpectedToken,
        found: Token<'input>,
    },
    TrailingToken {
        position: usize,
        found: Token<'input>,
    },
    NestingTooDeep {
        position: usize,
        max_depth: usize,
    },
}

struct Parser<'tokens, 'input> {
    tokens: &'tokens [Token<'input>],
    position: usize,
    depth: usize,
}

impl<'tokens, 'input> Parser<'tokens, 'input> {
    fn new(tokens: &'tokens [Token<'input>]) -> Self {
        Self {
            tokens,
            position: 0,
            depth: 0,
        }
    }

    fn next(&mut self) -> Option<Token<'input>> {
        let token = self.tokens.get(self.position).copied();

        if token.is_some() {
            self.position += 1;
        }

        token
    }

    fn peek_token(&self) -> Option<Token<'input>> {
        self.tokens.get(self.position).copied()
    }

    fn expect_token(
        &mut self,
        expected_token: Token<'input>,
        expected: ExpectedToken,
    ) -> Result<(), ParseError<'input>> {
        let position = self.position;

        match self.next() {
            Some(found) if found == expected_token => Ok(()),
            Some(found) => Err(ParseError::UnexpectedToken {
                position,
                expected,
                found,
            }),
            None => Err(ParseError::UnexpectedEnd { position, expected }),
        }
    }

    fn expect_string(&mut self) -> Result<&'input str, ParseError<'input>> {
        let position = self.position;

        match self.next() {
            Some(Token::String(value)) => Ok(value),
            Some(found) => Err(ParseError::UnexpectedToken {
                position,
                expected: ExpectedToken::String,
                found,
            }),
            None => Err(ParseError::UnexpectedEnd {
                position,
                expected: ExpectedToken::String,
            }),
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue<'input>, ParseError<'input>> {
        let position = self.position;

        match self.peek_token() {
            Some(Token::String(value)) => {
                self.next();
                Ok(JsonValue::String(value))
            }
            Some(Token::Number(value)) => {
                self.next();
                Ok(JsonValue::Number(value))
            }
            Some(Token::Boolean(value)) => {
                self.next();
                Ok(JsonValue::Boolean(value))
            }
            Some(Token::Null) => {
                self.next();
                Ok(JsonValue::Null)
            }
            Some(Token::LeftBrace) => self.parse_object().map(JsonValue::Object),
            Some(Token::LeftBracket) => self.parse_array().map(JsonValue::Array),
            Some(found) => Err(ParseError::UnexpectedToken {
                position,
                expected: ExpectedToken::Value,
                found,
            }),
            None => Err(ParseError::UnexpectedEnd {
                position,
                expected: ExpectedToken::Value,
            }),
        }
    }

    fn expect_end(&self) -> Result<(), ParseError<'input>> {
        match self.peek_token() {
            Some(found) => Err(ParseError::TrailingToken {
                position: self.position,
                found,
            }),
            None => Ok(()),
        }
    }

    fn enter_container(&mut self) -> Result<(), ParseError<'input>> {
        if self.depth == MAX_NESTING_DEPTH {
            return Err(ParseError::NestingTooDeep {
                position: self.position,
                max_depth: MAX_NESTING_DEPTH,
            });
        }

        self.depth += 1;
        Ok(())
    }

    fn parse_pair(&mut self) -> Result<JsonPair<'input>, ParseError<'input>> {
        let key = self.expect_string()?;
        self.expect_token(Token::Colon, ExpectedToken::Colon)?;
        let value = self.parse_value()?;

        Ok(JsonPair { key, value })
    }

    fn parse_object(&mut self) -> Result<JsonObject<'input>, ParseError<'input>> {
        self.enter_container()?;
        self.expect_token(Token::LeftBrace, ExpectedToken::LeftBrace)?;
        let mut pairs = Vec::new();

        while self.peek_token() != Some(Token::RightBrace) {
            if !pairs.is_empty() {
                self.expect_token(Token::Comma, ExpectedToken::Comma)?;
            }

            pairs.push(self.parse_pair()?);
        }

        self.expect_token(Token::RightBrace, ExpectedToken::RightBrace)?;
        self.depth -= 1;

        Ok(JsonObject { pairs })
    }

    fn parse_array(&mut self) -> Result<Vec<JsonValue<'input>>, ParseError<'input>> {
        self.enter_container()?;
        self.expect_token(Token::LeftBracket, ExpectedToken::LeftBracket)?;
        let mut values = Vec::new();

        while self.peek_token() != Some(Token::RightBracket) {
            if !values.is_empty() {
                self.expect_token(Token::Comma, ExpectedToken::Comma)?;
            }

            values.push(self.parse_value()?);
        }

        self.expect_token(Token::RightBracket, ExpectedToken::RightBracket)?;
        self.depth -= 1;

        Ok(values)
    }

    fn parse_document(&mut self) -> Result<JsonValue<'input>, ParseError<'input>> {
        let value = match self.peek_token() {
            Some(Token::LeftBrace) => self.parse_object().map(JsonValue::Object)?,
            Some(Token::LeftBracket) => self.parse_array().map(JsonValue::Array)?,
            Some(found) => {
                return Err(ParseError::UnexpectedToken {
                    position: self.position,
                    expected: ExpectedToken::Document,
                    found,
                });
            }
            None => {
                return Err(ParseError::UnexpectedEnd {
                    position: self.position,
                    expected: ExpectedToken::Document,
                });
            }
        };

        self.expect_end()?;
        Ok(value)
    }
}

pub(crate) fn parse_json_document<'input>(
    tokens: &[Token<'input>],
) -> Result<JsonValue<'input>, ParseError<'input>> {
    Parser::new(tokens).parse_document()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::tokenize_json;

    fn parse_json_object<'input>(
        tokens: &[Token<'input>],
    ) -> Result<JsonObject<'input>, ParseError<'input>> {
        match parse_json_document(tokens)? {
            JsonValue::Object(object) => Ok(object),
            _ => unreachable!("the test helper only receives object documents"),
        }
    }

    #[test]
    fn parses_one_key_value_pair() {
        let tokens = tokenize_json(r#"{"key": "value"}"#).unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Ok(JsonObject {
                pairs: vec![JsonPair {
                    key: "key",
                    value: JsonValue::String("value"),
                }],
            })
        );
    }

    #[test]
    fn parses_an_empty_object() {
        let tokens = tokenize_json("{}").unwrap();

        assert_eq!(parse_json_object(&tokens), Ok(JsonObject { pairs: vec![] }));
    }

    #[test]
    fn rejects_a_missing_colon() {
        let tokens = tokenize_json(r#"{"key" "value"}"#).unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Err(ParseError::UnexpectedToken {
                position: 2,
                expected: ExpectedToken::Colon,
                found: Token::String("value"),
            })
        );
    }

    #[test]
    fn parses_many_key_value_pairs() {
        let tokens = tokenize_json(r#"{"key": "value", "other": "value"}"#).unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Ok(JsonObject {
                pairs: vec![
                    JsonPair {
                        key: "key",
                        value: JsonValue::String("value"),
                    },
                    JsonPair {
                        key: "other",
                        value: JsonValue::String("value"),
                    },
                ],
            })
        );
    }

    #[test]
    fn rejects_a_missing_comma() {
        let tokens = tokenize_json(r#"{"key": "value" "other": "value"}"#).unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Err(ParseError::UnexpectedToken {
                position: 4,
                expected: ExpectedToken::Comma,
                found: Token::String("other"),
            })
        );
    }

    #[test]
    fn rejects_a_trailing_comma() {
        let tokens = tokenize_json(r#"{"key": "value",}"#).unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Err(ParseError::UnexpectedToken {
                position: 5,
                expected: ExpectedToken::String,
                found: Token::RightBrace,
            })
        );
    }

    #[test]
    fn rejects_tokens_after_the_object() {
        let tokens = tokenize_json(r#"{"key": "value"}{}"#).unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Err(ParseError::TrailingToken {
                position: 5,
                found: Token::LeftBrace,
            })
        );
    }

    #[test]
    fn reports_unexpected_end_of_input() {
        assert_eq!(
            parse_json_object(&[]),
            Err(ParseError::UnexpectedEnd {
                position: 0,
                expected: ExpectedToken::Document,
            })
        );
    }

    #[test]
    fn parses_supported_value_types() {
        let tokens = tokenize_json(
            r#"{"string":"value","integer":101,"decimal":1223.23,"yes":true,"no":false,"nothing":null}"#,
        )
        .unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Ok(JsonObject {
                pairs: vec![
                    JsonPair {
                        key: "string",
                        value: JsonValue::String("value"),
                    },
                    JsonPair {
                        key: "integer",
                        value: JsonValue::Number("101"),
                    },
                    JsonPair {
                        key: "decimal",
                        value: JsonValue::Number("1223.23"),
                    },
                    JsonPair {
                        key: "yes",
                        value: JsonValue::Boolean(true),
                    },
                    JsonPair {
                        key: "no",
                        value: JsonValue::Boolean(false),
                    },
                    JsonPair {
                        key: "nothing",
                        value: JsonValue::Null,
                    },
                ],
            })
        );
    }

    #[test]
    fn parses_nested_objects_and_arrays() {
        let tokens = tokenize_json(
            r#"{"object":{"inner":"value"},"array":["text",101,true,null,{"nested":[]},[false]]}"#,
        )
        .unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Ok(JsonObject {
                pairs: vec![
                    JsonPair {
                        key: "object",
                        value: JsonValue::Object(JsonObject {
                            pairs: vec![JsonPair {
                                key: "inner",
                                value: JsonValue::String("value"),
                            }],
                        }),
                    },
                    JsonPair {
                        key: "array",
                        value: JsonValue::Array(vec![
                            JsonValue::String("text"),
                            JsonValue::Number("101"),
                            JsonValue::Boolean(true),
                            JsonValue::Null,
                            JsonValue::Object(JsonObject {
                                pairs: vec![JsonPair {
                                    key: "nested",
                                    value: JsonValue::Array(vec![]),
                                }],
                            }),
                            JsonValue::Array(vec![JsonValue::Boolean(false)]),
                        ]),
                    },
                ],
            })
        );
    }

    #[test]
    fn rejects_a_trailing_comma_in_an_array() {
        let tokens = tokenize_json(r#"{"array":[true,]}"#).unwrap();

        assert_eq!(
            parse_json_object(&tokens),
            Err(ParseError::UnexpectedToken {
                position: 6,
                expected: ExpectedToken::Value,
                found: Token::RightBracket,
            })
        );
    }
}
