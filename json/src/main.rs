use std::env;
use std::fs;

mod parser;
mod tokenizer;

use parser::{parse_json_object, JsonObject};
use tokenizer::tokenize_json;

fn get_content_from_file(file_path: &str) -> String {
    fs::read_to_string(file_path)
        .unwrap_or_else(|_| panic!("the file {} cannot be read", file_path))
}

fn parse_json(content: &str) -> Result<JsonObject<'_>, String> {
    let tokens = tokenize_json(content)
        .map_err(|error| format!("the JSON cannot be tokenized: {error:?}"))?;

    parse_json_object(&tokens).map_err(|error| format!("the JSON cannot be parsed: {error:?}"))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("wrong number of arguments");
    }
    let content = get_content_from_file(&args[1]);
    let parsed = parse_json(&content).unwrap_or_else(|error| panic!("{error}"));

    println!("{:?}", &parsed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_step1_valid_json() {
        let content = include_str!("../tests/step1/valid.json");

        assert!(parse_json(content).is_ok());
    }

    #[test]
    fn rejects_step1_invalid_json() {
        let content = include_str!("../tests/step1/invalid.json");

        assert!(parse_json(content).is_err());
    }

    #[test]
    fn accepts_step2_valid_json() {
        let content = include_str!("../tests/step2/valid.json");

        assert!(parse_json(content).is_ok());
    }

    #[test]
    fn accepts_step2_valid_json_with_multiple_pairs() {
        let content = include_str!("../tests/step2/valid2.json");

        assert!(parse_json(content).is_ok());
    }

    #[test]
    fn rejects_step2_json_with_a_trailing_comma() {
        let content = include_str!("../tests/step2/invalid.json");

        assert!(parse_json(content).is_err());
    }

    #[test]
    fn rejects_step2_json_with_an_unquoted_key() {
        let content = include_str!("../tests/step2/invalid2.json");

        assert!(parse_json(content).is_err());
    }
}
