use std::env;
use std::fs;

mod tokenizer;

use tokenizer::{tokenize_json, Token};

#[derive(Debug)]
enum Json {
    Obj,
    Value,
}

fn get_content_from_file(file_path: &str) -> String {
    fs::read_to_string(file_path).expect(format!("the file {} cannot be read", file_path).as_str())
}

#[derive(Debug, PartialEq)]
struct JsonObject<'input> {
    key: &'input str,
    value: &'input str,
}

struct Parser<'tokens, 'input> {
    tokens: &'tokens [Token<'input>],
    position: usize,
}

fn parse_json_obj<'a>(tokens: &'a Vec<&'a str>) -> Json {
    if tokens.is_empty() || tokens[0] != "{" {
        panic!("expecting '{{' to start a json object");
    }
    let mut result = Json::Obj;
    if tokens.len() < 2 || tokens[1] != "}" {
        panic!("expecting '}}' after '{{'");
    }
    result
}

fn parse_json_str<'a>(tokens: &'a Vec<&'a str>) -> &'a str {
    todo!()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("wrong number of arguments");
    }
    let content = get_content_from_file(&args[1]);
    let tokenized = tokenize_json(&content).expect("the JSON cannot be tokenized");

    println!("{:?}", &tokenized);
}
