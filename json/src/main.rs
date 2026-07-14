use std::env;
use std::fs;

#[derive(Debug)]
enum Json {
    Obj,
    Value,
}

fn get_content_from_file(file_path: &str) -> String {
    fs::read_to_string(file_path).expect(format!("the file {} cannot be read", file_path).as_str())
}

fn tokenize_json<'a>(content: &'a String) -> Vec<&'a str> {
    let mut result = vec![];

    let mut start = -1;    
    let mut in_whitespace = false;
    let mut in_alphanumeric = false;

    for (i, c) in content.chars().enumerate() {
        match c {
            '{' | '}' | '"' | ':' | '[' | ']' => {
                result.push(&content[i..i + 1]);
                in_whitespace = false;
                in_alphanumeric = false;
            },
            ' ' | '\n' | '\r' | '\t' => {
                
            },
            c if c.is_alphanumeric() {
                todo!()
            }
        }
    }

    result
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
    let tokenized = tokenize_json(&content);
    let parsed = parse_json_obj(&tokenized);

    println!("{:?}", &parsed);
}
