use core::panic;
use std::{
    env::{self},
    fs, io,
};

enum CountKind {
    Byte,
    Line,
    Word,
    Char,
}

impl CountKind {
    fn from_arg(arg: &str) -> CountKind {
        CountKind::try_from_arg(arg).unwrap()
    }

    fn try_from_arg(arg: &str) -> Option<CountKind> {
        match arg {
            "-c" => Some(CountKind::Byte),
            "-l" => Some(CountKind::Line),
            "-w" => Some(CountKind::Word),
            "-m" => Some(CountKind::Char),
            _ => None,
        }
    }
}

fn get_content_from_file(file_path: &str) -> String {
    fs::read_to_string(file_path).expect(format!("the file {} cannot be read", file_path).as_str())
}

fn get_content_from_stdin() -> String {
    let reader = io::BufReader::new(io::stdin());
    io::read_to_string(reader).unwrap()
}

fn count(kind: &CountKind, content: &String) -> usize {
    match kind {
        CountKind::Byte => content.len(),
        CountKind::Line => content.split_inclusive("\n").count(),
        CountKind::Word => content.split_whitespace().count(),
        CountKind::Char => content.chars().count(),
    }
}

fn count_all(content: &String) -> (usize, usize, usize) {
    let lines = count(&CountKind::Line, &content);
    let words = count(&CountKind::Word, &content);
    let bytes = count(&CountKind::Byte, &content);

    (lines, words, bytes)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    match args.len() {
        1 => {
            let content = get_content_from_stdin();
            let (lines, words, bytes) = count_all(&content);
            println!("{} {} {}", lines, words, bytes);
        }
        2 => {
            if let Some(kind) = CountKind::try_from_arg(&args[1]) {
                let content = get_content_from_stdin();
                let count = count(&kind, &content);
                println!("{}", count);
            } else {
                let content = get_content_from_file(&args[1]);
                let (lines, words, bytes) = count_all(&content);
                println!("{} {} {} {}", lines, words, bytes, args[1]);
            }
        }
        3 => {
            let content = get_content_from_file(&args[2]);
            let kind = CountKind::from_arg(&args[1]);
            let result = count(&kind, &content);
            println!("{} {}", result, args[2]);
        }
        _ => panic!("wrong number of arguments"),
    }
}
