use core::panic;
use std::env;
use std::fs::{self, File};

mod deserialize;
mod freq;
mod huffman;
mod serialize;

fn get_content_from_file(file_path: &str) -> String {
    fs::read_to_string(file_path)
        .unwrap_or_else(|_| panic!("the file {} cannot be read", file_path))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        panic!("usage: compression <encode|decode> <input> <output>");
    }
    let (mode, in_file, out_file) = (&args[1], &args[2], &args[3]);
    if in_file == out_file {
        panic!("in file and out file cannot be the same");
    }

    match mode.as_str() {
        "encode" => encode_file(in_file, out_file),
        "decode" => decode_file(in_file, out_file),
        _ => panic!("unknown mode {mode:?}; expected encode or decode"),
    }
}

fn encode_file(in_file: &str, out_file: &str) {
    let content = get_content_from_file(in_file);
    let freq_table = freq::construct_frequency_table(&content);
    let prefix_table = if freq_table.is_empty() {
        Default::default()
    } else {
        huffman::HuffmanTree::new(&freq_table).to_prefix_table()
    };
    let output = File::create(out_file)
        .unwrap_or_else(|_| panic!("the output file {} cannot be created", out_file));
    serialize::encode(&content, &prefix_table, output).unwrap_or_else(|error| {
        panic!("the output file {} cannot be written: {}", out_file, error)
    });
}

fn decode_file(in_file: &str, out_file: &str) {
    let input = File::open(in_file)
        .unwrap_or_else(|_| panic!("the input file {} cannot be opened", in_file));
    let output = File::create(out_file)
        .unwrap_or_else(|_| panic!("the output file {} cannot be created", out_file));
    deserialize::decode(input, output)
        .unwrap_or_else(|error| panic!("the input file {} cannot be decoded: {}", in_file, error));
}
