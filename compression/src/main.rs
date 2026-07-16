use std::env;
use std::fs;

mod freq;
mod huffman;

fn get_content_from_file(file_path: &str) -> String {
    fs::read_to_string(file_path)
        .unwrap_or_else(|_| panic!("the file {} cannot be read", file_path))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("wrong number of arguments");
    }
    let content = get_content_from_file(&args[1]);
    let freq_table = freq::construct_frequency_table(&content);
    let huffman_tree = huffman::HuffmanTree::new(&freq_table);

    println!("{:?}", freq_table.len());

    println!("{:?}", huffman_tree.root());
}
