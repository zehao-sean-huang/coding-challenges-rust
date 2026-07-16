use std::collections::HashMap;

pub(crate) fn construct_frequency_table(content: &str) -> HashMap<char, usize> {
    let mut table = HashMap::new();
    for c in content.chars() {
        *table.entry(c).or_insert(0) += 1;
    }
    table
}
