use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
};

pub(crate) struct HuffmanTree {
    root: HuffmanNode,
}

impl HuffmanTree {
    pub(crate) fn new(freq_table: &HashMap<char, usize>) -> Self {
        let mut min_heap = BinaryHeap::new();
        freq_table
            .iter()
            .map(|(c, count)| HuffmanNode::Leaf {
                c: *c,
                count: *count,
            })
            .for_each(|leaf| min_heap.push(Reverse(leaf)));

        while min_heap.len() >= 2 {
            let Reverse(left) = min_heap.pop().unwrap();
            let Reverse(right) = min_heap.pop().unwrap();
            let count = &left.count() + &right.count();
            let intermediate = HuffmanNode::Intermediate {
                left: Box::new(left),
                right: Box::new(right),
                count,
            };
            min_heap.push(Reverse(intermediate));
        }

        let Reverse(root) = min_heap.pop().unwrap();
        Self { root }
    }

    pub(crate) fn root(&self) -> &HuffmanNode {
        &self.root
    }

    pub(crate) fn to_prefix_table(&self) -> HashMap<char, (u32, u32)> {
        let mut result = HashMap::new();
        let mut current = 0u32;
        let mut depth = 0u32;
        self.to_prefix_table_helper(&self.root, &mut current, &mut depth, &mut result);
        result
    }

    fn to_prefix_table_helper(
        &self,
        node: &HuffmanNode,
        current: &mut u32,
        depth: &mut u32,
        result: &mut HashMap<char, (u32, u32)>,
    ) {
        match node {
            HuffmanNode::Intermediate { left, right, .. } => {
                *depth += 1;
                *current <<= 1;
                self.to_prefix_table_helper(left, current, depth, result);
                *current += 1;
                self.to_prefix_table_helper(right, current, depth, result);
                *current >>= 1;
                *depth -= 1;
            }
            HuffmanNode::Leaf { c, .. } => {
                result.insert(*c, (*current, *depth));
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HuffmanNode {
    Leaf {
        c: char,
        count: usize,
    },
    Intermediate {
        left: Box<HuffmanNode>,
        right: Box<HuffmanNode>,
        count: usize,
    },
}

impl HuffmanNode {
    fn count(&self) -> usize {
        match self {
            HuffmanNode::Leaf { count, .. } => *count,
            HuffmanNode::Intermediate { count, .. } => *count,
        }
    }
}

impl Ord for HuffmanNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count().cmp(&other.count())
    }
}

impl PartialOrd for HuffmanNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::{HuffmanNode, HuffmanTree};
    use std::collections::{HashMap, HashSet};

    fn frequencies(entries: &[(char, usize)]) -> HashMap<char, usize> {
        entries.iter().copied().collect()
    }

    fn assert_tree_counts_are_consistent(node: &HuffmanNode) -> usize {
        match node {
            HuffmanNode::Leaf { count, .. } => *count,
            HuffmanNode::Intermediate { left, right, count } => {
                let child_sum = assert_tree_counts_are_consistent(left)
                    + assert_tree_counts_are_consistent(right);
                assert_eq!(*count, child_sum);
                *count
            }
        }
    }

    fn is_prefix(first: (u32, u32), second: (u32, u32)) -> bool {
        let (first_bits, first_len) = first;
        let (second_bits, second_len) = second;

        first_len <= second_len && first_bits == (second_bits >> (second_len - first_len))
    }

    #[test]
    fn tree_root_count_is_the_sum_of_all_frequencies() {
        let frequencies = frequencies(&[('a', 2), ('b', 3), ('c', 5), ('d', 11)]);
        let tree = HuffmanTree::new(&frequencies);

        assert_eq!(assert_tree_counts_are_consistent(tree.root()), 21);
    }

    #[test]
    fn prefix_table_contains_every_input_character_exactly_once() {
        let frequencies = frequencies(&[('a', 1), ('é', 2), ('🦀', 3), ('中', 4)]);
        let tree = HuffmanTree::new(&frequencies);
        let table = tree.to_prefix_table();

        assert_eq!(table.len(), frequencies.len());
        assert_eq!(
            table.keys().copied().collect::<HashSet<_>>(),
            frequencies.keys().copied().collect()
        );
    }

    #[test]
    fn two_characters_are_encoded_with_one_bit_each() {
        let tree = HuffmanTree::new(&frequencies(&[('a', 1), ('b', 2)]));
        let table = tree.to_prefix_table();

        let mut encodings = table.values().copied().collect::<Vec<_>>();
        encodings.sort_unstable();
        assert_eq!(encodings, vec![(0, 1), (1, 1)]);
    }

    #[test]
    fn generated_encodings_are_prefix_free() {
        let tree = HuffmanTree::new(&frequencies(&[
            ('a', 5),
            ('b', 9),
            ('c', 12),
            ('d', 13),
            ('e', 16),
            ('f', 45),
        ]));
        let table = tree.to_prefix_table();
        let encodings = table.iter().collect::<Vec<_>>();

        for (index, (first_char, first_encoding)) in encodings.iter().enumerate() {
            let (bits, length) = **first_encoding;
            assert!(length <= u32::BITS);
            if length < u32::BITS {
                assert!(bits < (1u32 << length));
            }

            for (second_char, second_encoding) in encodings.iter().skip(index + 1) {
                assert!(
                    !is_prefix(**first_encoding, **second_encoding)
                        && !is_prefix(**second_encoding, **first_encoding),
                    "encodings for {first_char:?} and {second_char:?} overlap"
                );
            }
        }
    }

    #[test]
    fn more_frequent_symbol_does_not_get_a_longer_encoding() {
        let tree = HuffmanTree::new(&frequencies(&[
            ('a', 1),
            ('b', 2),
            ('c', 4),
            ('d', 8),
            ('e', 16),
        ]));
        let table = tree.to_prefix_table();

        for (less_frequent, more_frequent) in [('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e')] {
            assert!(table[&more_frequent].1 <= table[&less_frequent].1);
        }
    }
}
