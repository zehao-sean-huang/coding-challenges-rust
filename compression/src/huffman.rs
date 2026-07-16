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
