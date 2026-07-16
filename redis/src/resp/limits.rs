#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CodecLimits {
    pub max_bulk_len: usize,
    pub max_aggregate_len: usize,
    pub max_depth: usize,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_bulk_len: 536_870_912,
            max_aggregate_len: 1_000_000,
            max_depth: 128,
        }
    }
}
