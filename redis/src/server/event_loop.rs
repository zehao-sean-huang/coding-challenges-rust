pub(super) mod connection;

pub(super) use connection::MAX_INCOMPLETE_BUFFER;

pub(super) const ACCEPT_BUDGET: usize = 64;
