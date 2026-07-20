mod connection_logging;
// Task 5 wires this staged state machine into the selected server path.
#[allow(dead_code, unused_imports)]
mod event_loop;
mod threaded;

use crate::logging::LogMode;
use std::io;

pub(crate) fn run(address: &str, log_mode: LogMode) -> io::Result<()> {
    threaded::run(address, log_mode)
}
