mod threaded;

use crate::logging::LogMode;
use std::io;

pub(crate) fn run(address: &str, log_mode: LogMode) -> io::Result<()> {
    threaded::run(address, log_mode)
}
