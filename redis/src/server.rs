mod connection_logging;
mod event_loop;
mod threaded;

#[cfg(test)]
mod tests;

use crate::logging::LogMode;
use std::io;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServerMode {
    Threaded,
    EventLoop,
}

pub(crate) fn run(address: &str, log_mode: LogMode, server_mode: ServerMode) -> io::Result<()> {
    match server_mode {
        ServerMode::Threaded => threaded::run(address, log_mode),
        ServerMode::EventLoop => event_loop::run(address, log_mode),
    }
}
