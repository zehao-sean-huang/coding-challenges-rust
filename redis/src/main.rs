mod command;
mod database;
mod logging;
mod server;

use crate::logging::LogMode;
use std::env;
use std::ffi::OsStr;
use std::io::{self, Write};
use std::process::ExitCode;

const DEFAULT_ADDRESS: &str = "127.0.0.1:6379";
const USAGE: &str = "Usage: redis [--quiet]";

#[derive(Debug, Eq, PartialEq)]
struct Config {
    log_mode: LogMode,
}

impl Config {
    fn parse<I, S>(arguments: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut arguments = arguments.into_iter();
        let log_mode = match arguments.next() {
            None => LogMode::Enabled,
            Some(argument) if argument.as_ref() == "--quiet" => LogMode::Disabled,
            Some(argument) => return Err(argument_error(argument.as_ref())),
        };
        if let Some(argument) = arguments.next() {
            return Err(argument_error(argument.as_ref()));
        }
        Ok(Self { log_mode })
    }
}

fn argument_error(argument: &OsStr) -> String {
    format!("unknown argument '{}'\n{USAGE}", argument.to_string_lossy())
}

fn write_server_error<W: Write>(log_mode: LogMode, error: &io::Error, output: &mut W) {
    if log_mode == LogMode::Enabled {
        let _ = writeln!(output, "{error}");
    }
}

fn main() -> ExitCode {
    let config = match Config::parse(env::args_os().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    match server::run(DEFAULT_ADDRESS, config.log_mode) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            write_server_error(config.log_mode, &error, &mut io::stderr());
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_ADDRESS, write_server_error};
    use crate::logging::LogMode;
    use std::io;

    #[test]
    fn logging_is_enabled_by_default() {
        assert_eq!(
            Config::parse(std::iter::empty::<&str>()).unwrap().log_mode,
            LogMode::Enabled
        );
    }

    #[test]
    fn quiet_disables_logging() {
        assert_eq!(
            Config::parse(["--quiet"]).unwrap().log_mode,
            LogMode::Disabled
        );
    }

    #[test]
    fn unknown_argument_is_rejected_with_usage() {
        assert_eq!(
            Config::parse(["--verbose"]).unwrap_err(),
            "unknown argument '--verbose'\nUsage: redis [--quiet]"
        );
    }

    #[test]
    fn quiet_suppresses_server_errors() {
        let mut output = Vec::new();
        write_server_error(
            LogMode::Disabled,
            &io::Error::new(io::ErrorKind::AddrInUse, "in use"),
            &mut output,
        );
        assert!(output.is_empty());
    }

    #[test]
    fn enabled_logging_reports_server_errors() {
        let mut output = Vec::new();
        write_server_error(
            LogMode::Enabled,
            &io::Error::new(io::ErrorKind::AddrInUse, "in use"),
            &mut output,
        );
        assert_eq!(output, b"in use\n");
    }

    #[test]
    fn production_default_is_loopback_redis_port() {
        assert_eq!(DEFAULT_ADDRESS, "127.0.0.1:6379");
    }
}
