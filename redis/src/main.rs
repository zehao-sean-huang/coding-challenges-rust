mod command;
mod database;
mod logging;
mod server;

use crate::logging::LogMode;
use crate::server::ServerMode;
use std::env;
use std::ffi::OsStr;
use std::io::{self, Write};
use std::process::ExitCode;

const DEFAULT_ADDRESS: &str = "127.0.0.1:6379";
const USAGE: &str = "Usage: redis [--quiet] [--server <threaded|event-loop>]";

#[derive(Debug, Eq, PartialEq)]
struct Config {
    log_mode: LogMode,
    server_mode: ServerMode,
}

impl Config {
    fn parse<I, S>(arguments: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut arguments = arguments.into_iter();
        let mut log_mode = LogMode::Enabled;
        let mut server_mode = ServerMode::Threaded;
        let mut quiet_seen = false;
        let mut server_seen = false;

        while let Some(argument) = arguments.next() {
            let argument = argument.as_ref();
            if argument == "--quiet" {
                if quiet_seen {
                    return Err(parse_error("duplicate argument '--quiet'"));
                }
                quiet_seen = true;
                log_mode = LogMode::Disabled;
            } else if argument == "--server" {
                if server_seen {
                    return Err(parse_error("duplicate argument '--server'"));
                }
                server_seen = true;
                let Some(value) = arguments.next() else {
                    return Err(parse_error("missing value for '--server'"));
                };
                let value = value.as_ref();
                if value.to_string_lossy().starts_with('-') {
                    return Err(parse_error("missing value for '--server'"));
                }
                server_mode = match value.to_str() {
                    Some("threaded") => ServerMode::Threaded,
                    Some("event-loop") => ServerMode::EventLoop,
                    _ => {
                        return Err(parse_error(&format!(
                            "unknown server '{}'",
                            value.to_string_lossy()
                        )));
                    }
                };
            } else {
                return Err(parse_error(&format!(
                    "unknown argument '{}'",
                    argument.to_string_lossy()
                )));
            }
        }
        Ok(Self {
            log_mode,
            server_mode,
        })
    }
}

fn parse_error(message: &str) -> String {
    format!("{message}\n{USAGE}")
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
    match server::run(DEFAULT_ADDRESS, config.log_mode, config.server_mode) {
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
    use crate::server::ServerMode;
    use std::io;

    #[test]
    fn defaults_to_enabled_logging_and_threaded_server() {
        assert_eq!(
            Config::parse(std::iter::empty::<&str>()).unwrap(),
            Config {
                log_mode: LogMode::Enabled,
                server_mode: ServerMode::Threaded,
            }
        );
    }

    #[test]
    fn server_flag_selects_each_server_mode() {
        assert_eq!(
            Config::parse(["--server", "event-loop"])
                .unwrap()
                .server_mode,
            ServerMode::EventLoop
        );
        assert_eq!(
            Config::parse(["--server", "threaded"]).unwrap().server_mode,
            ServerMode::Threaded
        );
    }

    #[test]
    fn quiet_and_server_options_are_order_independent() {
        assert_eq!(
            Config::parse(["--quiet"]).unwrap().log_mode,
            LogMode::Disabled
        );
        for arguments in [
            ["--server", "threaded", "--quiet"],
            ["--quiet", "--server", "event-loop"],
        ] {
            assert_eq!(
                Config::parse(arguments).unwrap().log_mode,
                LogMode::Disabled
            );
        }
        assert_eq!(
            Config::parse(["--server", "threaded", "--quiet"])
                .unwrap()
                .server_mode,
            ServerMode::Threaded
        );
        assert_eq!(
            Config::parse(["--quiet", "--server", "event-loop"])
                .unwrap()
                .server_mode,
            ServerMode::EventLoop
        );
    }

    #[test]
    fn invalid_arguments_are_rejected_with_exact_errors_and_usage() {
        let usage = "Usage: redis [--quiet] [--server <threaded|event-loop>]";
        for (arguments, first_line) in [
            (vec!["--quiet", "--quiet"], "duplicate argument '--quiet'"),
            (
                vec!["--server", "threaded", "--server", "event-loop"],
                "duplicate argument '--server'",
            ),
            (vec!["--server"], "missing value for '--server'"),
            (vec!["--server", "--quiet"], "missing value for '--server'"),
            (
                vec!["--server", "worker-pool"],
                "unknown server 'worker-pool'",
            ),
            (vec!["--verbose"], "unknown argument '--verbose'"),
        ] {
            assert_eq!(
                Config::parse(arguments).unwrap_err(),
                format!("{first_line}\n{usage}")
            );
        }
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
