mod command;
mod database;
mod logging;
mod server;

use std::io;

const DEFAULT_ADDRESS: &str = "127.0.0.1:6379";

fn main() -> io::Result<()> {
    server::run(DEFAULT_ADDRESS)
}

#[cfg(test)]
mod tests {
    use super::DEFAULT_ADDRESS;

    #[test]
    fn production_default_is_loopback_redis_port() {
        assert_eq!(DEFAULT_ADDRESS, "127.0.0.1:6379");
    }
}
