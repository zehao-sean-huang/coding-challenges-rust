pub(super) mod connection;

pub(super) use connection::MAX_INCOMPLETE_BUFFER;

use self::connection::{Connection, DesiredInterest, Readiness};
use super::connection_logging::{DisabledLogger, EnabledLogger, EventLogger, client_identity};
use crate::database::Database;
use crate::logging::LogMode;
use mio::event::Event;
use mio::{Events, Interest, Poll, Token};
use redis::resp::{Encoder, RequestDecoder};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

pub(super) const ACCEPT_BUDGET: usize = 64;
const LISTENER: Token = Token(0);
const MAX_CONTINUATIONS_PER_POLL: usize = 256;
const EVENT_CAPACITY: usize = 1024;

pub(super) fn run(address: &str, log_mode: LogMode) -> io::Result<()> {
    match log_mode {
        LogMode::Enabled => run_with_logger(address, true, |stream| {
            EnabledLogger::new(
                client_identity(stream.peer_addr(), stream.local_addr()),
                io::stderr(),
            )
        }),
        LogMode::Disabled => run_with_logger(address, false, |_stream| DisabledLogger),
    }
}

fn run_with_logger<L, F>(address: &str, announce: bool, logger_factory: F) -> io::Result<()>
where
    L: EventLogger,
    F: FnMut(&mio::net::TcpStream) -> L,
{
    let mut event_loop = EventLoop::bind(address, logger_factory, MAX_INCOMPLETE_BUFFER)?;
    if announce {
        eprintln!("[redis] listening address={}", event_loop.local_addr()?);
    }
    event_loop.run()
}

#[cfg(test)]
pub(super) fn run_test_listener(
    listener: TcpListener,
    log_mode: LogMode,
    expected_connections: usize,
    buffer_limit: usize,
    deadline: Instant,
) -> io::Result<()> {
    match log_mode {
        LogMode::Enabled => run_test_with_logger(
            listener,
            expected_connections,
            buffer_limit,
            deadline,
            |stream| {
                EnabledLogger::new(
                    client_identity(stream.peer_addr(), stream.local_addr()),
                    io::stderr(),
                )
            },
        ),
        LogMode::Disabled => run_test_with_logger(
            listener,
            expected_connections,
            buffer_limit,
            deadline,
            |_stream| DisabledLogger,
        ),
    }
}

#[cfg(test)]
fn run_test_with_logger<L, F>(
    listener: TcpListener,
    expected_connections: usize,
    buffer_limit: usize,
    deadline: Instant,
    logger_factory: F,
) -> io::Result<()>
where
    L: EventLogger,
    F: FnMut(&mio::net::TcpStream) -> L,
{
    let mut event_loop = EventLoop::from_listener(listener, logger_factory, buffer_limit)?;
    loop {
        if event_loop.accepted_connections > expected_connections {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "test server accepted more connections than declared",
            ));
        }
        if event_loop.accepted_connections == expected_connections
            && event_loop.connections.is_empty()
        {
            return Ok(());
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "test event loop deadline elapsed",
            ));
        }
        event_loop.drive_once(Some(
            deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(50)),
        ))?;
    }
}

enum WorkItem {
    Listener,
    Connection(Token),
}

#[derive(Clone, Copy)]
struct ReadyEvent {
    token: Token,
    readable: bool,
    writable: bool,
}

struct EventLoop<L, F> {
    poll: Poll,
    events: Events,
    ready_events: Vec<ReadyEvent>,
    listener: mio::net::TcpListener,
    connections: HashMap<Token, Connection<mio::net::TcpStream, L>>,
    continuations: VecDeque<WorkItem>,
    listener_queued: bool,
    next_token: usize,
    database: Database,
    decoder: RequestDecoder,
    encoder: Encoder,
    buffer_limit: usize,
    logger_factory: F,
    #[cfg(test)]
    accepted_connections: usize,
}

impl<L, F> EventLoop<L, F>
where
    L: EventLogger,
    F: FnMut(&mio::net::TcpStream) -> L,
{
    fn bind(address: &str, logger_factory: F, buffer_limit: usize) -> io::Result<Self> {
        let standard_listener = std::net::TcpListener::bind(address)?;
        Self::from_listener(standard_listener, logger_factory, buffer_limit)
    }

    fn from_listener(
        standard_listener: TcpListener,
        logger_factory: F,
        buffer_limit: usize,
    ) -> io::Result<Self> {
        let poll = Poll::new()?;
        standard_listener.set_nonblocking(true)?;
        let mut listener = mio::net::TcpListener::from_std(standard_listener);
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;

        Ok(Self {
            poll,
            events: Events::with_capacity(EVENT_CAPACITY),
            ready_events: Vec::with_capacity(EVENT_CAPACITY),
            listener,
            connections: HashMap::new(),
            continuations: VecDeque::new(),
            listener_queued: false,
            next_token: 1,
            database: Database::default(),
            decoder: RequestDecoder::default(),
            encoder: Encoder::default(),
            buffer_limit,
            logger_factory,
            #[cfg(test)]
            accepted_connections: 0,
        })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    fn run(&mut self) -> io::Result<()> {
        loop {
            self.drive_once(None)?;
        }
    }

    fn drive_once(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let timeout = if self.continuations.is_empty() {
            timeout
        } else {
            Some(Duration::ZERO)
        };
        self.poll.poll(&mut self.events, timeout)?;
        self.copy_ready_events();

        self.service_ready_events()?;
        self.service_continuations()?;

        Ok(())
    }

    fn service_ready_events(&mut self) -> io::Result<()> {
        for index in 0..self.ready_events.len() {
            let ready = self.ready_events[index];
            if ready.token == LISTENER {
                self.accept_ready()?;
            } else {
                self.service_connection(
                    ready.token,
                    Readiness {
                        readable: ready.readable,
                        writable: ready.writable,
                    },
                );
            }
        }
        Ok(())
    }

    fn service_continuations(&mut self) -> io::Result<()> {
        for _ in 0..MAX_CONTINUATIONS_PER_POLL {
            let Some(work) = self.continuations.pop_front() else {
                break;
            };
            match work {
                WorkItem::Listener => {
                    self.listener_queued = false;
                    self.accept_ready()?;
                }
                WorkItem::Connection(token) => {
                    let Some(connection) = self.connections.get_mut(&token) else {
                        continue;
                    };
                    connection.clear_queued();
                    self.service_connection(
                        token,
                        Readiness {
                            readable: false,
                            writable: false,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn copy_ready_events(&mut self) {
        self.ready_events.clear();
        self.ready_events
            .extend(self.events.iter().map(ReadyEvent::from));
    }

    fn accept_ready(&mut self) -> io::Result<()> {
        let mut accepted = 0;
        while accepted < ACCEPT_BUDGET {
            let socket = match self.listener.accept() {
                Ok((socket, _peer)) => socket,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(error),
            };
            accepted += 1;
            #[cfg(test)]
            {
                self.accepted_connections = self
                    .accepted_connections
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("accepted connection count overflowed"))?;
            }

            let token = self.allocate_token()?;
            let logger = (self.logger_factory)(&socket);
            let mut connection = Connection::new(socket, logger, self.buffer_limit);
            let registration =
                self.poll
                    .registry()
                    .register(&mut connection.socket, token, Interest::READABLE);
            match registration {
                Ok(()) => {
                    connection.logger.connected();
                    self.connections.insert(token, connection);
                }
                Err(error) => {
                    let result = Err(error);
                    connection.logger.finished(&result);
                }
            }
        }

        self.enqueue_listener();
        Ok(())
    }

    fn allocate_token(&mut self) -> io::Result<Token> {
        let token = Token(self.next_token);
        self.next_token = self
            .next_token
            .checked_add(1)
            .ok_or_else(|| io::Error::other("connection token space exhausted"))?;
        Ok(token)
    }

    fn enqueue_listener(&mut self) {
        if !self.listener_queued {
            self.listener_queued = true;
            self.continuations.push_back(WorkItem::Listener);
        }
    }

    fn service_connection(&mut self, token: Token, readiness: Readiness) {
        let previous_interest = match self.connections.get(&token) {
            Some(connection) => connection.desired_interest(),
            None => return,
        };
        let service_result = {
            let connection = self
                .connections
                .get_mut(&token)
                .expect("connection existence checked above");
            connection.service(readiness, &self.database, &self.decoder, &self.encoder)
        };

        let result = match service_result {
            Ok(result) => result,
            Err(error) => {
                self.remove_connection(token, Err(error));
                return;
            }
        };

        if result.close {
            self.remove_connection(token, Ok(()));
            return;
        }

        if result.interest != previous_interest {
            let interest = match result.interest {
                Some(DesiredInterest::Readable) => Interest::READABLE,
                Some(DesiredInterest::Writable) => Interest::WRITABLE,
                None => {
                    self.remove_connection(
                        token,
                        Err(io::Error::other("open connection has no desired interest")),
                    );
                    return;
                }
            };
            let registration = {
                let connection = self
                    .connections
                    .get_mut(&token)
                    .expect("connection remains registered while serviced");
                self.poll
                    .registry()
                    .reregister(&mut connection.socket, token, interest)
            };
            if let Err(error) = registration {
                self.remove_connection(token, Err(error));
                return;
            }
        }

        if result.requeue {
            self.enqueue_connection(token);
        }
    }

    fn enqueue_connection(&mut self, token: Token) {
        let Some(connection) = self.connections.get_mut(&token) else {
            return;
        };
        if connection.mark_queued() {
            self.continuations.push_back(WorkItem::Connection(token));
        }
    }

    fn remove_connection(&mut self, token: Token, result: io::Result<()>) {
        let Some(mut connection) = self.connections.remove(&token) else {
            return;
        };
        let deregistration = self.poll.registry().deregister(&mut connection.socket);
        let result = match (result, deregistration) {
            (Err(error), _) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        };
        connection.logger.finished(&result);
    }
}

impl From<&Event> for ReadyEvent {
    fn from(event: &Event) -> Self {
        Self {
            token: event.token(),
            readable: event.is_readable() || event.is_read_closed(),
            writable: event.is_writable() || event.is_write_closed() || event.is_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ACCEPT_BUDGET, EventLoop, LISTENER, MAX_CONTINUATIONS_PER_POLL, MAX_INCOMPLETE_BUFFER,
        Readiness, ReadyEvent, Token, WorkItem,
    };
    use crate::logging::LogMode;
    use crate::server::connection_logging::{DisabledLogger, EventLogger};
    use std::cell::Cell;
    use std::io::{self, Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::rc::Rc;
    use std::time::Duration;

    struct CountingLogger {
        finished: Rc<Cell<usize>>,
    }

    impl EventLogger for CountingLogger {
        fn finished(&mut self, _result: &io::Result<()>) {
            self.finished.set(self.finished.get() + 1);
        }
    }

    fn event_loop() -> EventLoop<DisabledLogger, impl FnMut(&mio::net::TcpStream) -> DisabledLogger>
    {
        EventLoop::bind(
            "127.0.0.1:0",
            |_stream| DisabledLogger,
            MAX_INCOMPLETE_BUFFER,
        )
        .unwrap()
    }

    fn accept_connections<L, F>(event_loop: &mut EventLoop<L, F>, expected: usize)
    where
        L: EventLogger,
        F: FnMut(&mio::net::TcpStream) -> L,
    {
        for _ in 0..20 {
            event_loop
                .drive_once(Some(Duration::from_millis(50)))
                .unwrap();
            if event_loop.connections.len() == expected {
                return;
            }
        }
        panic!(
            "accepted {} connections, expected {expected}",
            event_loop.connections.len()
        );
    }

    fn token_for_client<L, F>(event_loop: &EventLoop<L, F>, client: &TcpStream) -> Token {
        let client_address = client.local_addr().unwrap();
        event_loop
            .connections
            .iter()
            .find_map(|(token, connection)| {
                (connection.socket.peer_addr().unwrap() == client_address).then_some(*token)
            })
            .unwrap()
    }

    #[test]
    fn binds_ephemeral_address_and_serves_ping() {
        let mut event_loop = event_loop();
        let mut client = TcpStream::connect(event_loop.local_addr().unwrap()).unwrap();
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        client.set_nonblocking(true).unwrap();

        let mut response = [0; 7];
        let mut received = 0;
        for _ in 0..20 {
            event_loop
                .drive_once(Some(Duration::from_millis(50)))
                .unwrap();
            match client.read(&mut response[received..]) {
                Ok(0) => panic!("server closed before returning PONG"),
                Ok(read) => {
                    received += read;
                    if received == response.len() {
                        break;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("failed to read PONG: {error}"),
            }
        }

        assert_eq!(&response[..received], b"+PONG\r\n");
    }

    #[test]
    fn accept_budget_requeues_listener_once() {
        let mut event_loop = event_loop();
        let address = event_loop.local_addr().unwrap();
        let clients = (0..ACCEPT_BUDGET + 1)
            .map(|_| TcpStream::connect(address).unwrap())
            .collect::<Vec<_>>();

        event_loop.accept_ready().unwrap();

        assert_eq!(event_loop.connections.len(), ACCEPT_BUDGET);
        assert!(event_loop.listener_queued);
        assert_eq!(event_loop.continuations.len(), 1);
        assert!(matches!(
            event_loop.continuations.front(),
            Some(WorkItem::Listener)
        ));

        event_loop.service_continuations().unwrap();
        assert_eq!(event_loop.connections.len(), ACCEPT_BUDGET + 1);
        assert!(!event_loop.listener_queued);
        assert!(event_loop.continuations.is_empty());

        drop(clients);
    }

    #[test]
    fn pipelined_client_continuation_does_not_starve_ready_ping() {
        let mut event_loop = event_loop();
        let address = event_loop.local_addr().unwrap();
        let mut pipelined = TcpStream::connect(address).unwrap();
        let mut ping = TcpStream::connect(address).unwrap();
        accept_connections(&mut event_loop, 2);

        let request = b"*1\r\n$4\r\nPING\r\n";
        pipelined.write_all(&request.repeat(65)).unwrap();
        ping.write_all(request).unwrap();
        pipelined
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        ping.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();

        let pipelined_token = token_for_client(&event_loop, &pipelined);
        let ping_token = token_for_client(&event_loop, &ping);
        let mut pipelined_ready = false;
        let mut ping_ready = false;
        for _ in 0..20 {
            event_loop
                .poll
                .poll(&mut event_loop.events, Some(Duration::from_millis(50)))
                .unwrap();
            for event in &event_loop.events {
                pipelined_ready |= event.token() == pipelined_token && event.is_readable();
                ping_ready |= event.token() == ping_token && event.is_readable();
            }
            if pipelined_ready && ping_ready {
                break;
            }
        }
        assert!(pipelined_ready && ping_ready);

        event_loop.ready_events.clear();
        event_loop.ready_events.extend([
            ReadyEvent {
                token: pipelined_token,
                readable: true,
                writable: false,
            },
            ReadyEvent {
                token: ping_token,
                readable: true,
                writable: false,
            },
        ]);
        event_loop.service_ready_events().unwrap();

        let mut ping_response = [0; 7];
        ping.read_exact(&mut ping_response).unwrap();
        assert_eq!(ping_response, *b"+PONG\r\n");

        let expected_pipeline = b"+PONG\r\n".repeat(65);
        event_loop.service_continuations().unwrap();
        let mut pipeline_response = vec![0; expected_pipeline.len()];
        pipelined.read_exact(&mut pipeline_response).unwrap();
        assert_eq!(pipeline_response, expected_pipeline);
    }

    #[test]
    fn duplicate_readiness_and_continuation_signals_enqueue_once() {
        let mut event_loop = event_loop();
        let address = event_loop.local_addr().unwrap();
        let mut client = TcpStream::connect(address).unwrap();
        accept_connections(&mut event_loop, 1);
        let token = *event_loop.connections.keys().next().unwrap();
        client
            .write_all(&b"*1\r\n$4\r\nPING\r\n".repeat(65))
            .unwrap();

        let readable = Readiness {
            readable: true,
            writable: false,
        };
        event_loop.service_connection(token, readable);
        event_loop.service_connection(token, readable);
        event_loop.enqueue_connection(token);

        assert_eq!(event_loop.continuations.len(), 1);
        assert!(matches!(
            event_loop.continuations.front(),
            Some(WorkItem::Connection(queued)) if *queued == token
        ));
    }

    #[test]
    fn token_exhaustion_is_exact_and_does_not_wrap() {
        let mut event_loop = event_loop();
        event_loop.next_token = usize::MAX;

        let error = event_loop.allocate_token().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "connection token space exhausted");
        assert_eq!(event_loop.next_token, usize::MAX);
        assert!(!event_loop.connections.contains_key(&LISTENER));
    }

    #[test]
    fn removed_token_is_not_reused_and_stale_event_is_ignored() {
        let mut event_loop = event_loop();
        let address = event_loop.local_addr().unwrap();
        let first_client = TcpStream::connect(address).unwrap();
        accept_connections(&mut event_loop, 1);
        let first_token = *event_loop.connections.keys().next().unwrap();
        event_loop.remove_connection(first_token, Ok(()));
        drop(first_client);

        let _second_client = TcpStream::connect(address).unwrap();
        accept_connections(&mut event_loop, 1);
        let second_token = *event_loop.connections.keys().next().unwrap();
        assert!(second_token.0 > first_token.0);
        let interest = event_loop.connections[&second_token].desired_interest();

        event_loop.ready_events.clear();
        event_loop.ready_events.push(ReadyEvent {
            token: first_token,
            readable: true,
            writable: true,
        });
        event_loop.service_ready_events().unwrap();

        assert_eq!(event_loop.connections.len(), 1);
        assert_eq!(
            event_loop.connections[&second_token].desired_interest(),
            interest
        );
    }

    #[test]
    fn removed_connection_finishes_logger_exactly_once() {
        let finished = Rc::new(Cell::new(0));
        let finished_for_factory = Rc::clone(&finished);
        let mut event_loop = EventLoop::bind(
            "127.0.0.1:0",
            move |_stream| CountingLogger {
                finished: Rc::clone(&finished_for_factory),
            },
            MAX_INCOMPLETE_BUFFER,
        )
        .unwrap();
        let client = TcpStream::connect(event_loop.local_addr().unwrap()).unwrap();
        accept_connections(&mut event_loop, 1);
        let token = *event_loop.connections.keys().next().unwrap();
        client.shutdown(Shutdown::Both).unwrap();
        drop(client);

        for _ in 0..20 {
            event_loop
                .drive_once(Some(Duration::from_millis(50)))
                .unwrap();
            if event_loop.connections.is_empty() {
                break;
            }
        }
        assert!(event_loop.connections.is_empty());
        assert_eq!(finished.get(), 1);

        event_loop.ready_events.push(ReadyEvent {
            token,
            readable: true,
            writable: true,
        });
        event_loop.service_ready_events().unwrap();
        assert_eq!(finished.get(), 1);
    }

    #[test]
    fn continuation_drain_is_fifo_and_bounded() {
        let mut event_loop = event_loop();
        event_loop.continuations.extend(
            (1..=MAX_CONTINUATIONS_PER_POLL + 1).map(|value| WorkItem::Connection(Token(value))),
        );

        event_loop.service_continuations().unwrap();

        assert_eq!(event_loop.continuations.len(), 1);
        assert!(matches!(
            event_loop.continuations.front(),
            Some(WorkItem::Connection(Token(value)))
                if *value == MAX_CONTINUATIONS_PER_POLL + 1
        ));
    }

    #[test]
    fn production_run_function_has_expected_signature() {
        let _run: fn(&str, LogMode) -> io::Result<()> = super::run;
    }
}
