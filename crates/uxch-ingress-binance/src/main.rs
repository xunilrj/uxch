//! `cargo run -p uxch-ingress-binance [-- <symbol>]` — default symbol `bnbusdt`.
//!
//! Reads the Binance `@trade` stream and forwards each mapped `OrderMsg` to the
//! `uxch` TCP listener (NDJSON, one JSON object per line) at `$UXCH_INGRESS_ADDR`
//! (default `127.0.0.1:9999`).

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};
use uxch_core::OrderMsg;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_millis(200);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

struct Forwarder {
    addr: SocketAddr,
    stream: Option<TcpStream>,
    next_attempt: Instant,
}

impl Forwarder {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
            next_attempt: Instant::now(),
        }
    }

    fn send(&mut self, order: &OrderMsg) {
        // Lazily (re)connect, at most once per RECONNECT_INTERVAL, so a down
        // channel never blocks the Binance read loop for long.
        if self.stream.is_none() && Instant::now() >= self.next_attempt {
            match TcpStream::connect_timeout(&self.addr, CONNECT_TIMEOUT) {
                Ok(s) => {
                    let _ = s.set_write_timeout(Some(WRITE_TIMEOUT));
                    self.stream = Some(s);
                    eprintln!("binance relay: connected to {}", self.addr);
                }
                Err(e) => {
                    eprintln!("binance relay: uxch unreachable ({e}); retrying in 1s");
                    self.next_attempt = Instant::now() + RECONNECT_INTERVAL;
                }
            }
        }

        let Some(s) = self.stream.as_mut() else {
            return; // channel down: drop this order, keep reading Binance
        };

        let Ok(mut line) = serde_json::to_string(order) else {
            return;
        };
        line.push('\n');

        if s.write_all(line.as_bytes()).is_err() {
            eprintln!("binance relay: write failed; will reconnect");
            self.stream = None;
            self.next_attempt = Instant::now() + RECONNECT_INTERVAL;
        }
    }
}

fn main() {
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "bnbusdt".to_string());
    let addr: SocketAddr = std::env::var("UXCH_INGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9999".into())
        .parse()
        .expect("UXCH_INGRESS_ADDR must be ip:port");
    println!("uxch-ingress-binance: {symbol}@trade → {addr} (Ctrl-C to stop)");

    let mut forwarder = Forwarder::new(addr);
    if let Err(e) = uxch_ingress_binance::run(&symbol, |msg| forwarder.send(&msg)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
