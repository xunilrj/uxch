//! `cargo run -p uxch-ingress-binance [-- <symbol>]` — default symbol `bnbusdt`.
//!

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};
use uxch_core::OrderMsg;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_millis(200);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);
const ENSURE_CONNECT_ATTEMPTS: u32 = 30;

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

    fn ensure_connected(&mut self) {
        for _ in 0..ENSURE_CONNECT_ATTEMPTS {
            if self.stream.is_some() {
                return;
            }
            match TcpStream::connect_timeout(&self.addr, CONNECT_TIMEOUT) {
                Ok(s) => {
                    let _ = s.set_write_timeout(Some(WRITE_TIMEOUT));
                    self.stream = Some(s);
                    eprintln!("binance relay: connected to {}", self.addr);
                    return;
                }
                Err(e) => {
                    eprintln!(
                        "binance relay: uxch unreachable ({e}); retrying in 1s (waiting for sink)"
                    );
                    std::thread::sleep(RECONNECT_INTERVAL);
                }
            }
        }
        eprintln!(
            "binance relay: uxch still unreachable after {}s; skipping snapshot seed",
            ENSURE_CONNECT_ATTEMPTS
        );
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
    // Make ring the process-default rustls provider before any TLS use (snapshot
    // fetch + tungstenite both pick this up).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "bnbusdt".to_string());
    let addr: SocketAddr = std::env::var("UXCH_INGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9999".into())
        .parse()
        .expect("UXCH_INGRESS_ADDR must be ip:port");
    println!("uxch-ingress-binance: sync {symbol}@depth → {addr} (Ctrl-C to stop)");

    let mut next_id: u64 = 1;
    let mut forwarder = Forwarder::new(addr);

    forwarder.ensure_connected();

    if let Err(e) = uxch_ingress_binance::run(&symbol, &mut next_id, |msg| forwarder.send(&msg)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
