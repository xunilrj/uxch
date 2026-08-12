//! `cargo run -p uxch` — the matching engine.
//!
//! 1. Listens on `$UXCH_INGRESS_ADDR` (default `127.0.0.1:9999`) for `OrderMsg`
//!    NDJSON from an ingress (e.g. `uxch-ingress-binance`).
//! 2. Forwards every `MarketEvent` as NDJSON over a TCP connection to the egress
//!    sink (`$UXCH_EGRESS_ADDR`, default `127.0.0.1:9998`, e.g.
//!    `uxch-egress-terminal`).

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use uxch::core::{MarketEvent, MatchingEngine, OrderMsg, bounded};

const CHANNEL_CAP: usize = 65_536;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_millis(200);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);
const ENSURE_CONNECT_ATTEMPTS: u32 = 150;

fn main() {
    let ingress_addr: SocketAddr = std::env::var("UXCH_INGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9999".into())
        .parse()
        .expect("UXCH_INGRESS_ADDR must be ip:port");
    let egress_addr: SocketAddr = std::env::var("UXCH_EGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9998".into())
        .parse()
        .expect("UXCH_EGRESS_ADDR must be ip:port");

    let (ingress_tx, ingress_rx) = bounded::bounded::<OrderMsg>(CHANNEL_CAP);
    let (egress_tx, egress_rx) = bounded::bounded::<MarketEvent>(CHANNEL_CAP);

    thread::spawn(move || egress_forward(egress_rx, egress_addr));

    let mut engine = MatchingEngine::new(ingress_rx, egress_tx);

    let listener = TcpListener::bind(ingress_addr).unwrap_or_else(|e| {
        eprintln!("uxch: bind {ingress_addr} failed: {e}");
        std::process::exit(1);
    });
    println!("uxch: listening on {ingress_addr} for ingress OrderMsg (NDJSON)");

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("uxch: accept failed: {e}");
                continue;
            }
        };
        let peer = stream
            .peer_addr()
            .map(|p| p.to_string())
            .unwrap_or_else(|_| "?".into());
        println!("uxch: ingress connected from {peer}");

        for line in BufReader::new(stream).lines() {
            match line {
                Ok(l) if l.is_empty() => continue,
                Ok(l) => match serde_json::from_str::<OrderMsg>(&l) {
                    Ok(msg) => {
                        if ingress_tx.send(msg).is_err() {
                            eprintln!("uxch: ingress channel closed; stopping");
                            return;
                        }
                        engine.run();
                    }
                    Err(e) => eprintln!("uxch: decode error: {e} | {l}"),
                },
                Err(e) => {
                    eprintln!("uxch: ingress read: {e}");
                    break;
                }
            }
        }

        println!("uxch: ingress disconnected; waiting for reconnect");
    }
}

fn egress_forward(egress_rx: uxch::core::bounded::Receiver<MarketEvent>, addr: SocketAddr) {
    let mut stream: Option<TcpStream> = None;
    let mut next_attempt = Instant::now();

    // Block-retry at startup so a terminal that's a moment behind still
    // receives the first events.
    for _ in 0..ENSURE_CONNECT_ATTEMPTS {
        if stream.is_some() {
            break;
        }
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(s) => {
                let _ = s.set_write_timeout(Some(WRITE_TIMEOUT));
                stream = Some(s);
                eprintln!("uxch: connected to egress {addr}");
            }
            Err(e) => {
                eprintln!(
                    "uxch: egress {addr} unreachable ({e}); retrying in 1s (waiting for terminal)"
                );
                std::thread::sleep(RECONNECT_INTERVAL);
            }
        }
    }

    if stream.is_none() {
        eprintln!(
            "uxch: egress {addr} still unreachable after {}s; going live (events dropped until it connects)",
            ENSURE_CONNECT_ATTEMPTS
        );
    }

    loop {
        let Some(ev) = egress_rx.recv() else {
            eprintln!("uxch: egress channel closed; forwarder exiting");
            return;
        };

        // Lazily (re)connect, at most once per RECONNECT_INTERVAL.
        if stream.is_none() && Instant::now() >= next_attempt {
            match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                Ok(s) => {
                    let _ = s.set_write_timeout(Some(WRITE_TIMEOUT));
                    eprintln!("uxch: connected to egress {addr}");
                    stream = Some(s);
                }
                Err(e) => {
                    eprintln!("uxch: egress {addr} unreachable ({e}); retrying in 1s");
                    next_attempt = Instant::now() + RECONNECT_INTERVAL;
                }
            }
        }

        let Some(s) = stream.as_mut() else {
            continue; // terminal down: drop this event, keep the engine draining
        };

        let Ok(mut line) = serde_json::to_string(&ev) else {
            continue;
        };
        line.push('\n');

        if s.write_all(line.as_bytes()).is_err() {
            eprintln!("uxch: egress write failed; will reconnect");
            stream = None;
            next_attempt = Instant::now() + RECONNECT_INTERVAL;
        }
    }
}
