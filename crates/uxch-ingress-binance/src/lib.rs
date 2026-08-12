//! Binance ingress
//!
//! Maintains a local order book synced from the Binance and mirrors it
//! downstream to the uxch engine.

pub mod depth;

use std::thread;
use std::time::Duration;
use tungstenite::{Message, connect};
use uxch_core::OrderMsg;

use depth::{ApplyOutcome, DepthSync, SnapshotFetcher, SnapshotOutcome};

pub fn run(
    symbol: &str,
    next_id: &mut u64,
    mut on_order: impl FnMut(OrderMsg),
) -> Result<(), Error> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let url = format!("wss://stream.binance.com:9443/ws/{symbol}@depth");
    let mut sync = DepthSync::new();

    loop {
        match relay_once(&url, symbol, next_id, &mut sync, &mut on_order) {
            Ok(()) => return Ok(()),
            Err(Error::Closed) | Err(Error::WebSocket(_)) | Err(Error::Http(_)) => {
                eprintln!("binance relay: disconnected; reconnecting in 1s");
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn relay_once(
    url: &str,
    symbol: &str,
    next_id: &mut u64,
    sync: &mut DepthSync,
    on_order: &mut dyn FnMut(OrderMsg),
) -> Result<(), Error> {
    // Cancel any stale book from a prior connection so the fresh snapshot
    // re-seeds from scratch (the downstream engine's ids are tracked by `sync`).
    for m in sync.resync() {
        on_order(m);
    }

    let (mut socket, _resp) = connect(url)?;
    let mut fetcher = SnapshotFetcher::new(symbol.to_string());
    fetcher.request();

    loop {
        let msg = socket.read()?;
        match msg {
            Message::Text(text) => {
                if let Some(diff) = depth::parse_depth(&text) {
                    match sync.apply_diff(diff, next_id) {
                        ApplyOutcome::Buffered => {}
                        ApplyOutcome::Applied(msgs) => {
                            for m in msgs {
                                on_order(m);
                            }
                        }
                        ApplyOutcome::Gap(cancels) => {
                            for m in cancels {
                                on_order(m);
                            }
                            fetcher.request();
                        }
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = socket.write(Message::Pong(payload));
            }
            Message::Close(_) => return Err(Error::Closed),
            // Binary (Binance doesn't send on /ws), Pong, raw Frame — ignore.
            _ => {}
        }

        // Apply a snapshot if the fetch has landed.
        if let Some(snap) = fetcher.poll() {
            match sync.load_snapshot(snap, next_id) {
                SnapshotOutcome::Synced(msgs) => {
                    for m in msgs {
                        on_order(m);
                    }
                }
                SnapshotOutcome::Gap => fetcher.request(), // snapshot stale; re-fetch
            }
        }

        // Keep a snapshot in flight while we're still buffering.
        if sync.is_buffering() && fetcher.needs_snapshot() {
            fetcher.request();
        }
    }
}

#[derive(Debug)]
pub enum Error {
    WebSocket(String),
    Http(String),
    Closed,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WebSocket(s) => write!(f, "websocket: {s}"),
            Self::Http(s) => write!(f, "http: {s}"),
            Self::Closed => write!(f, "connection closed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tungstenite::Error> for Error {
    fn from(e: tungstenite::Error) -> Self {
        Self::WebSocket(e.to_string())
    }
}
