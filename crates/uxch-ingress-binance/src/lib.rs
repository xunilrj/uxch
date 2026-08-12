//! Binance market-stream ingress — WebSocket → `OrderMsg`.
//!
//! Subscribes to the Binance `<symbol>@trade` stream and maps each trade to a
//! GTC limit [`OrderMsg`].
//!
//! TODO: Fixed-point conversion
//!
//! Binance prices/quantities are decimal strings (e.g. `"60000.50"`); the core
//! uses integer ticks (`Price = i64`, `Qty = u64`). Both are scaled by 10^8 to
//! fixed-point before building the order, so precision beyond 8 decimals is
//! truncated.

use std::thread;
use std::time::Duration;
use tungstenite::{Message, connect};
use uxch_core::{OrderMsg, Side};

/// Scale factor for decimal → fixed-point conversion (8 decimals).
const SCALE: f64 = 1e8;

pub fn run(symbol: &str, mut on_order: impl FnMut(OrderMsg)) -> Result<(), Error> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let url = format!("wss://stream.binance.com:9443/ws/{symbol}@trade");

    loop {
        match relay_once(&url, &mut on_order) {
            Ok(()) => return Ok(()),
            Err(Error::Closed) | Err(Error::WebSocket(_)) => {
                eprintln!("binance relay: disconnected; reconnecting in 1s");
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

/// One connection attempt: read until the socket closes or errors.
fn relay_once(url: &str, on_order: &mut dyn FnMut(OrderMsg)) -> Result<(), Error> {
    let (mut socket, _resp) = connect(url)?;
    let mut next_id: u64 = 1;

    loop {
        let msg = socket.read()?;
        match msg {
            Message::Text(text) => {
                if let Some(order) = parse_trade(&text, &mut next_id) {
                    on_order(order);
                }
            }
            Message::Ping(payload) => {
                let _ = socket.write(Message::Pong(payload));
            }
            Message::Close(_) => return Err(Error::Closed),
            // Binary (Binance doesn't send on /ws), Pong, raw Frame — ignore.
            _ => {}
        }
    }
}

/// Parse a Binance `@trade` frame into an [`OrderMsg`].
fn parse_trade(text: &str, next_id: &mut u64) -> Option<OrderMsg> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Only trade events carry `"e":"trade"`; skip anything else.
    if v.get("e").and_then(|e| e.as_str()) != Some("trade") {
        return None;
    }

    let price = v.get("p")?.as_str()?.parse::<f64>().ok()?;
    let qty = v.get("q")?.as_str()?.parse::<f64>().ok()?;
    let is_buyer_maker = v.get("m")?.as_bool()?;

    let side = if is_buyer_maker {
        Side::Sell
    } else {
        Side::Buy
    };
    let id = *next_id;
    *next_id += 1;

    Some(OrderMsg::new_limit(
        id,
        side,
        (price * SCALE).round() as i64,
        (qty * SCALE).round() as u64,
    ))
}

#[derive(Debug)]
pub enum Error {
    WebSocket(String),
    Closed,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WebSocket(s) => write!(f, "websocket: {s}"),
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
