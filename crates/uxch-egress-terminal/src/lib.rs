//! Terminal display for uxch egress.
//!
//! Listens on a TCP socket (default `127.0.0.1:9998`).

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;

use uxch_core::egress::RejectReason;
use uxch_core::{Bbo, MarketEvent, Trade};

/// Fixed-point scale used across uxch (8 decimals).
const SCALE: u64 = 100_000_000;

pub fn serve(addr: SocketAddr, save: Option<PathBuf>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr)?;
    println!("uxch-egress-terminal: listening on {addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let peer = s
                    .peer_addr()
                    .map(|p| p.to_string())
                    .unwrap_or_else(|_| "?".into());
                println!("uxch-egress-terminal: uxch connected from {peer}");
                if let Err(e) = relay(s, save.as_ref()) {
                    eprintln!("uxch-egress-terminal: {e}");
                }
                println!("uxch-egress-terminal: uxch disconnected; waiting for reconnect");
            }
            Err(e) => eprintln!("uxch-egress-terminal: accept failed: {e}"),
        }
    }
    Ok(())
}

fn relay(stream: TcpStream, save: Option<&PathBuf>) -> Result<(), std::io::Error> {
    let mut tape = match save {
        Some(p) => Some(BufWriter::new(File::create(p)?)),
        None => None,
    };

    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let l = line?;
        if l.is_empty() {
            continue;
        }
        match serde_json::from_str::<MarketEvent>(&l) {
            Ok(ev) => println!("{}", render(&ev)),
            Err(e) => eprintln!("uxch-egress-terminal: decode error: {e} | {l}"),
        }
        if let Some(out) = tape.as_mut() {
            out.write_all(l.as_bytes())?;
            out.write_all(b"\n")?;
            out.flush()?;
        }
    }
    Ok(())
}

pub fn render(ev: &MarketEvent) -> String {
    match ev {
        MarketEvent::Trade(t) => render_trade(t),
        MarketEvent::Bbo(b) => render_bbo(b),
        MarketEvent::Reject { id, reason } => {
            format!("reject id={id} reason={}", render_reason(reason))
        }
    }
}

fn render_trade(t: &Trade) -> String {
    format!(
        "trade  price={}  qty={}  maker={}  taker={}",
        fmt_price(t.price),
        fmt_qty(t.qty),
        t.maker,
        t.taker
    )
}

fn render_bbo(b: &Bbo) -> String {
    let bid = b
        .bid
        .map(|(p, q)| format!("{}x{}", fmt_price(p), fmt_qty(q)))
        .unwrap_or_else(|| "-".into());
    let ask = b
        .ask
        .map(|(p, q)| format!("{}x{}", fmt_price(p), fmt_qty(q)))
        .unwrap_or_else(|| "-".into());
    format!("bbo    bid={bid}  ask={ask}")
}

fn render_reason(r: &RejectReason) -> &'static str {
    match r {
        RejectReason::FokNotFullyFillable => "fok-not-fully-fillable",
    }
}

/// Unscaled fixed-point price (i64 / 10^8) with 8 decimal places.
fn fmt_price(p: i64) -> String {
    let neg = p < 0;
    let abs = p.unsigned_abs();
    let int = abs / SCALE;
    let frac = abs % SCALE;
    if neg {
        format!("-{int}.{frac:08}")
    } else {
        format!("{int}.{frac:08}")
    }
}

/// Unscaled fixed-point quantity (u64 / 10^8) with 8 decimal places.
fn fmt_qty(q: u64) -> String {
    let int = q / SCALE;
    let frac = q % SCALE;
    format!("{int}.{frac:08}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uxch_core::egress::RejectReason;
    use uxch_core::{Bbo, MarketEvent, Trade};

    #[test]
    fn fmt_price_and_qty_unscale_with_8_decimals() {
        assert_eq!(fmt_price(6_000_010_000_000), "60000.10000000");
        assert_eq!(fmt_price(0), "0.00000000");
        assert_eq!(fmt_qty(125_000_000), "1.25000000");
        assert_eq!(fmt_qty(0), "0.00000000");
    }

    #[test]
    fn render_trade_lines_up_fields() {
        let t = Trade {
            price: 6_000_010_000_000,
            qty: 125_000_000,
            maker: 7,
            taker: 8,
        };
        assert_eq!(
            render(&MarketEvent::Trade(t)),
            "trade  price=60000.10000000  qty=1.25000000  maker=7  taker=8"
        );
    }

    #[test]
    fn render_bbo_handles_missing_sides() {
        let full = Bbo {
            bid: Some((6_000_010_000_000, 125_000_000)),
            ask: Some((6_000_050_000_000, 50_000_000)),
        };
        assert_eq!(
            render(&MarketEvent::Bbo(full)),
            "bbo    bid=60000.10000000x1.25000000  ask=60000.50000000x0.50000000"
        );
        let empty = Bbo {
            bid: None,
            ask: None,
        };
        assert_eq!(render(&MarketEvent::Bbo(empty)), "bbo    bid=-  ask=-");
    }

    #[test]
    fn render_reject_uses_kebab_reason() {
        let ev = MarketEvent::Reject {
            id: 42,
            reason: RejectReason::FokNotFullyFillable,
        };
        assert_eq!(render(&ev), "reject id=42 reason=fok-not-fully-fillable");
    }

    #[test]
    fn market_event_round_trips_through_serde() {
        let ev = MarketEvent::Bbo(Bbo {
            bid: Some((100, 5)),
            ask: None,
        });
        let s = serde_json::to_string(&ev).unwrap();
        assert!(s.starts_with(r#"{"bbo":"#));
        let back: MarketEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }
}
