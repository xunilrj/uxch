//! `uxch-ingress-binance` — sync a Binance order book and forward the derived
//! `OrderMsg` stream downstream.
//!
//! Modes:
//!   `uxch-ingress-binance [<symbol>]`
//!       Live (default): forward to `OrderMsg` over TCP.
//!   `uxch-ingress-binance --record <file> [<symbol>]`
//!       Record: write each `OrderMsg` as NDJSON to `<file>`.
//!   `uxch-ingress-binance --replay <file> [<symbol>]`
//!       Replay: read `<file>` and forward each `OrderMsg` to uxch over TCP.
//!       `--pace <microsecs>` sleeps that long between forwarded orders (replay
//!       only) so the egress sink can keep up — handy for deterministic tests.
//!
//! `<symbol>` defaults to `bnbusdt`. `--record`/`--replay` may be given as
//! `--record <file>` or `--record=<file>`, and are mutually exclusive.

use std::fs::File;
use std::io::{BufWriter, Write};
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

struct Tape {
    out: BufWriter<File>,
}

impl Tape {
    fn open(path: &str) -> std::io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            out: BufWriter::new(file),
        })
    }

    fn write(&mut self, order: &OrderMsg) {
        let Ok(mut line) = serde_json::to_string(order) else {
            return;
        };
        line.push('\n');
        // Per-line flush makes the tape kill-safe: a SIGTERM/SIGINT between
        // lines leaves a complete, parseable file.
        if self.out.write_all(line.as_bytes()).is_err() {
            eprintln!("binance tape: write failed");
        }
        let _ = self.out.flush();
    }
}

enum Mode {
    Live,
    Record(String),
    Replay(String),
}

struct Args {
    mode: Mode,
    symbol: String,
    pace_us: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut symbol = String::new();
    let mut record: Option<String> = None;
    let mut replay: Option<String> = None;
    let mut pace_us: u64 = 0;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        // Accept both `--record <file>` and `--record=<file>` (same for replay).
        let (flag, inline) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        match flag.as_str() {
            "--record" => {
                let f = inline
                    .or_else(|| it.next())
                    .ok_or_else(|| "--record needs a file".to_string())?;
                if f.is_empty() {
                    return Err("--record needs a non-empty file".into());
                }
                record = Some(f);
            }
            "--replay" => {
                let f = inline
                    .or_else(|| it.next())
                    .ok_or_else(|| "--replay needs a file".to_string())?;
                if f.is_empty() {
                    return Err("--replay needs a non-empty file".into());
                }
                replay = Some(f);
            }
            "--pace" => {
                let v = inline
                    .or_else(|| it.next())
                    .ok_or_else(|| "--pace needs a number of microsecs".to_string())?;
                pace_us = v.parse().map_err(|_| {
                    format!("--pace needs a non-negative integer of microsecs, got {v:?}")
                })?;
            }
            "--help" | "-h" => {
                print!(
                    "uxch-ingress-binance — sync a Binance order book.\n\
                     \n\
                     Usage:\n  \
                     uxch-ingress-binance [<symbol>]                  live → uxch\n  \
                     uxch-ingress-binance --record <file> [<symbol>]  record → file\n  \
                     uxch-ingress-binance --replay <file> [<symbol>]  replay file → uxch\n  \
                     uxch-ingress-binance --replay <file> --pace <us>  paced replay\n\
                     \n\
                     Env: UXCH_INGRESS_ADDR (default 127.0.0.1:9999) — uxch ingress addr.\n\
                     Default symbol: bnbusdt. --pace <microsecs> sleeps between orders (replay).\n"
                );
                std::process::exit(0);
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown flag: {other}"));
            }
            _ => {
                if symbol.is_empty() {
                    symbol = arg;
                } else {
                    return Err(format!("unexpected positional: {arg}"));
                }
            }
        }
    }

    if record.is_some() && replay.is_some() {
        return Err("--record and --replay are mutually exclusive".into());
    }
    let mode = match (record, replay) {
        (Some(f), None) => Mode::Record(f),
        (None, Some(f)) => Mode::Replay(f),
        (None, None) => Mode::Live,
        // unreachable: exclusivity checked above
        (Some(_), Some(_)) => return Err("--record and --replay are mutually exclusive".into()),
    };
    let symbol = if symbol.is_empty() {
        "bnbusdt".to_string()
    } else {
        symbol
    };
    if pace_us > 0 && !matches!(mode, Mode::Replay(_)) {
        return Err("--pace is only meaningful with --replay".into());
    }
    Ok(Args {
        mode,
        symbol,
        pace_us,
    })
}

fn main() {
    // Make ring the process-default rustls provider before any TLS use (snapshot
    // fetch + tungstenite both pick this up).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("uxch-ingress-binance: {e}");
            std::process::exit(2);
        }
    };
    let symbol = args.symbol;

    let addr: SocketAddr = std::env::var("UXCH_INGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9999".into())
        .parse()
        .expect("UXCH_INGRESS_ADDR must be ip:port");

    match args.mode {
        Mode::Live => {
            println!("uxch-ingress-binance: live {symbol}@depth → {addr} (Ctrl-C to stop)");
            let mut next_id: u64 = 1;
            let mut forwarder = Forwarder::new(addr);
            forwarder.ensure_connected();
            if let Err(e) =
                uxch_ingress_binance::run(&symbol, &mut next_id, |msg| forwarder.send(&msg))
            {
                eprintln!("fatal: {e}");
                std::process::exit(1);
            }
        }
        Mode::Record(file) => {
            println!("uxch-ingress-binance: record {symbol}@depth → {file} (Ctrl-C to stop)");
            let mut tape = match Tape::open(&file) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("fatal: cannot open tape {file}: {e}");
                    std::process::exit(1);
                }
            };
            let mut next_id: u64 = 1;
            if let Err(e) = uxch_ingress_binance::run(&symbol, &mut next_id, |msg| tape.write(&msg))
            {
                eprintln!("fatal: {e}");
                std::process::exit(1);
            }
        }
        Mode::Replay(file) => {
            let pace = std::time::Duration::from_micros(args.pace_us);
            println!(
                "uxch-ingress-binance: replay {file} → {addr}{}",
                if pace.is_zero() {
                    String::new()
                } else {
                    format!(" (pace {}µs/order)", args.pace_us)
                }
            );
            let mut forwarder = Forwarder::new(addr);
            forwarder.ensure_connected();
            if let Err(e) = uxch_ingress_binance::replay(&file, pace, |msg| forwarder.send(&msg)) {
                eprintln!("fatal: {e}");
                std::process::exit(1);
            }
        }
    }
}
