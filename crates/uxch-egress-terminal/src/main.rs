//! `cargo run -p uxch-egress-terminal [--save <file>] [--record <file>]`
//!
//! Listens on `$UXCH_EGRESS_ADDR` (default `127.0.0.1:9998`). The value may be
//! a comma-separated list of addresses; the terminal opens all of them, and
//! the list order is the failover preference (first = primary).
//!
//! `--save <file>`   — raw NDJSON of the *active* source's `MarketEvent`s.
//! `--record <file>` — *every* source's `MarketEvent`s, tagged
//!                    `PORT <port>: <ndjson>`, for offline convergence diffing.
//!
//! `UXCH_FAILOVER_STALL_SECS` (default 3) — seconds with no events from the
//! active source before failing over to a fresh standby.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    let addrs = parse_addrs();
    let save = parse_flag("--save");
    let record = parse_flag("--record");
    let stall_secs: u64 = std::env::var("UXCH_FAILOVER_STALL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let extra = match (&save, &record) {
        (Some(s), Some(r)) => format!(" — save {} · record {}", s.display(), r.display()),
        (Some(s), None) => format!(" — save {}", s.display()),
        (None, Some(r)) => format!(" — record {}", r.display()),
        (None, None) => String::new(),
    };
    println!(
        "uxch-egress-terminal: binding {} address(es){} (Ctrl-C to stop)",
        addrs.len(),
        extra
    );

    if let Err(e) =
        uxch_egress_terminal::serve(addrs, save, record, Duration::from_secs(stall_secs))
    {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

fn parse_addrs() -> Vec<SocketAddr> {
    let raw = std::env::var("UXCH_EGRESS_ADDR").unwrap_or_else(|_| "127.0.0.1:9998".into());
    let addrs: Vec<SocketAddr> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse()
                .unwrap_or_else(|_| panic!("UXCH_EGRESS_ADDR: bad entry {s:?} (expected ip:port)"))
        })
        .collect();
    if addrs.is_empty() {
        eprintln!("uxch-egress-terminal: UXCH_EGRESS_ADDR has no addresses");
        std::process::exit(2);
    }
    addrs
}

/// Parse `--flag <val>` or `--flag=<val>` from argv. Returns the path, if present.
fn parse_flag(flag: &str) -> Option<PathBuf> {
    let mut it = std::env::args().skip(1);
    let prefix = format!("{flag}=");
    while let Some(arg) = it.next() {
        if arg == flag {
            match it.next() {
                Some(v) if !v.is_empty() => return Some(PathBuf::from(v)),
                _ => {
                    eprintln!("uxch-egress-terminal: {flag} needs a file");
                    std::process::exit(2);
                }
            }
        }
        if let Some(rest) = arg.strip_prefix(&prefix) {
            if rest.is_empty() {
                eprintln!("uxch-egress-terminal: {flag} needs a file");
                std::process::exit(2);
            }
            return Some(PathBuf::from(rest));
        }
    }
    None
}
