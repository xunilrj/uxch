//! `cargo run -p uxch-egress-terminal [--save <file>]` — display uxch egress.
//!
//! Listens on `$UXCH_EGRESS_ADDR` (default `127.0.0.1:9998`).
//!
//! `--save <file>` also writes every received `MarketEvent` as NDJSON to `<file>`
//! (one JSON object per line), the diff target for convergence checks.

use std::net::SocketAddr;
use std::path::PathBuf;

fn main() {
    let (addr, save) = parse_args();

    println!(
        "uxch-egress-terminal: binding {addr}{} (Ctrl-C to stop)",
        save.as_ref()
            .map(|p| format!(" — saving events to {}", p.display()))
            .unwrap_or_default()
    );

    if let Err(e) = uxch_egress_terminal::serve(addr, save) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

fn parse_args() -> (SocketAddr, Option<PathBuf>) {
    let mut save: Option<PathBuf> = None;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg, None),
        };
        match flag.as_str() {
            "--save" => {
                let f = inline.or_else(|| it.next()).unwrap_or_else(String::new);
                if f.is_empty() {
                    eprintln!("uxch-egress-terminal: --save needs a file");
                    std::process::exit(2);
                }
                save = Some(PathBuf::from(f));
            }
            "--help" | "-h" => {
                println!(
                    "uxch-egress-terminal — display uxch egress.\n\
                     \nUsage:\n  \
                     uxch-egress-terminal [--save <file>]\n\
                     \nEnv:\n  \
                     UXCH_EGRESS_ADDR (default 127.0.0.1:9998) — listen addr.\n"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("uxch-egress-terminal: unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }

    let addr: SocketAddr = std::env::var("UXCH_EGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9998".into())
        .parse()
        .expect("UXCH_EGRESS_ADDR must be ip:port");
    (addr, save)
}
