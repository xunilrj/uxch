//! `cargo run -p uxch-egress-terminal` — display uxch egress.
//!
//! Listens on `$UXCH_EGRESS_ADDR` (default `127.0.0.1:9998`).

use std::net::SocketAddr;

fn main() {
    let addr: SocketAddr = std::env::var("UXCH_EGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9998".into())
        .parse()
        .expect("UXCH_EGRESS_ADDR must be ip:port");
    println!("uxch-egress-terminal: binding {addr} (Ctrl-C to stop)");

    if let Err(e) = uxch_egress_terminal::serve(addr) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
