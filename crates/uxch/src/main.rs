//! `cargo run -p uxch`
//! Binds to `$UXCH_INGRESS_ADDR` (default `127.0.0.1:9999`).

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use uxch::core::OrderMsg;

fn main() {
    let addr = std::env::var("UXCH_INGRESS_ADDR").unwrap_or_else(|_| "127.0.0.1:9999".into());
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("uxch: bind {addr} failed: {e}");
        std::process::exit(1);
    });
    println!("uxch: listening on {addr} for ingress OrderMsg (NDJSON)");

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
                    Ok(msg) => println!("{msg:?}"),
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
