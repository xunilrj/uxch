//! `uxch-web-server` — a tiny web UI + ingress/egress

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tungstenite::Message;
use uxch_core::{MarketEvent, MarketEventContent, OrderMsg, OrderType, Side, TimeInForce};

const INDEX_HTML: &str = include_str!("../static/index.html");

const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_millis(200);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// Shared app state: the ingress forwarder to uxch and the live WS clients.
struct State {
    ingress: Mutex<IngressForwarder>,
    ws_clients: Mutex<Vec<Sender<String>>>,
}

/// Ingress client to uxch: lazily (re)connects and writes `OrderMsg` NDJSON.
/// Mirrors `uxch-ingress-binance`'s `Forwarder`.
struct IngressForwarder {
    addr: SocketAddr,
    stream: Option<TcpStream>,
    next_attempt: Instant,
}

impl IngressForwarder {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
            next_attempt: Instant::now(),
        }
    }

    fn send(&mut self, order: &OrderMsg) {
        if self.stream.is_none() && Instant::now() >= self.next_attempt {
            match TcpStream::connect_timeout(&self.addr, CONNECT_TIMEOUT) {
                Ok(s) => {
                    let _ = s.set_write_timeout(Some(WRITE_TIMEOUT));
                    eprintln!("web-server: ingress connected to {}", self.addr);
                    self.stream = Some(s);
                }
                Err(e) => {
                    eprintln!("web-server: uxch unreachable ({e}); retrying in 1s");
                    self.next_attempt = Instant::now() + RECONNECT_INTERVAL;
                }
            }
        }
        let Some(s) = self.stream.as_mut() else {
            return; // uxch down: drop this order, keep serving the UI
        };
        let Ok(mut line) = serde_json::to_string(order) else {
            return;
        };
        line.push('\n');
        if s.write_all(line.as_bytes()).is_err() {
            eprintln!("web-server: ingress write failed; will reconnect");
            self.stream = None;
            self.next_attempt = Instant::now() + RECONNECT_INTERVAL;
        }
    }
}

fn main() {
    let ingress_addr: SocketAddr = std::env::var("UXCH_INGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9999".into())
        .parse()
        .expect("UXCH_INGRESS_ADDR must be ip:port");
    let egress_addr: SocketAddr = std::env::var("UXCH_EGRESS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9998".into())
        .parse()
        .expect("UXCH_EGRESS_ADDR must be ip:port");
    let web_addr: SocketAddr = std::env::var("UXCH_WEB_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()
        .expect("UXCH_WEB_ADDR must be ip:port");

    let state = Arc::new(State {
        ingress: Mutex::new(IngressForwarder::new(ingress_addr)),
        ws_clients: Mutex::new(Vec::new()),
    });

    // Egress listener: uxch dials us and streams MarketEvent NDJSON. Every BBO
    // is broadcast to all registered WS clients (dead senders are reaped).
    {
        let state = state.clone();
        thread::spawn(move || egress_listener(egress_addr, state));
    }

    let listener = TcpListener::bind(web_addr).unwrap_or_else(|e| {
        eprintln!("web-server: bind {web_addr} failed: {e}");
        std::process::exit(1);
    });
    eprintln!("web-server: serving UI on http://{web_addr} (ws at /ws)");

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let state = state.clone();
        thread::spawn(move || handle_conn(stream, state));
    }
}

/// Accept uxch's egress connection and broadcast BBO updates to WS clients.
fn egress_listener(addr: SocketAddr, state: Arc<State>) {
    let listener = match TcpListener::bind(addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("web-server: bind egress {addr} failed: {e}");
            return;
        }
    };
    eprintln!("web-server: egress listening on {addr} (uxch connects here)");

    // uxch dials us; accept in a loop so a reconnecting uxch is picked up.
    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                eprintln!("web-server: egress connected from {peer}");
                let state = state.clone();
                thread::spawn(move || egress_relay(stream, state));
            }
            Err(e) => eprintln!("web-server: egress accept failed: {e}"),
        }
    }
}

fn egress_relay(stream: TcpStream, state: Arc<State>) {
    for line in BufReader::new(stream).lines() {
        let Ok(line) = line else { break };
        if line.is_empty() {
            continue;
        }
        let Ok(ev) = serde_json::from_str::<MarketEvent>(&line) else {
            continue;
        };
        if let MarketEventContent::Bbo(bbo) = ev.event {
            let Ok(msg) = serde_json::to_string(&bbo) else {
                continue;
            };
            // Broadcast, dropping any client whose channel is closed.
            let mut clients = state.ws_clients.lock().unwrap();
            clients.retain(|tx| tx.send(msg.clone()).is_ok());
        }
    }
    eprintln!("web-server: egress disconnected; waiting for reconnect");
}

/// Route a fresh connection by peeking the first request line, without
/// consuming bytes, so a WebSocket upgrade can still be handed to tungstenite.
enum Route {
    Ws,
    Http,
    Closed,
}

fn route(stream: &TcpStream) -> Route {
    let mut buf = [0u8; 512];
    for _ in 0..100 {
        let n = stream.peek(&mut buf).unwrap_or(0);
        if n == 0 {
            return Route::Closed;
        }
        if let Ok(s) = std::str::from_utf8(&buf[..n])
            && let Some(end) = s.find("\r\n")
        {
            return if s[..end].starts_with("GET /ws ") {
                Route::Ws
            } else {
                Route::Http
            };
        }
        thread::sleep(Duration::from_millis(2));
    }
    Route::Http
}

fn handle_conn(stream: TcpStream, state: Arc<State>) {
    match route(&stream) {
        Route::Ws => handle_ws(stream, state),
        Route::Http => handle_http(stream, state),
        Route::Closed => {}
    }
}

/// Complete the WS handshake, register the client, and push BBO JSON to it.
/// Liveness is checked with a periodic Ping; a failed send drops the client
/// (the egress relay reaps its channel on the next broadcast).
fn handle_ws(stream: TcpStream, state: Arc<State>) {
    let mut ws = match tungstenite::accept(stream) {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("web-server: ws handshake failed: {e}");
            return;
        }
    };
    let (tx, rx) = mpsc::channel::<String>();
    let n = {
        let mut clients = state.ws_clients.lock().unwrap();
        clients.push(tx);
        clients.len()
    };
    eprintln!("web-server: ws client connected ({n} total)");

    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(msg) => {
                if ws.send(Message::Text(msg.into())).is_err() {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if ws.send(Message::Ping(vec![].into())).is_err() {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    eprintln!("web-server: ws client disconnected");
}

/// Handle a plain HTTP request: `GET /` (HTML), `POST /order`, `POST /cancel`.
fn handle_http(stream: TcpStream, state: Arc<State>) {
    let mut reader = BufReader::new(stream);

    // Request line.
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).is_err() {
        return;
    }
    let req_line = String::from_utf8_lossy(&line);
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    // Headers (only Content-Length matters here).
    let mut content_length: usize = 0;
    loop {
        let mut h = Vec::new();
        if reader.read_until(b'\n', &mut h).is_err() {
            return;
        }
        let s = String::from_utf8_lossy(&h);
        if s.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = s.split_once(':')
            && k.trim().eq_ignore_ascii_case("content-length")
        {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }

    // Body.
    let mut body = vec![0u8; content_length];
    if content_length > 0 && reader.read_exact(&mut body).is_err() {
        return;
    }
    let body = String::from_utf8_lossy(&body);

    let resp: String = match (method, path) {
        ("GET", "/") => http_response("200 OK", "text/html; charset=utf-8", INDEX_HTML),
        ("POST", "/order") => post_response(handle_order(&body, &state)),
        ("POST", "/cancel") => post_response(handle_cancel(&body, &state)),
        _ => http_response("404 Not Found", "text/plain", "not found"),
    };

    let _ = reader.get_mut().write_all(resp.as_bytes());
}

/// HTTP response for a POST handler's `Result`: `{}` on success, an error JSON
/// on failure.
fn post_response(res: Result<(), String>) -> String {
    match res {
        Ok(()) => http_response("200 OK", "application/json", "{}"),
        Err(e) => {
            let body = format!(
                r#"{{"error":{}}}"#,
                serde_json::to_string(&e).unwrap_or_else(|_| "\"?\"".into())
            );
            http_response("400 Bad Request", "application/json", &body)
        }
    }
}

#[derive(Deserialize)]
struct OrderInput {
    kind: String,
    side: String,
    tif: String,
    #[serde(default)]
    price: Option<i64>,
    #[serde(default)]
    qty: u64,
    id: u64,
}

fn handle_order(body: &str, state: &State) -> Result<(), String> {
    let inp: OrderInput = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let side = match inp.side.as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        _ => return Err(format!("bad side: {}", inp.side)),
    };
    let tif = match inp.tif.as_str() {
        "gtc" => TimeInForce::Gtc,
        "ioc" => TimeInForce::Ioc,
        "fok" => TimeInForce::Fok,
        _ => return Err(format!("bad tif: {}", inp.tif)),
    };
    let msg = match inp.kind.as_str() {
        "limit" => {
            let price = inp
                .price
                .ok_or_else(|| "limit order requires a price".to_string())?;
            OrderMsg::New {
                id: inp.id,
                side,
                ty: OrderType::Limit { price },
                tif,
                qty: inp.qty,
            }
        }
        "market" => OrderMsg::New {
            id: inp.id,
            side,
            ty: OrderType::Market,
            tif,
            qty: inp.qty,
        },
        _ => return Err(format!("bad kind: {}", inp.kind)),
    };
    eprintln!("web-server: order {msg:?}");
    state.ingress.lock().unwrap().send(&msg);
    Ok(())
}

#[derive(Deserialize)]
struct CancelInput {
    id: u64,
}

fn handle_cancel(body: &str, state: &State) -> Result<(), String> {
    let inp: CancelInput = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let msg = OrderMsg::Cancel { id: inp.id };
    eprintln!("web-server: cancel id={}", inp.id);
    state.ingress.lock().unwrap().send(&msg);
    Ok(())
}

fn http_response(status: &str, ctype: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
