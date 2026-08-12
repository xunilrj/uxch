//! Binance diff-depth → local order book → synthetic `OrderMsg` stream.

use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use uxch_core::{OrderId, OrderMsg, Price, Qty, Side};

use crate::Error;

const SCALE: f64 = 1e8;

pub const SNAPSHOT_LIMIT: u32 = 1000;

const API_HOST: &str = "api.binance.com";

/// Decimal-string price → fixed-point `Price` (×10^8, rounded).
fn parse_price(s: &str) -> Option<Price> {
    Some((s.parse::<f64>().ok()? * SCALE).round() as Price)
}

/// Decimal-string quantity → fixed-point `Qty` (×10^8, rounded).
fn parse_qty(s: &str) -> Option<Qty> {
    Some((s.parse::<f64>().ok()? * SCALE).round() as Qty)
}

#[inline]
fn take_id(next_id: &mut u64) -> u64 {
    let id = *next_id;
    *next_id += 1;
    id
}

/// A parsed `/api/v3/depth` snapshot.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub last_update_id: u64,
    pub bids: Vec<(Price, Qty)>,
    pub asks: Vec<(Price, Qty)>,
}

/// A parsed `@depth` `depthUpdate` event.
#[derive(Debug, Clone)]
pub struct DepthDiff {
    pub first_update_id: u64,    // `U`
    pub last_update_id: u64,     // `u`
    pub bids: Vec<(Price, Qty)>, // `b`
    pub asks: Vec<(Price, Qty)>, // `a`
}

/// The local book: each price level carries its current size and the synthetic
/// order id used for it, so a changed level can cancel the old id before
/// placing the new one.
#[derive(Debug, Default)]
struct LocalBook {
    bids: BTreeMap<Price, (Qty, OrderId)>,
    asks: BTreeMap<Price, (Qty, OrderId)>,
}

impl LocalBook {
    /// Cancel every resting synthetic order and empty the book.
    fn clear(&mut self) -> Vec<OrderMsg> {
        let mut out = Vec::new();
        for &(_, id) in self.bids.values() {
            out.push(OrderMsg::cancel(id));
        }
        for &(_, id) in self.asks.values() {
            out.push(OrderMsg::cancel(id));
        }
        self.bids.clear();
        self.asks.clear();
        out
    }

    /// Seed the book from a snapshot. Returns the `New` orders.
    fn seed(
        &mut self,
        bids: &[(Price, Qty)],
        asks: &[(Price, Qty)],
        next_id: &mut u64,
    ) -> Vec<OrderMsg> {
        let mut out = Vec::with_capacity(bids.len() + asks.len());
        for &(p, q) in bids {
            if q == 0 {
                continue;
            }
            let id = take_id(next_id);
            self.bids.insert(p, (q, id));
            out.push(OrderMsg::new_limit(id, Side::Buy, p, q));
        }
        for &(p, q) in asks {
            if q == 0 {
                continue;
            }
            let id = take_id(next_id);
            self.asks.insert(p, (q, id));
            out.push(OrderMsg::new_limit(id, Side::Sell, p, q));
        }
        out
    }

    /// For each level: if the size is unchanged, skip; otherwise cancel the old
    /// id (if any) and, if the new size is non-zero, place a new order.
    /// Returns the `Cancel`/`New` orders in order.
    fn apply_levels(
        &mut self,
        bids: &[(Price, Qty)],
        asks: &[(Price, Qty)],
        next_id: &mut u64,
    ) -> Vec<OrderMsg> {
        let mut out = Vec::new();
        for &(p, q) in bids {
            apply_side(&mut self.bids, Side::Buy, p, q, next_id, &mut out);
        }
        for &(p, q) in asks {
            apply_side(&mut self.asks, Side::Sell, p, q, next_id, &mut out);
        }
        out
    }
}

/// Apply one level change to one side of the book.
fn apply_side(
    side: &mut BTreeMap<Price, (Qty, OrderId)>,
    s: Side,
    p: Price,
    q: Qty,
    next_id: &mut u64,
    out: &mut Vec<OrderMsg>,
) {
    if let Some(&(cur_q, cur_id)) = side.get(&p) {
        if cur_q == q {
            return; // no-op: level unchanged
        }
        out.push(OrderMsg::cancel(cur_id));
        side.remove(&p);
    }

    if q == 0 {
        return; // level deleted
    }

    let id = take_id(next_id);
    side.insert(p, (q, id));
    out.push(OrderMsg::new_limit(id, s, p, q));
}

#[derive(Debug)]
enum State {
    /// Connected; snapshot not yet applied. Collecting diffs.
    Buffering(VecDeque<DepthDiff>),
    /// Synced; applying diffs with strict continuity.
    Live { last_update_id: u64 },
}

/// What `apply_diff` produces.
pub enum ApplyOutcome {
    /// Diff was queued (pre-snapshot); nothing to emit.
    Buffered,
    /// Diff applied; emit these `Cancel`/`New` orders.
    Applied(Vec<OrderMsg>),
    /// Continuity broken. Emit these cancels (the book was cleared), then the
    /// caller must re-fetch the snapshot — the diff has been re-buffered.
    Gap(Vec<OrderMsg>),
}

/// What `load_snapshot` produces.
pub enum SnapshotOutcome {
    /// Snapshot applied; emit these (cancels + seed `New`s + replayed updates).
    Synced(Vec<OrderMsg>),
    /// Snapshot is stale relative to the buffer; re-fetch (no mutation, no emit).
    Gap,
}

#[derive(Debug)]
pub struct DepthSync {
    book: LocalBook,
    state: State,
}

impl DepthSync {
    pub fn new() -> Self {
        Self {
            book: LocalBook::default(),
            state: State::Buffering(VecDeque::new()),
        }
    }

    pub fn is_buffering(&self) -> bool {
        matches!(self.state, State::Buffering(_))
    }

    /// Cancel any stale book and return to buffering. Called when (re)starting
    /// a connection so a fresh snapshot re-seeds from scratch.
    pub fn resync(&mut self) -> Vec<OrderMsg> {
        let cancels = self.book.clear();
        self.state = State::Buffering(VecDeque::new());
        cancels
    }

    /// Apply a `depthUpdate` event according to the current state.
    pub fn apply_diff(&mut self, diff: DepthDiff, next_id: &mut u64) -> ApplyOutcome {
        let live_last = match &self.state {
            State::Live { last_update_id } => Some(*last_update_id),
            State::Buffering(_) => None,
        };

        match live_last {
            None => {
                if let State::Buffering(buf) = &mut self.state {
                    buf.push_back(diff);
                }
                ApplyOutcome::Buffered
            }
            Some(last) => {
                if diff.first_update_id == last + 1 {
                    let msgs = self.book.apply_levels(&diff.bids, &diff.asks, next_id);
                    if let State::Live { last_update_id } = &mut self.state {
                        *last_update_id = diff.last_update_id;
                    }
                    ApplyOutcome::Applied(msgs)
                } else {
                    // Gap: the book is now unreliable. Clear it (emit cancels),
                    // re-buffer from this diff, and ask the caller to re-snapshot.
                    let cancels = self.book.clear();
                    self.state = State::Buffering(VecDeque::from([diff]));
                    ApplyOutcome::Gap(cancels)
                }
            }
        }
    }

    /// Apply a fetched snapshot: drop stale buffered diffs, detect a snapshot
    /// gap, then clear + seed + replay the remaining buffer.
    pub fn load_snapshot(&mut self, snap: Snapshot, next_id: &mut u64) -> SnapshotOutcome {
        let mut last = snap.last_update_id;

        // Drop buffered diffs already reflected in the snapshot (u < last), then
        // detect a gap: the first remaining diff must straddle (U <= last + 1).
        if let State::Buffering(buf) = &mut self.state {
            while buf.front().is_some_and(|d| d.last_update_id < last) {
                buf.pop_front();
            }
            if buf.front().is_some_and(|d| d.first_update_id > last + 1) {
                return SnapshotOutcome::Gap;
            }
        }

        // Commit: clear any stale book, seed from the snapshot, replay the buffer.
        let mut out = self.book.clear();
        out.extend(self.book.seed(&snap.bids, &snap.asks, next_id));
        if let State::Buffering(buf) = &mut self.state {
            while let Some(diff) = buf.pop_front() {
                out.extend(self.book.apply_levels(&diff.bids, &diff.asks, next_id));
                last = diff.last_update_id;
            }
        }
        self.state = State::Live {
            last_update_id: last,
        };
        SnapshotOutcome::Synced(out)
    }
}

impl Default for DepthSync {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a `/api/v3/depth` JSON body.
pub fn parse_snapshot(text: &str) -> Option<Snapshot> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    Some(Snapshot {
        last_update_id: v.get("lastUpdateId")?.as_u64()?,
        bids: parse_levels(v.get("bids")?)?,
        asks: parse_levels(v.get("asks")?)?,
    })
}

/// Parse a `@depth` `depthUpdate` frame. Returns `None` for non-depth frames.
pub fn parse_depth(text: &str) -> Option<DepthDiff> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    if v.get("e").and_then(|e| e.as_str()) != Some("depthUpdate") {
        return None;
    }

    Some(DepthDiff {
        first_update_id: v.get("U")?.as_u64()?,
        last_update_id: v.get("u")?.as_u64()?,
        bids: parse_levels(v.get("b")?)?,
        asks: parse_levels(v.get("a")?)?,
    })
}

/// Parse a `[[price, qty], ...]` array of string pairs into fixed-point levels.
fn parse_levels(arr: &serde_json::Value) -> Option<Vec<(Price, Qty)>> {
    arr.as_array()?
        .iter()
        .map(|level| {
            let price = parse_price(level.get(0)?.as_str()?)?;
            let qty = parse_qty(level.get(1)?.as_str()?)?;
            Some((price, qty))
        })
        .collect()
}

/// Owns an in-flight snapshot fetch so the runner can request one, poll for it
/// non-blockingly, and re-request after a gap or a failed fetch.
pub struct SnapshotFetcher {
    symbol: String,
    rx: Option<std::sync::mpsc::Receiver<Snapshot>>,
}

impl SnapshotFetcher {
    pub fn new(symbol: String) -> Self {
        Self { symbol, rx: None }
    }

    /// No fetch currently in flight (and not yet delivered).
    pub fn needs_snapshot(&self) -> bool {
        self.rx.is_none()
    }

    /// Spawn a background fetch; the result is delivered via [`poll`](Self::poll).
    pub fn request(&mut self) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.rx = Some(rx);
        let symbol = self.symbol.clone();
        thread::spawn(move || match fetch_snapshot(&symbol) {
            Ok(s) => {
                let _ = tx.send(s);
            }
            Err(e) => eprintln!("binance relay: snapshot fetch failed: {e}"),
        });
    }

    /// Non-blocking check for a fetched snapshot. `None` if not yet ready or if
    /// the fetch failed (in which case the slot is freed for re-request).
    pub fn poll(&mut self) -> Option<Snapshot> {
        let rx = self.rx.as_ref()?;
        match rx.try_recv() {
            Ok(s) => {
                self.rx = None;
                Some(s)
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.rx = None; // fetch thread died; allow re-request
                None
            }
        }
    }
}

/// Fetch `/api/v3/depth?symbol=<UPPER>&limit=<N>` and parse it.
pub fn fetch_snapshot(symbol: &str) -> Result<Snapshot, Error> {
    let symbol = symbol.to_ascii_uppercase();
    let path = format!("/api/v3/depth?symbol={symbol}&limit={SNAPSHOT_LIMIT}");
    let body = http_get(API_HOST, &path)?;
    parse_snapshot(&body).ok_or_else(|| Error::Http("malformed depth snapshot".into()))
}

/// Minimal blocking HTTPS GET using rustls (ring) + webpki-roots. Returns the
/// response body as a string. Used only for snapshot fetches, so simplicity
/// beats a full HTTP client.
fn http_get(host: &'static str, path: &str) -> Result<String, Error> {
    let mut roots = rustls::RootCertStore::empty();
    roots.roots = webpki_roots::TLS_SERVER_ROOTS.to_vec();

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = rustls::pki_types::ServerName::try_from(host)
        .map_err(|e| Error::Http(format!("invalid host {host}: {e}")))?;
    let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|e| Error::Http(format!("tls handshake setup: {e}")))?;

    let sock =
        TcpStream::connect((host, 443)).map_err(|e| Error::Http(format!("connect {host}: {e}")))?;
    let _ = sock.set_nodelay(true);

    let mut tls = rustls::StreamOwned::new(conn, sock);

    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         Accept-Encoding: identity\r\nUser-Agent: uxch-ingress-binance\r\n\r\n"
    );
    tls.write_all(request.as_bytes())
        .map_err(|e| Error::Http(format!("write: {e}")))?;

    let mut buf = Vec::new();
    tls.read_to_end(&mut buf)
        .map_err(|e| Error::Http(format!("read: {e}")))?;

    body_from_response(&buf)
}

/// Split an HTTP/1.1 response into its body, decoding `chunked` transfer
/// encoding or honoring `Content-Length` as needed.
fn body_from_response(buf: &[u8]) -> Result<String, Error> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut headers);
    let header_len = match resp
        .parse(buf)
        .map_err(|e| Error::Http(format!("http parse: {e}")))?
    {
        httparse::Status::Complete(n) => n,
        httparse::Status::Partial => return Err(Error::Http("partial http response".into())),
    };

    let code = resp.code.unwrap_or(0);
    if code != 200 {
        return Err(Error::Http(format!("snapshot http {code}")));
    }

    let body = &buf[header_len..];

    let mut chunked = false;
    let mut content_length: Option<usize> = None;
    for h in resp.headers.iter() {
        if h.name.eq_ignore_ascii_case("transfer-encoding")
            && std::str::from_utf8(h.value)
                .unwrap_or("")
                .contains("chunked")
        {
            chunked = true;
        }
        if h.name.eq_ignore_ascii_case("content-length") {
            content_length = std::str::from_utf8(h.value)
                .ok()
                .and_then(|s| s.trim().parse().ok());
        }
    }

    let body = if chunked {
        decode_chunked(body).ok_or_else(|| Error::Http("chunked decode failed".into()))?
    } else if let Some(n) = content_length {
        body.get(..n)
            .ok_or_else(|| Error::Http("body shorter than content-length".into()))?
            .to_vec()
    } else {
        body.to_vec()
    };

    String::from_utf8(body).map_err(|e| Error::Http(format!("non-utf8 body: {e}")))
}

/// Decode an HTTP/1.1 chunked-transfer-encoding body.
fn decode_chunked(body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;

    while i < body.len() {
        let rest = &body[i..];
        let nl = rest.iter().position(|&b| b == b'\n')?;
        // trim a trailing '\r' (CRLF line endings)
        let end = if nl > 0 && rest[nl - 1] == b'\r' {
            nl - 1
        } else {
            nl
        };
        let line = std::str::from_utf8(&rest[..end]).ok()?;
        // chunk extensions, if any, follow a ';'
        let size_str = line.split(';').next()?;
        let size = usize::from_str_radix(size_str.trim(), 16).ok()?;
        i += nl + 1;

        if size == 0 {
            break; // last-chunk
        }
        if i + size > body.len() {
            return None;
        }
        out.extend_from_slice(&body[i..i + size]);
        i += size;

        // trailing CRLF after each chunk
        if body[i..].starts_with(b"\r\n") {
            i += 2;
        } else if body[i..].starts_with(b"\n") {
            i += 1;
        }
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // 60000.10 * 1e8 = 6_000_010_000_000 ; 1.25 * 1e8 = 125_000_000
    // 60000.50 * 1e8 = 6_000_050_000_000 ; 0.50 * 1e8 = 50_000_000
    const SNAPSHOT_JSON: &str = r#"{
        "lastUpdateId": 100,
        "bids": [["60000.10", "1.25"], ["59999.50", "0"], ["59999.00", "2.00"]],
        "asks": [["60000.50", "0.50"], ["60001.00", "0"], ["60001.50", "3.00"]]
    }"#;

    const DEPTH_JSON: &str = r#"{
        "e": "depthUpdate", "E": 1, "s": "BNBUSDT",
        "U": 101, "u": 101,
        "b": [["60000.10", "2.00"], ["59998.00", "0"]],
        "a": [["60000.50", "0"], ["60002.00", "1.00"]]
    }"#;

    fn diff(u: u64, uu: u64) -> DepthDiff {
        DepthDiff {
            first_update_id: u,
            last_update_id: uu,
            bids: vec![],
            asks: vec![],
        }
    }

    fn ids(msgs: &[OrderMsg]) -> Vec<u64> {
        msgs.iter()
            .map(|m| match m {
                OrderMsg::New { id, .. } | OrderMsg::Cancel { id } => *id,
            })
            .collect()
    }

    #[test]
    fn parse_snapshot_reads_last_update_id_and_levels() {
        let snap = parse_snapshot(SNAPSHOT_JSON).unwrap();
        assert_eq!(snap.last_update_id, 100);
        assert_eq!(snap.bids.len(), 3);
        assert_eq!(snap.bids[0], (6_000_010_000_000, 125_000_000));
        assert_eq!(snap.asks[0], (6_000_050_000_000, 50_000_000));
    }

    #[test]
    fn parse_depth_filters_non_depth_and_reads_ids() {
        assert!(parse_depth(r#"{"e":"trade","p":"1"}"#).is_none());
        let d = parse_depth(DEPTH_JSON).unwrap();
        assert_eq!(d.first_update_id, 101);
        assert_eq!(d.last_update_id, 101);
        assert_eq!(d.bids[0], (6_000_010_000_000, 200_000_000)); // 2.00
        assert_eq!(d.asks[1], (6_000_200_000_000, 100_000_000)); // 60002.00, 1.00
    }

    #[test]
    fn local_book_seed_then_apply_then_clear() {
        let mut book = LocalBook::default();
        let mut next_id = 1u64;
        let snap = parse_snapshot(SNAPSHOT_JSON).unwrap();

        // seed: 2 non-zero bids + 2 non-zero asks → 4 News, ids 1..4
        let seeded = book.seed(&snap.bids, &snap.asks, &mut next_id);
        assert_eq!(seeded.len(), 4);
        assert_eq!(next_id, 5);
        assert!(seeded.iter().take(2).all(|m| matches!(
            m,
            OrderMsg::New {
                side: Side::Buy,
                ..
            }
        )));
        assert!(seeded.iter().skip(2).all(|m| matches!(
            m,
            OrderMsg::New {
                side: Side::Sell,
                ..
            }
        )));

        // apply a diff: replace bid 60000.10 (1.25→2.00), remove bid 59998 (new,
        // qty 0 → just nothing), remove ask 60000.50 (qty 0 → cancel), add ask
        // 60002.00 (1.00 → new).
        let d = parse_depth(DEPTH_JSON).unwrap();
        let applied = book.apply_levels(&d.bids, &d.asks, &mut next_id);
        // bid 60000.10: cancel old id 1 + new id 5
        // bid 59998.00 qty 0, not present → nothing
        // ask 60000.50: cancel old id 3 (qty 0)
        // ask 60002.00: new id 6
        assert_eq!(ids(&applied), vec![1, 5, 3, 6]);

        // clear: cancels the 4 currently-tracked ids (5, 2, 6, 4) in bids-then-asks order
        let cleared = book.clear();
        assert_eq!(cleared.len(), 4);
        assert!(cleared.iter().all(|m| matches!(m, OrderMsg::Cancel { .. })));
    }

    #[test]
    fn apply_side_is_noop_when_qty_unchanged() {
        let mut side = BTreeMap::<Price, (Qty, OrderId)>::new();
        side.insert(100, (10, 7));
        let mut next_id = 100u64;
        let mut out = Vec::new();
        apply_side(&mut side, Side::Buy, 100, 10, &mut next_id, &mut out);
        assert!(out.is_empty(), "unchanged level must produce no orders");
        assert_eq!(next_id, 100);
    }

    #[test]
    fn depth_sync_buffers_then_loads_snapshot_and_goes_live() {
        let mut sync = DepthSync::new();
        let mut next_id = 1u64;

        // Pre-snapshot: buffer diffs. d1 (u=99) is older than snapshot (100) → dropped.
        sync.apply_diff(diff(98, 99), &mut next_id);
        // d2 (U=100,u=101) straddles; d3 (U=102,u=103) continues.
        sync.apply_diff(diff(100, 101), &mut next_id);
        sync.apply_diff(diff(102, 103), &mut next_id);
        assert!(sync.is_buffering());

        let snap = Snapshot {
            last_update_id: 100,
            bids: vec![(100, 5)],
            asks: vec![(101, 5)],
        };
        let out = match sync.load_snapshot(snap, &mut next_id) {
            SnapshotOutcome::Synced(m) => m,
            _ => panic!("expected synced"),
        };
        // 2 seed News; d1 dropped; d2/d3 carry no levels → no updates.
        assert_eq!(out.len(), 2);
        assert!(matches!(
            sync.state,
            State::Live {
                last_update_id: 103
            }
        ));

        // Next live diff must continue (U == 104).
        match sync.apply_diff(diff(104, 104), &mut next_id) {
            ApplyOutcome::Applied(m) => assert!(m.is_empty()),
            _ => panic!("expected applied"),
        }
        assert!(matches!(
            sync.state,
            State::Live {
                last_update_id: 104
            }
        ));
    }

    #[test]
    fn depth_sync_live_gap_clears_and_rebuffers() {
        let mut sync = DepthSync::new();
        let mut next_id = 1u64;
        let snap = Snapshot {
            last_update_id: 100,
            bids: vec![(100, 5)],
            asks: vec![(101, 5)],
        };
        let _ = sync.load_snapshot(snap, &mut next_id);
        // seed produced 2 News (ids 1,2); book has them.

        // Gap: U=200 != 100+1.
        match sync.apply_diff(diff(200, 201), &mut next_id) {
            ApplyOutcome::Gap(cancels) => {
                // both seeded orders cancelled
                assert_eq!(cancels.len(), 2);
                assert!(cancels.iter().all(|m| matches!(m, OrderMsg::Cancel { .. })));
            }
            _ => panic!("expected gap"),
        }
        assert!(sync.is_buffering(), "gap must return to buffering");
    }

    #[test]
    fn depth_sync_stale_snapshot_is_gap_without_mutation() {
        let mut sync = DepthSync::new();
        let mut next_id = 1u64;
        // buffer a diff far ahead of the snapshot
        sync.apply_diff(diff(200, 201), &mut next_id);

        let snap = Snapshot {
            last_update_id: 100,
            bids: vec![(100, 5)],
            asks: vec![],
        };
        assert!(matches!(
            sync.load_snapshot(snap, &mut next_id),
            SnapshotOutcome::Gap
        ));
        // book untouched, still buffering
        assert!(sync.is_buffering());
    }

    #[test]
    fn resync_cancels_and_returns_to_buffering() {
        let mut sync = DepthSync::new();
        let mut next_id = 1u64;
        let _ = sync.load_snapshot(
            Snapshot {
                last_update_id: 100,
                bids: vec![(100, 5)],
                asks: vec![(101, 5)],
            },
            &mut next_id,
        );
        assert!(matches!(sync.state, State::Live { .. }));

        let cancels = sync.resync();
        assert_eq!(cancels.len(), 2);
        assert!(sync.is_buffering());
    }

    #[test]
    fn decode_chunked_decodes_a_simple_body() {
        let raw = b"5\r\nhello\r\n0\r\n\r\n";
        assert_eq!(decode_chunked(raw).as_deref(), Some(b"hello" as &[u8]));
    }

    #[test]
    fn body_from_response_handles_content_length() {
        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        assert_eq!(body_from_response(resp).unwrap(), "hello");
    }

    #[test]
    fn body_from_response_handles_chunked() {
        let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        assert_eq!(body_from_response(resp).unwrap(), "hello");
    }
}
