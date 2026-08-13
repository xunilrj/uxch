//! Terminal display for uxch egress.
//!
//! Listens on one or more TCP sockets (`$UXCH_EGRESS_ADDR`, comma-separated).

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use uxch_core::egress::RejectReason;
use uxch_core::{Bbo, MarketEvent, MarketEventContent, Trade};

/// Fixed-point scale used across uxch (8 decimals).
const SCALE: u64 = 100_000_000;

enum Feed {
    Connected(usize),
    Disconnected(usize),
    Line { src: usize, port: u16, line: String },
}

pub(crate) struct Failover {
    connected: Vec<bool>,
    last_seen: Vec<Option<Instant>>,
    active: Option<usize>,
    stall: Duration,
}

impl Failover {
    pub(crate) fn new(n: usize, stall: Duration) -> Self {
        Self {
            connected: vec![false; n],
            last_seen: vec![None; n],
            active: None,
            stall,
        }
    }

    pub(crate) fn active(&self) -> Option<usize> {
        self.active
    }

    /// Force the active source to `i`
    pub(crate) fn set_active(&mut self, i: usize) {
        self.active = Some(i);
    }

    /// Apply a feed event observed at `now`. Returns the raw line to forward
    /// (render / `--save`) when the event is a line from the active source.
    pub(crate) fn apply<'a>(&mut self, feed: &'a Feed, now: Instant) -> Option<&'a str> {
        match feed {
            Feed::Connected(i) => {
                self.connected[*i] = true;
                // A more-preferred source (re)connected → return to it.
                if self.active.is_none_or(|a| *i < a) {
                    self.active = Some(*i);
                }
                None
            }
            Feed::Disconnected(i) => {
                self.connected[*i] = false;
                self.last_seen[*i] = None;
                if self.active == Some(*i) {
                    self.active = self.preferred_connected();
                }
                None
            }
            Feed::Line { src, line, .. } => {
                self.last_seen[*src] = Some(now);
                if self.active.is_none() {
                    // A line with no prior Connected (e.g. missed event): adopt
                    // this source so a single-source stream still forwards.
                    self.active = Some(*src);
                }
                if self.active == Some(*src) {
                    Some(line.as_str())
                } else {
                    None
                }
            }
        }
    }

    /// Called on a recv_timeout. If the active source is stale and another
    /// connected source is fresh, fail over to the lowest-index fresh source.
    /// Does nothing when both are quiet (no-flap guard).
    pub(crate) fn on_stall(&mut self, now: Instant) {
        let Some(a) = self.active else {
            return;
        };
        if self.is_fresh(a, now) {
            return;
        }
        for i in 0..self.connected.len() {
            if i == a || !self.connected[i] {
                continue;
            }
            if self.is_fresh(i, now) {
                self.active = Some(i);
                return;
            }
        }
    }

    fn is_fresh(&self, i: usize, now: Instant) -> bool {
        match self.last_seen[i] {
            Some(t) => now.duration_since(t) <= self.stall,
            None => false,
        }
    }

    fn preferred_connected(&self) -> Option<usize> {
        (0..self.connected.len()).find(|&i| self.connected[i])
    }
}

/// What the egress terminal does with an incoming event of sequence `seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeqDecision {
    /// `seq <= last_fwd`: already forwarded
    Stale,
    /// `seq == last_fwd + 1`
    Advance,
    /// `seq > last_fwd + 1`
    Gap { need: u64, missing: u64 },
}

fn seq_decision(last_fwd: Option<u64>, seq: u64) -> SeqDecision {
    match last_fwd {
        None => SeqDecision::Advance,
        Some(n) => {
            let need = n + 1;
            if seq < need {
                SeqDecision::Stale
            } else if seq == need {
                SeqDecision::Advance
            } else {
                SeqDecision::Gap {
                    need,
                    missing: seq - need,
                }
            }
        }
    }
}

/// Serve the terminal. Binds every addr in `addrs` (preference order), fans all
/// sources into one selection loop.
pub fn serve(
    addrs: Vec<SocketAddr>,
    save: Option<PathBuf>,
    record: Option<PathBuf>,
    stall: Duration,
) -> Result<(), std::io::Error> {
    if addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no egress addresses",
        ));
    }
    let n = addrs.len();

    // Bind all listeners up front so a bad addr fails fast.
    let mut listeners: Vec<(usize, u16, TcpListener)> = Vec::with_capacity(n);
    let mut port_of: Vec<u16> = Vec::with_capacity(n);
    for (i, addr) in addrs.iter().enumerate() {
        let l = TcpListener::bind(addr)?;
        let port = l
            .local_addr()
            .map(|a| a.port())
            .unwrap_or_else(|_| addr.port());
        port_of.push(port);
        let tag = if i == 0 { " [primary]" } else { "" };
        println!("uxch-egress-terminal: listening on {addr} (source {i}{tag})");
        listeners.push((i, port, l));
    }

    let mut record_out = match &record {
        Some(p) => Some(BufWriter::new(File::create(p)?)),
        None => None,
    };
    let mut save_out = match &save {
        Some(p) => Some(BufWriter::new(File::create(p)?)),
        None => None,
    };

    let (tx, rx) = mpsc::channel::<Feed>();
    for (i, port, listener) in listeners {
        let tx = tx.clone();
        thread::spawn(move || reader_loop(i, port, listener, tx));
    }

    let mut failover = Failover::new(n, stall);
    let mut last_fwd: Option<u64> = None;
    let mut recovering = false;

    // Per-source ring of recent `(seq, raw line)` events.
    const BUF_CAP: usize = 4096;
    let mut buffers = (0..n).map(|_| VecDeque::new()).collect::<Vec<_>>();

    loop {
        let feed = match rx.recv_timeout(stall) {
            Ok(f) => f,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let now = Instant::now();
                let before = failover.active();
                failover.on_stall(now);
                let after = failover.active();
                if before != after {
                    eprintln!(
                        "uxch-egress-terminal: active {} → {} (stalled)",
                        fmt_port(&port_of, before),
                        fmt_port(&port_of, after),
                    );
                }

                // A stall may switch to a source whose buffer already holds the
                // next needed seq (its recent past). Drain before reporting so a
                // quiet-but-fillable gap doesn't stall the stream.
                let advanced = drain(
                    &mut failover,
                    &port_of,
                    &mut buffers,
                    &mut last_fwd,
                    &mut save_out,
                )?;
                if advanced {
                    recovering = false;
                }
                if recovering {
                    let need = last_fwd.map(|n| n + 1).unwrap_or(0);
                    eprintln!(
                        "uxch-egress-terminal: recovery: still waiting for seq {need} \
                         (last forwarded {})",
                        last_fwd.map_or("none".to_string(), |n| n.to_string()),
                    );
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                eprintln!("uxch-egress-terminal: all readers exited");
                break;
            }
        };
        let now = Instant::now();

        if let Feed::Line { port, line, .. } = &feed
            && let Some(out) = record_out.as_mut()
        {
            writeln!(out, "PORT {port}: {line}")?;
            out.flush()?;
        }

        // Update failover connection/last_seen/active state and log any
        // active-source switch the state machine makes on connect / disconnect.
        let before = failover.active();
        let _ = failover.apply(&feed, now);
        let after = failover.active();
        if before != after {
            let reason = match &feed {
                Feed::Connected(_) => match (before, after) {
                    (None, Some(_)) => "adopted",
                    _ => "return-to-preferred",
                },
                Feed::Disconnected(_) => "dropped",
                Feed::Line { .. } => "adopted",
            };
            eprintln!(
                "uxch-egress-terminal: active {} → {} ({reason})",
                fmt_port(&port_of, before),
                fmt_port(&port_of, after),
            );
        }

        // Forward the line. The active source is forwarded directly on the hot
        // path (single parse, no clone — the common case). Non-active sources
        // are buffered into their ring as gap-fill material. On a gap (or while
        // recovering) the rings are drained to fill missing seqs from a
        // standby's recent past.
        if let Feed::Line { src, line, .. } = feed {
            let ev = match serde_json::from_str::<MarketEvent>(&line) {
                Ok(ev) => ev,
                Err(e) => {
                    eprintln!("uxch-egress-terminal: decode error: {e} | {line}");
                    continue;
                }
            };
            let seq = ev.seq;
            let is_active = failover.active() == Some(src);

            if !recovering && is_active {
                // Hot path: judge the active source against the watermark.
                match seq_decision(last_fwd, seq) {
                    SeqDecision::Stale => {}
                    SeqDecision::Advance => {
                        println!("{}", render(&ev.event));
                        if let Some(out) = save_out.as_mut() {
                            out.write_all(line.as_bytes())?;
                            out.write_all(b"\n")?;
                            out.flush()?;
                        }
                        last_fwd = Some(seq);
                    }
                    SeqDecision::Gap { need, missing } => {
                        // The active source jumped ahead: seqs need..seq-1 were
                        // not delivered. Stash the ahead event and try to fill
                        // the gap from other sources' buffers (a standby's
                        // recent past, sent while it was non-active).
                        buffer_in(&mut buffers[src], seq, line, BUF_CAP);
                        recovering = true;
                        eprintln!(
                            "uxch-egress-terminal: GAP after seq {}: missing seqs \
                             {need}..{} ({missing} event(s)); recovering from buffers",
                            last_fwd.unwrap_or(0),
                            need + missing - 1,
                        );
                        let advanced = drain(
                            &mut failover,
                            &port_of,
                            &mut buffers,
                            &mut last_fwd,
                            &mut save_out,
                        )?;
                        if advanced
                            && !need_in_buffers(&buffers, last_fwd.map(|n| n + 1).unwrap_or(0))
                        {
                            recovering = false;
                        }
                    }
                }
            } else if !recovering {
                // Non-active, normal mode: just buffer (gap-fill material). No
                // drain — the active source owns the live stream.
                buffer_in(&mut buffers[src], seq, line, BUF_CAP);
            } else {
                // Recovery: buffer every source's event, then drain. The needed
                // seq may now be in any source's buffer.
                buffer_in(&mut buffers[src], seq, line, BUF_CAP);
                let advanced = drain(
                    &mut failover,
                    &port_of,
                    &mut buffers,
                    &mut last_fwd,
                    &mut save_out,
                )?;
                if advanced && !need_in_buffers(&buffers, last_fwd.map(|n| n + 1).unwrap_or(0)) {
                    recovering = false;
                }
            }
        }
    }

    Ok(())
}

/// Forward the contiguous run of missing seqs from the per-source ring buffers,
/// advancing `last_fwd`. Called on a gap (or during recovery) to fill from a
/// standby's recent past — events it sent while non-active, which were buffered
/// rather than forwarded.
fn drain(
    failover: &mut Failover,
    port_of: &[u16],
    buffers: &mut [VecDeque<(u64, String)>],
    last_fwd: &mut Option<u64>,
    save_out: &mut Option<BufWriter<File>>,
) -> Result<bool, std::io::Error> {
    let mut advanced = false;
    loop {
        let need = last_fwd.map(|n| n + 1).unwrap_or(0);
        let preferred = failover.active();

        // Prefer the active source's buffer (no flapping); fall back to any
        // source's buffer for gap-fill. `find_seq` clones the line so the ring
        // can be mutated afterwards without a borrow conflict.
        let mut found: Option<(usize, u64, String)> = None;
        if let Some(p) = preferred
            && let Some(line) = find_seq(&buffers[p], need)
        {
            found = Some((p, need, line));
        }
        if found.is_none() {
            for (i, buf) in buffers.iter().enumerate() {
                if let Some(line) = find_seq(buf, need) {
                    found = Some((i, need, line));
                    break;
                }
            }
        }

        let Some((src, seq, line)) = found else {
            break;
        };
        remove_seq(&mut buffers[src], seq);

        if preferred != Some(src) {
            let before = failover.active();
            failover.set_active(src);
            eprintln!(
                "uxch-egress-terminal: RECOVERED seq {seq} from {} — following it",
                fmt_port(port_of, Some(src)),
            );
            let after = failover.active();
            if before != after {
                eprintln!(
                    "uxch-egress-terminal: active {} → {} (recovered)",
                    fmt_port(port_of, before),
                    fmt_port(port_of, after),
                );
            }
        }

        // Render + save. The line is re-parsed only on this (rare) gap-fill
        // path; the hot path renders from the event it just parsed.
        if let Ok(ev) = serde_json::from_str::<MarketEvent>(&line) {
            println!("{}", render(&ev.event));
        }
        if let Some(out) = save_out.as_mut() {
            out.write_all(line.as_bytes())?;
            out.write_all(b"\n")?;
            out.flush()?;
        }
        *last_fwd = Some(seq);
        advanced = true;
    }
    Ok(advanced)
}

/// Push `(seq, line)` onto a source's ring, evicting the oldest entry when it
/// exceeds `cap`. Takes the line by value (no clone): callers move the received
/// line in.
fn buffer_in(buf: &mut VecDeque<(u64, String)>, seq: u64, line: String, cap: usize) {
    buf.push_back((seq, line));
    while buf.len() > cap {
        buf.pop_front();
    }
}

/// Whether any source's ring holds the entry for `seq` (used to decide when
/// recovery has caught up to the live frontier).
fn need_in_buffers(buffers: &[VecDeque<(u64, String)>], seq: u64) -> bool {
    buffers.iter().any(|b| b.iter().any(|(s, _)| *s == seq))
}

/// Find the raw line for `seq` in a source's ring. Linear scan; rings are small
/// (`BUF_CAP`). Returns a cloned line so the caller can mutate the ring
/// afterwards.
fn find_seq(buf: &VecDeque<(u64, String)>, seq: u64) -> Option<String> {
    buf.iter().find(|(s, _)| *s == seq).map(|(_, l)| l.clone())
}

/// Remove the (unique) entry with `seq` from a source's ring.
fn remove_seq(buf: &mut VecDeque<(u64, String)>, seq: u64) {
    if let Some(i) = buf.iter().position(|(s, _)| *s == seq) {
        buf.remove(i);
    }
}

/// Format a source reference for a failover-switch log line: `source {i}
/// (port {p})`, or `no active source` when `i` is `None`.
fn fmt_port(ports: &[u16], i: Option<usize>) -> String {
    match i {
        Some(i) => match ports.get(i) {
            Some(p) => format!("source {i} (port {p})"),
            None => format!("source {i}"),
        },
        None => "no active source".into(),
    }
}

/// Per-listener accept loop
fn reader_loop(i: usize, port: u16, listener: TcpListener, tx: mpsc::Sender<Feed>) {
    loop {
        let stream = match listener.accept() {
            Ok((s, peer)) => {
                eprintln!("uxch-egress-terminal: source {i} (port {port}) connected from {peer}");
                s
            }
            Err(e) => {
                eprintln!("uxch-egress-terminal: accept port {port} failed: {e}");
                continue;
            }
        };
        let _ = tx.send(Feed::Connected(i));
        if let Err(e) = relay(stream, i, port, &tx) {
            eprintln!("uxch-egress-terminal: source {i} (port {port}): {e}");
        }
        let _ = tx.send(Feed::Disconnected(i));
        eprintln!(
            "uxch-egress-terminal: source {i} (port {port}) disconnected; awaiting reconnect"
        );
    }
}

fn relay(
    stream: TcpStream,
    src: usize,
    port: u16,
    tx: &mpsc::Sender<Feed>,
) -> Result<(), std::io::Error> {
    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let l = line?;
        if l.is_empty() {
            continue;
        }
        if tx.send(Feed::Line { src, port, line: l }).is_err() {
            break; // main loop gone
        }
    }
    Ok(())
}

pub fn render(ev: &MarketEventContent) -> String {
    match ev {
        MarketEventContent::Trade(t) => render_trade(t),
        MarketEventContent::Bbo(b) => render_bbo(b),
        MarketEventContent::Reject { id, reason } => {
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

    fn line(src: usize, port: u16, s: &str) -> Feed {
        Feed::Line {
            src,
            port,
            line: s.to_string(),
        }
    }

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
            render(&MarketEventContent::Trade(t)),
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
            render(&MarketEventContent::Bbo(full)),
            "bbo    bid=60000.10000000x1.25000000  ask=60000.50000000x0.50000000"
        );
        let empty = Bbo {
            bid: None,
            ask: None,
        };
        assert_eq!(
            render(&MarketEventContent::Bbo(empty)),
            "bbo    bid=-  ask=-"
        );
    }

    #[test]
    fn render_reject_uses_kebab_reason() {
        let ev = MarketEventContent::Reject {
            id: 42,
            reason: RejectReason::FokNotFullyFillable,
        };
        assert_eq!(render(&ev), "reject id=42 reason=fok-not-fully-fillable");
    }

    #[test]
    fn market_event_round_trips_through_serde() {
        let ev = MarketEvent {
            seq: 7,
            event: MarketEventContent::Bbo(Bbo {
                bid: Some((100, 5)),
                ask: None,
            }),
        };
        let s = serde_json::to_string(&ev).unwrap();
        assert!(s.starts_with(r#"{"seq":7,"event":{"bbo":"#));
        let back: MarketEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn seq_decision_first_event_advances() {
        assert_eq!(seq_decision(None, 0), SeqDecision::Advance);
        // A first event that isn't seq 0 is still accepted as the start.
        assert_eq!(seq_decision(None, 5), SeqDecision::Advance);
    }

    #[test]
    fn seq_decision_contiguous_advances() {
        assert_eq!(seq_decision(Some(10), 11), SeqDecision::Advance);
    }

    #[test]
    fn seq_decision_behind_is_stale() {
        // A behind source re-sends already-forwarded events → drop.
        assert_eq!(seq_decision(Some(10), 10), SeqDecision::Stale);
        assert_eq!(seq_decision(Some(10), 3), SeqDecision::Stale);
    }

    #[test]
    fn seq_decision_ahead_is_a_gap() {
        // Active source jumped from 10 to 13 → seqs 11,12 missing.
        assert_eq!(
            seq_decision(Some(10), 13),
            SeqDecision::Gap {
                need: 11,
                missing: 2
            }
        );
    }

    fn stall_3s() -> Duration {
        Duration::from_secs(3)
    }

    #[test]
    fn primary_drops_then_standby_forwards() {
        let mut f = Failover::new(2, stall_3s());
        let t = Instant::now();
        f.apply(&Feed::Connected(0), t);
        f.apply(&Feed::Connected(1), t);
        assert_eq!(f.active(), Some(0));
        assert_eq!(f.apply(&line(0, 9997, "a"), t), Some("a"));
        // Standby events are dropped while primary is active.
        assert_eq!(f.apply(&line(1, 9996, "x"), t), None);
        f.apply(&Feed::Disconnected(0), t);
        assert_eq!(f.active(), Some(1));
        assert_eq!(f.apply(&line(1, 9996, "b"), t), Some("b"));
    }

    #[test]
    fn primary_reconnect_returns_to_primary() {
        let mut f = Failover::new(2, stall_3s());
        let t = Instant::now();
        f.apply(&Feed::Connected(0), t);
        f.apply(&Feed::Connected(1), t);
        f.apply(&Feed::Disconnected(0), t);
        assert_eq!(f.active(), Some(1));
        assert_eq!(f.apply(&line(1, 9996, "b"), t), Some("b"));
        // Primary returns → active switches back to 0.
        f.apply(&Feed::Connected(0), t);
        assert_eq!(f.active(), Some(0));
        assert_eq!(f.apply(&line(0, 9997, "c"), t), Some("c"));
    }

    #[test]
    fn primary_stall_fails_over_to_fresh_standby() {
        let mut f = Failover::new(2, stall_3s());
        let t0 = Instant::now();
        f.apply(&Feed::Connected(0), t0);
        f.apply(&Feed::Connected(1), t0);
        // Primary last seen at t0; standby last seen just before the stall
        // check so it is still fresh when the primary is already stale.
        f.apply(&line(0, 9997, "a"), t0);
        f.apply(&line(1, 9996, "b"), t0 + Duration::from_millis(3900));
        // At t0 + 4s the primary is stale (4s > 3s) but the standby is fresh.
        f.on_stall(t0 + Duration::from_secs(4));
        assert_eq!(f.active(), Some(1));
        assert_eq!(
            f.apply(&line(1, 9996, "c"), t0 + Duration::from_secs(4)),
            Some("c")
        );
    }

    #[test]
    fn both_stale_does_not_flap() {
        let mut f = Failover::new(2, stall_3s());
        let t0 = Instant::now();
        f.apply(&Feed::Connected(0), t0);
        f.apply(&Feed::Connected(1), t0);
        f.apply(&line(0, 9997, "a"), t0);
        f.apply(&line(1, 9996, "b"), t0 + Duration::from_millis(50));
        // Both stale at t0 + 4s → no switch.
        f.on_stall(t0 + Duration::from_secs(4));
        assert_eq!(f.active(), Some(0));
    }

    #[test]
    fn standby_connects_first_then_yields_to_primary() {
        let mut f = Failover::new(2, stall_3s());
        let t = Instant::now();
        f.apply(&Feed::Connected(1), t);
        assert_eq!(f.active(), Some(1));
        assert_eq!(f.apply(&line(1, 9996, "x"), t), Some("x"));
        f.apply(&Feed::Connected(0), t);
        assert_eq!(f.active(), Some(0));
        assert_eq!(f.apply(&line(0, 9997, "y"), t), Some("y"));
    }

    #[test]
    fn single_source_is_passthrough() {
        let mut f = Failover::new(1, stall_3s());
        let t = Instant::now();
        f.apply(&Feed::Connected(0), t);
        assert_eq!(f.active(), Some(0));
        assert_eq!(f.apply(&line(0, 9998, "z"), t), Some("z"));
    }
}
