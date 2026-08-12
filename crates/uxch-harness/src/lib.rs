//! Latency harness

use std::hint::black_box;
use std::time::Instant;
use uxch_core::{MarketEvent, MatchingEngine, OrderMsg, Side, bounded::bounded};

/// Summary of one latency run.
#[derive(Debug, Clone)]
pub struct Report {
    pub label: &'static str,
    pub orders: usize,
    pub events: u64,
    pub min_ns: u64,
    pub mean_ns: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub total_ns: u64,
    pub throughput_ops_per_sec: f64,
}

impl Report {
    pub fn to_json(&self) -> String {
        format!(
            concat!(
                r#"{{"label":{label},"orders":{orders},"events":{events},"#,
                r#""latency_ns":{{"min":{min},"mean":{mean},"p50":{p50},"#,
                r#""p99":{p99},"p999":{p999},"max":{max}}},"#,
                r#""total_ns":{total},"throughput_ops_per_sec":{tps}}}"#
            ),
            label = sanitize_json_string(self.label),
            orders = self.orders,
            events = self.events,
            min = self.min_ns,
            mean = self.mean_ns,
            p50 = self.p50_ns,
            p99 = self.p99_ns,
            p999 = self.p999_ns,
            max = self.max_ns,
            total = self.total_ns,
            tps = self.throughput_ops_per_sec,
        )
    }
}

fn sanitize_json_string(s: &str) -> String {
    let mut out = String::from('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Run `orders` through a fresh engine, one at a time, recording end-to-end
/// per-order latency. First we run a warmup pass.
///
/// Egress is drained after each order, so the ring never fills.
pub fn run(label: &'static str, orders: &[OrderMsg]) -> Report {
    let cap = orders.len().max(1);

    // warmup (discard timings)
    {
        let (itx, irx) = bounded::<OrderMsg>(cap);
        let (etx, erx) = bounded::<MarketEvent>(cap);
        let mut engine = MatchingEngine::new(irx, etx);
        for order in orders {
            itx.send(*order).unwrap();
            engine.run();
            while erx.try_recv().is_some() {}
        }
    }

    // measured
    let (itx, irx) = bounded::<OrderMsg>(cap);
    let (etx, erx) = bounded::<MarketEvent>(cap);
    let mut engine = MatchingEngine::new(irx, etx);

    let mut latencies = Vec::with_capacity(orders.len());
    let mut events = 0u64;
    let t_start = Instant::now();
    for order in orders {
        let t0 = Instant::now();
        itx.send(*order).unwrap();
        engine.run();
        while let Some(ev) = erx.try_recv() {
            black_box(ev);
            events += 1;
        }
        latencies.push(t0.elapsed().as_nanos() as u64);
    }
    let total_ns = t_start.elapsed().as_nanos() as u64;

    latencies.sort_unstable();
    let n = latencies.len();
    let sum_ns = latencies.iter().map(|&x| x as u128).sum::<u128>();
    let mean_ns = if n == 0 {
        0
    } else {
        (sum_ns / n as u128) as u64
    };
    let throughput = if total_ns == 0 {
        0.0
    } else {
        n as f64 * 1_000_000_000.0 / total_ns as f64
    };

    Report {
        label,
        orders: n,
        events,
        min_ns: *latencies.first().unwrap_or(&0),
        mean_ns,
        p50_ns: percentile(&latencies, 50.0),
        p99_ns: percentile(&latencies, 99.0),
        p999_ns: percentile(&latencies, 99.9),
        max_ns: *latencies.last().unwrap_or(&0),
        total_ns,
        throughput_ops_per_sec: throughput,
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p / 100.0) * (sorted.len() - 1) as f64;
    let idx = rank.round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Alternating sell/buy limit orders at the same price,
/// so every other order crosses and fills. Exercises the match + trade + BBO
/// path rather than just resting.
pub fn steady_crossing(n: u64) -> Vec<OrderMsg> {
    (1..=n)
        .map(|i| {
            if i % 2 == 1 {
                OrderMsg::new_limit(i, Side::Sell, 100, 1)
            } else {
                OrderMsg::new_limit(i, Side::Buy, 100, 1)
            }
        })
        .collect()
}

/// Rest `levels` sell limits at increasing prices, then
/// one market buy large enough to sweep every level. Exercises a multi-level
/// sweep (many trades from a single order).
pub fn build_then_sweep(levels: u64) -> Vec<OrderMsg> {
    let mut orders = (1..=levels)
        .map(|i| OrderMsg::new_limit(i, Side::Sell, 100 + i as i64, 10))
        .collect::<Vec<_>>();
    orders.push(OrderMsg::new_market(levels + 1, Side::Buy, levels * 10));
    orders
}
