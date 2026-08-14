use libtest_mimic::{Arguments, Failed, Trial};
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::Path;
use uxch::core::{
    MarketEvent, MarketEventContent, MatchingEngine, OrderMsg, OrderType, Side, TimeInForce,
    bounded,
};
use uxch_core::egress::RejectReason;

#[derive(Deserialize)]
struct TestCase {
    /// Human-readable note; not asserted on. Kept for fixture readability.
    #[serde(default)]
    #[allow(dead_code)]
    description: Option<String>,
    #[serde(default)]
    orders: Vec<OrderInput>,
    expected: Expected,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Limit,
    Market,
}

#[derive(Deserialize)]
struct OrderInput {
    id: u64,
    side: Side,
    #[serde(rename = "type")]
    kind: Kind,
    tif: TimeInForce,
    price: Option<i64>,
    qty: u64,
}

#[derive(Deserialize, Default)]
struct Expected {
    #[serde(default)]
    trades: Vec<TradeInput>,
    #[serde(default)]
    rejects: Vec<RejectInput>,
    #[serde(default)]
    bbo: Option<BboInput>,
}

#[derive(Deserialize)]
struct TradeInput {
    price: i64,
    qty: u64,
    maker: u64,
    taker: u64,
}

#[derive(Deserialize)]
struct RejectInput {
    id: u64,
    #[serde(default)]
    reason: String,
}

#[derive(Deserialize)]
struct BboInput {
    #[serde(default)]
    bid: Option<LevelInput>,
    #[serde(default)]
    ask: Option<LevelInput>,
}

#[derive(Deserialize)]
struct LevelInput {
    price: i64,
    qty: u64,
}

fn main() {
    let args = Arguments::from_args();

    let trials = match load_trials() {
        Ok(trials) => trials,
        Err(e) => {
            eprintln!(
                "error loading trial: {}",
                e.message().unwrap_or("(no message)")
            );
            std::process::exit(1);
        }
    };

    libtest_mimic::run(&args, trials).exit();
}

fn load_trials() -> Result<Vec<Trial>, Failed> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/matches");
    let mut trials = Vec::new();

    let entries = std::fs::read_dir(&dir)
        .map_err(|e| format!("read {dir_display}: {e}", dir_display = dir.display()))?;
    for entry in entries {
        let path = entry.map_err(|e| format!("dir entry: {e}"))?.path();
        if path.extension() == Some(OsStr::new("toml")) {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| format!("bad fixture name: {}", path.display()))?
                .to_owned();
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("read {}: {e}", path.display()))?;
            let tc: TestCase =
                toml::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))?;
            trials.push((name, tc));
        }
    }

    trials.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(trials
        .into_iter()
        .map(|(name, tc)| Trial::test(name, move || run_test_case(tc)))
        .collect())
}

fn run_test_case(case: TestCase) -> Result<(), Failed> {
    let (ingress_tx, ingress_rx) = bounded::bounded::<OrderMsg>(1024);
    let (egress_tx, egress_rx) = bounded::bounded::<MarketEvent>(1024);

    for order in &case.orders {
        let ty = match order.kind {
            Kind::Limit => OrderType::Limit {
                price: order.price.ok_or("limit order requires a price")?,
            },
            Kind::Market => OrderType::Market,
        };
        let msg = OrderMsg::New {
            id: order.id,
            side: order.side,
            ty,
            tif: order.tif,
            qty: order.qty,
        };
        ingress_tx.send(msg).map_err(|_| "ingress channel closed")?;
    }

    let mut engine = MatchingEngine::new(ingress_rx, egress_tx);
    engine.run();

    let mut fills = Vec::new();
    let mut last_l2 = None;
    let mut reject_ids = Vec::new();
    let mut reject_reasons: Vec<&'static str> = Vec::new();
    while let Some(ev) = egress_rx.try_recv() {
        match ev.event {
            MarketEventContent::Trade(t) => fills.push((t.price, t.qty, t.maker, t.taker)),
            MarketEventContent::L2(l) => last_l2 = Some(l),
            MarketEventContent::Reject { id, reason } => {
                reject_ids.push(id);
                reject_reasons.push(match reason {
                    RejectReason::FokNotFullyFillable => "FokNotFullyFillable",
                });
            }
        }
    }

    let expected_trades = case
        .expected
        .trades
        .iter()
        .map(|t| (t.price, t.qty, t.maker, t.taker))
        .collect::<Vec<_>>();
    if fills != expected_trades {
        return Err(format!(
            "trades mismatch\n  expected: {expected_trades:?}\n  got:      {fills:?}"
        )
        .into());
    }

    let expected_reject_ids = case
        .expected
        .rejects
        .iter()
        .map(|r| r.id)
        .collect::<Vec<_>>();
    if reject_ids != expected_reject_ids {
        return Err(format!(
            "rejects mismatch\n  expected: {expected_reject_ids:?}\n  got:      {reject_ids:?}"
        )
        .into());
    }

    for (i, exp) in case.expected.rejects.iter().enumerate() {
        let got = reject_reasons.get(i).copied().unwrap_or("");
        if got != exp.reason.as_str() {
            return Err(format!(
                "reject {i} reason mismatch\n  expected: {}\n  got:      {got}",
                exp.reason
            )
            .into());
        }
    }

    if let Some(exp_bbo) = case.expected.bbo {
        let actual = last_l2.ok_or_else(|| "expected an L2 update but got none".to_string())?;
        let exp_bid = exp_bbo.bid.map(|l| (l.price, l.qty));
        let exp_ask = exp_bbo.ask.map(|l| (l.price, l.qty));
        let actual_bid = actual.bids.first().map(|(p, q)| (*p, *q));
        let actual_ask = actual.asks.first().map(|(p, q)| (*p, *q));
        if actual_bid != exp_bid || actual_ask != exp_ask {
            return Err(format!(
                "bbo mismatch\n  expected: bid={exp_bid:?} ask={exp_ask:?}\n  got:      bid={:?} ask={:?}",
                actual_bid, actual_ask
            )
            .into());
        }
    }

    Ok(())
}
