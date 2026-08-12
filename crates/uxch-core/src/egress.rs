use crate::{OrderId, Price, Qty};

/// A fill: the price and quantity traded, with the maker and taker order ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub price: Price,
    pub qty: Qty,
    pub maker: OrderId,
    pub taker: OrderId,
}

/// Best bid / best offer snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bbo {
    pub bid: Option<(Price, Qty)>,
    pub ask: Option<(Price, Qty)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    Trade(Trade),
    Bbo(Bbo),
    Reject { id: OrderId, reason: RejectReason },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    FokNotFullyFillable,
}
