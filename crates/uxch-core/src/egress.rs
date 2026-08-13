use crate::{OrderId, Price, Qty};

/// A fill: the price and quantity traded, with the maker and taker order ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Trade {
    pub price: Price,
    pub qty: Qty,
    pub maker: OrderId,
    pub taker: OrderId,
}

/// Best bid / best offer snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Bbo {
    pub bid: Option<(Price, Qty)>,
    pub ask: Option<(Price, Qty)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum MarketEventContent {
    Trade(Trade),
    Bbo(Bbo),
    Reject { id: OrderId, reason: RejectReason },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum RejectReason {
    FokNotFullyFillable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MarketEvent {
    pub seq: u64,
    pub event: MarketEventContent,
}
