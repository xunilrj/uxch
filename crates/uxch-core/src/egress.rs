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

/// Full L2 depth: aggregated size at every price level. Bids are ordered
/// best→worst (descending price), asks best→worst (ascending price).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct L2 {
    pub bids: Vec<(Price, Qty)>,
    pub asks: Vec<(Price, Qty)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum MarketEventContent {
    Trade(Trade),
    L2(L2),
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
