//! Matching engine core.

pub mod book;
pub mod egress;
pub mod engine;
pub mod ingress;
pub mod slab;

pub use book::{Book, Order, Pricing};
pub use egress::{Bbo, MarketEvent, MarketEventContent, Trade};
pub use engine::MatchingEngine;
pub use ingress::OrderMsg;
pub use milkyapps_core::sync::bounded;
pub use slab::Slab;

pub type OrderId = u64;
pub type Price = i64;
pub type Qty = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum OrderType {
    Limit { price: Price },
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum TimeInForce {
    /// Good-til-cancelled: the order rests on the book and stays there until it
    /// is fully filled or explicitly cancelled.
    Gtc,
    /// Immediate-or-cancel: any portion that can cross now is filled; whatever
    /// remains is cancelled and never rests on the book.
    Ioc,
    /// Fill-or-kill: the order must be filled in full immediately against
    /// current book depth, or it is cancelled entirely — no partial fill, no rest.
    Fok,
}
