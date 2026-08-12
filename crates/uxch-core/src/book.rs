use crate::{OrderId, OrderType, Price, Qty, Side, TimeInForce};

/// A resting order in the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub ty: OrderType,
    pub tif: TimeInForce,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    /// Monotonic sequence assigned on ingress — drives price-time priority.
    pub seq: u64,
}

#[derive(Debug, Default)]
pub struct Book;

impl Book {
    pub fn new() -> Self {
        Self
    }
}
