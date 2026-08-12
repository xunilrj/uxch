use crate::{OrderId, OrderType, Price, Qty, Side, TimeInForce};

/// Ingress message: a new order or a cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderMsg {
    New {
        id: OrderId,
        side: Side,
        ty: OrderType,
        tif: TimeInForce,
        qty: Qty,
    },
    Cancel {
        id: OrderId,
    },
}

impl OrderMsg {
    pub fn new_limit(id: OrderId, side: Side, price: Price, qty: Qty) -> Self {
        Self::New {
            id,
            side,
            ty: OrderType::Limit { price },
            tif: TimeInForce::Gtc,
            qty,
        }
    }

    pub fn new_market(id: OrderId, side: Side, qty: Qty) -> Self {
        Self::New {
            id,
            side,
            ty: OrderType::Market,
            tif: TimeInForce::Ioc,
            qty,
        }
    }

    pub fn cancel(id: OrderId) -> Self {
        Self::Cancel { id }
    }
}
