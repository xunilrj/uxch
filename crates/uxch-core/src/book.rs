use crate::egress::{L2, Trade};
use crate::{OrderId, OrderType, Price, Qty, Side, TimeInForce};
use std::collections::{BTreeMap, VecDeque};

/// A resting order in the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub ty: OrderType,
    pub tif: TimeInForce,
    pub qty: Qty,
    pub filled: Qty,
    /// Monotonic sequence assigned on ingress — drives price-time priority.
    pub seq: u64,
}

impl Order {
    /// Quantity still unfilled.
    #[inline]
    pub fn remaining(&self) -> Qty {
        self.qty - self.filled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pricing {
    /// Fill at the resting maker's level price, so any price improvement
    /// accrues to the taker. Standard lit-CLOB behavior.
    Maker,
    // Fill at the aggressor's limit, available as `OrderType::Limit { price }`
    // on `taker.ty`; price improvement would then accrue to the maker instead
    // of the taker.
    // Taker,
    // A dark-pool mode crossing at the midpoint
    // Midpoint,
}

impl Pricing {
    #[inline]
    pub fn fill_price(self, maker_price: Price, _: &Order) -> Price {
        match self {
            Pricing::Maker => maker_price,
        }
    }
}

/// The order book: bids and asks indexed by price, FIFO (price-time) per level.
#[derive(Debug, Default)]
pub struct Book {
    bids: BTreeMap<Price, VecDeque<Order>>,
    asks: BTreeMap<Price, VecDeque<Order>>,
}

impl Book {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rest an order on its side of the book at its price.
    pub fn insert(&mut self, order: Order) {
        let OrderType::Limit { price } = order.ty else {
            unreachable!("only limit orders rest on the book");
        };

        match order.side {
            Side::Buy => self.bids.entry(price).or_default().push_back(order),
            Side::Sell => self.asks.entry(price).or_default().push_back(order),
        }
    }

    pub fn best_bid(&self) -> Option<(Price, Qty)> {
        let (&price, level) = self.bids.last_key_value()?;
        Some((price, level.iter().map(Order::remaining).sum::<Qty>()))
    }

    pub fn best_ask(&self) -> Option<(Price, Qty)> {
        let (&price, level) = self.asks.first_key_value()?;
        Some((price, level.iter().map(Order::remaining).sum::<Qty>()))
    }

    /// Aggregated L2 depth: bids best→worst, asks best→worst.
    pub fn l2(&self) -> L2 {
        let bids = self
            .bids
            .iter()
            .rev()
            .map(|(&p, lvl)| (p, lvl.iter().map(Order::remaining).sum::<Qty>()))
            .collect();
        let asks = self
            .asks
            .iter()
            .map(|(&p, lvl)| (p, lvl.iter().map(Order::remaining).sum::<Qty>()))
            .collect();
        L2 { bids, asks }
    }

    pub fn cancel(&mut self, id: OrderId) -> bool {
        remove_by_id(&mut self.bids, id) || remove_by_id(&mut self.asks, id)
    }

    pub fn can_fill(&self, side: Side, limit: Option<Price>, qty: Qty) -> bool {
        let mut remaining = qty;
        match side {
            Side::Buy => {
                for (&price, level) in &self.asks {
                    if let Some(limit) = limit
                        && price > limit
                    {
                        break;
                    }

                    for o in level {
                        remaining = remaining.saturating_sub(o.remaining());
                        if remaining == 0 {
                            return true;
                        }
                    }
                }
            }
            Side::Sell => {
                for (&price, level) in self.bids.iter().rev() {
                    if let Some(limit) = limit
                        && price < limit
                    {
                        break;
                    }

                    for o in level {
                        remaining = remaining.saturating_sub(o.remaining());
                        if remaining == 0 {
                            return true;
                        }
                    }
                }
            }
        }

        remaining == 0
    }

    pub fn match_buy(
        &mut self,
        taker: &mut Order,
        limit: Option<Price>,
        pricing: Pricing,
    ) -> Vec<Trade> {
        let mut trades = vec![];

        while taker.remaining() > 0 {
            let Some(ask_price) = self.asks.keys().next().copied() else {
                break;
            };

            if let Some(limit) = limit
                && ask_price > limit
            {
                break;
            }

            cross_level(&mut self.asks, ask_price, taker, pricing, &mut trades);
        }

        trades
    }

    pub fn match_sell(
        &mut self,
        taker: &mut Order,
        limit: Option<Price>,
        pricing: Pricing,
    ) -> Vec<Trade> {
        let mut trades = vec![];

        while taker.remaining() > 0 {
            let Some(bid_price) = self.bids.keys().next_back().copied() else {
                break;
            };

            if let Some(limit) = limit
                && bid_price < limit
            {
                break;
            }

            cross_level(&mut self.bids, bid_price, taker, pricing, &mut trades);
        }

        trades
    }
}

fn cross_level(
    side: &mut BTreeMap<Price, VecDeque<Order>>,
    price: Price,
    taker: &mut Order,
    pricing: Pricing,
    trades: &mut Vec<Trade>,
) {
    let Some(level) = side.get_mut(&price) else {
        return;
    };

    while taker.remaining() > 0 {
        let Some(maker) = level.front_mut() else {
            break;
        };

        let fill = taker.remaining().min(maker.remaining());
        maker.filled += fill;
        taker.filled += fill;

        let fill_price = pricing.fill_price(price, taker);

        trades.push(Trade {
            price: fill_price,
            qty: fill,
            maker: maker.id,
            taker: taker.id,
        });

        if maker.remaining() == 0 {
            level.pop_front();
        }
    }

    if level.is_empty() {
        side.remove(&price);
    }
}

fn remove_by_id(side: &mut BTreeMap<Price, VecDeque<Order>>, id: OrderId) -> bool {
    let mut empty_level = None;
    let mut found = false;

    for (&price, level) in side.iter_mut() {
        if let Some(pos) = level.iter().position(|o| o.id == id) {
            level.remove(pos);
            found = true;
            if level.is_empty() {
                empty_level = Some(price);
            }
            break;
        }
    }

    if let Some(price) = empty_level {
        side.remove(&price);
    }

    found
}
