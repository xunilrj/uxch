use crate::book::{Book, Order, Pricing};
use crate::egress::{Bbo, MarketEvent, MarketEventContent, RejectReason};
use crate::ingress::OrderMsg;
use crate::slab::Slab;
use crate::{OrderId, OrderType, Qty, Side, TimeInForce, Trade};
use milkyapps_core::sync::bounded::{Receiver, Sender};

#[allow(dead_code)]
pub struct MatchingEngine {
    book: Book,
    orders: Slab<Order>,
    ingress: Receiver<OrderMsg>,
    egress: Sender<MarketEvent>,
    pricing: Pricing,
    seq: u64,
}

impl MatchingEngine {
    pub fn new(ingress: Receiver<OrderMsg>, egress: Sender<MarketEvent>) -> Self {
        Self {
            book: Book::new(),
            orders: Slab::default(),
            ingress,
            egress,
            pricing: Pricing::Maker,
            seq: 0,
        }
    }

    /// Match at price-time priority, publish trades + BBO on
    /// egress. Returns once the ingress channel is empty.
    pub fn run(&mut self) {
        while let Some(msg) = self.ingress.try_recv() {
            match msg {
                OrderMsg::New {
                    id,
                    side,
                    ty,
                    tif,
                    qty,
                } => self.handle_new(id, side, ty, tif, qty),
                OrderMsg::Cancel { id } => {
                    self.book.cancel(id);
                    self.publish_bbo();
                }
            }
        }
    }

    fn handle_new(&mut self, id: OrderId, side: Side, ty: OrderType, tif: TimeInForce, qty: Qty) {
        let crossing_limit = match ty {
            OrderType::Limit { price } => Some(price),
            OrderType::Market => None,
        };

        // reject unless the full quantity of a fok can be filled now.
        if tif == TimeInForce::Fok && !self.book.can_fill(side, crossing_limit, qty) {
            self.publish_reject(id, RejectReason::FokNotFullyFillable);
            self.publish_bbo();
            return;
        }

        let mut incoming = Order {
            id,
            side,
            ty,
            tif,
            qty,
            filled: 0,
            seq: 0,
        };

        let trades = match side {
            Side::Buy => self
                .book
                .match_buy(&mut incoming, crossing_limit, self.pricing),
            Side::Sell => self
                .book
                .match_sell(&mut incoming, crossing_limit, self.pricing),
        };

        for trade in trades {
            self.publish_trade(trade);
        }

        // only GTC limit orders rest on the book.
        if incoming.remaining() > 0
            && tif == TimeInForce::Gtc
            && matches!(ty, OrderType::Limit { .. })
        {
            self.book.insert(incoming);
        }

        self.publish_bbo();
    }

    fn publish(&mut self, event: MarketEventContent) {
        let ev = MarketEvent {
            seq: self.seq,
            event,
        };
        self.egress.send(ev).unwrap();
        self.seq += 1;
    }

    fn publish_bbo(&mut self) {
        self.publish(MarketEventContent::Bbo(Bbo {
            bid: self.book.best_bid(),
            ask: self.book.best_ask(),
        }));
    }

    fn publish_reject(&mut self, id: u64, reason: RejectReason) {
        self.publish(MarketEventContent::Reject { id, reason });
    }

    fn publish_trade(&mut self, trade: Trade) {
        self.publish(MarketEventContent::Trade(trade));
    }
}
