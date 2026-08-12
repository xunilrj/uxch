use crate::book::{Book, Order};
use crate::egress::MarketEvent;
use crate::ingress::OrderMsg;
use crate::slab::Slab;
use milkyapps_core::sync::bounded::{Receiver, Sender};

#[allow(dead_code)] // fields are read by the match loop, which is `todo!()` for now.
pub struct MatchingEngine {
    book: Book,
    orders: Slab<Order>,
    ingress: Receiver<OrderMsg>,
    egress: Sender<MarketEvent>,
}

impl MatchingEngine {
    pub fn new(ingress: Receiver<OrderMsg>, egress: Sender<MarketEvent>) -> Self {
        Self {
            book: Book::new(),
            orders: Slab::default(),
            ingress,
            egress,
        }
    }

    pub fn run(&mut self) {
        todo!("")
    }
}
