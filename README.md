# uxch

A single-threaded Rust matching-engine.

- central limit order book
- lock-free ingress ring
- emits normalized market data over a fan-out ring
- pre-trade risk gate
- latency harness
- kernel-tuning runbook.

Do not use this in production.

## Architecture

```
                         Binance diff-depth WS
                                 │
                                 ▼
   ┌────────────────────────────────────────────────────┐
   │ uxch-ingress-binance                                │
   │  depth-sync (snapshot seed + diff stream) → OrderMsg│
   │  modes: live · --record <tape> · --replay <tape>    │
   └──────────────────┬─────────────────────────────────┘
        OrderMsg NDJSON  (TCP, one JSON object per line)
                      ▼
   ┌────────────────────────────────────────────────────┐
   │ uxch  (single-threaded matching core)               │
   │  ingress ring ─▶ MatchingEngine ─▶ egress ring      │
   └──────────────────┬─────────────────────────────────┘
        MarketEvent NDJSON  (TCP; uxch dials out to the sink)
                      ▼
   ┌────────────────────────────────────────────────────┐
   │ uxch-egress-terminal                                │
   │  render to stdout · --save (active) · --record (all)│
   │  failover across comma-separated listen addresses   │
   └────────────────────────────────────────────────────┘
```

Single-threaded matching core (LMAX style): no memory barriers, no false sharing,
the book stays hot in L1/L2. Ingress and egress are decoupled via the lock-free
rings.

## High availability

Two complete pipelines on different ports; both ingresses independently 
sync their orders, so both uxch instances converge to the same BBO. 
If one dies, the other is already live.

```
ingress-A ─▶ uxch-A ─▶ :9997 ┐
                            ├──▶ uxch-egress-terminal
ingress-B ─▶ uxch-B ─▶ :9996 ┘   (prefers :9997, falls back to :9996)
```


## Build & test

```
cargo r -p uxch -r
cargo t
just ha
```

## See more

Background reading for the design choices in this project. Some links are old and may not work anymore.

### Architecture
- https://www.infoq.com/presentations/LMAX
- https://mechanical-sympathy.blogspot.com (for false sharing, cache lines etc.)
- https://www.youtube.com/watch?v=rX0ItVEVjlo (for allocation-free data structures)
- https://www.youtube.com/watch?v=L7zSU9HI-6I (for caches, branches, prefetch) or https://accu.org/conf-docs/PDFs_2007/Sutter-Machine_Architecture.pdf

### Lock-free rings & concurrency
- https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf (for a canonical description of the MPMC ring + sequencer)
- https://web.archive.org/web/20220927095854/https://www.1024cores.net/home/lock-free-algorithms/queues/queue-catalog (for the per-cell sequence-number trick this kind of ring uses)
- https://www.cs.rochester.edu/~scott/papers/1996_PODC_queues.pdf (for foundational lock-free/lock-based queue algorithms)
- https://www.puppetmastertrading.com/images/hwViewForSwHackers.pdf (for ordering, fences, and why the single-threaded core avoids them entirely)

### Matching engines & market microstructure
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3104816 (for context for where matching engines sit in the stack)
- https://arxiv.org/html/2606.01183 (for architecture comparisons)

### Latency tuning on Linux
- https://www.brendangregg.com/sysperfbook.html (for measurement methodology before optimization)
- https://www.brendangregg.com/ebpf.html (for `bpftrace`/`perf` for per-event latency)
- https://www.youtube.com/watch?v=lJ8ydIuPFeU (for why the harness reports p99/p999 and not just means)
