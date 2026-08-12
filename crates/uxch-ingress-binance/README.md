# uxch-ingress-binance

Mirrors the live Binance order book into `uxch` as a stream of synthetic
`OrderMsg` events over TCP NDJSON.

The ingress connects to Binance's **diff-depth** WebSocket stream, fetches a
REST depth snapshot, keeps a local book in sync using Binance's documented
diff-depth algorithm, and emits every book change downstream as `OrderMsg`.

Snapshot levels become `New` limit orders, a changed level becomes a `Cancel`
of the old synthetic id followed by a `New`, and a removed level becomes a
`Cancel`.

```
Binance @depth WS ─┐
                   ├─→ DepthSync (local book) ─→ OrderMsg stream ─→ uxch TCP sink
REST /depth ────────┘
```

## Why synthetic events

Binance depth is **L2**: each price level is an aggregate size, not a list of
individual orders. `uxch`'s matching engine expects orders (`OrderMsg`), so to
make the Binance book visible to the engine we synthesize **one synthetic limit
order per price level**.

## Data sources

| Source | URL | What it gives |
|---|---|---|
| Diff-depth WS | `wss://stream.binance.com:9443/ws/<symbol>@depth` | `depthUpdate` frames: incremental level changes |
| REST snapshot | `GET https://api.binance.com/api/v3/depth?symbol=<UPPER>&limit=1000` | A full L2 book + `lastUpdateId` |
