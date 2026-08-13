default:
    @just --list

# Build the whole workspace (dev profile).
[group("build")]
build:
    cargo build --all-features --all-targets

# Build the whole workspace (release profile).
[group("build")]
build-release:
    cargo build --release --all-features --all-targets

# Remove build artifacts.
[group("build")]
clean:
    cargo clean

# Run all tests.
[group("test")]
test:
    cargo test --all-features --all-targets

# Run the latency benchmark
[group("benchmark")]
bench-latency:
    cargo run --release -p uxch-harness --example latency

# Fail if any code is unformatted.
[group("ci")]
fmt-check:
    cargo fmt --all -- --check

# Lint the whole workspace, treating warnings as errors.
[group("ci")]
clippy:
    cargo clippy --all-features --all-targets -- -D warnings

# Complete CI checks: formatting, lints, tests.
[group("ci")]
check: fmt-check clippy test

# Run uxch-egress-terminal + uxch + uxch-ingress-binance. Logs at target/logs.
[group("smoke")]
smoke:
    #!/usr/bin/env bash
    set -euo pipefail

    duration="${SMOKE_DURATION:-60}"
    symbol="${SMOKE_SYMBOL:-bnbusdt}"

    cargo build -p uxch-egress-terminal -p uxch -p uxch-ingress-binance

    mkdir -p target/logs
    LOGS=$(mktemp -d target/logs/smoke.XXXXXX)
    LOG="$LOGS/all.log"
    : > "$LOG"
    echo "smoke: merged logs → $LOG"

    INGRESS_ADDR=127.0.0.1:9999
    EGRESS_ADDR=127.0.0.1:9998

    TERM_PID=0
    UXCH_PID=0
    ING_PID=0
    cleanup() {
        if [ "${TERM_PID:-0}" -ne 0 ] || [ "${UXCH_PID:-0}" -ne 0 ] || [ "${ING_PID:-0}" -ne 0 ]; then
            echo "smoke: killing pipeline"
        fi
        kill "$ING_PID" "$UXCH_PID" "$TERM_PID" 2>/dev/null || true
        wait "$ING_PID" "$UXCH_PID" "$TERM_PID" 2>/dev/null || true
    }
    trap cleanup EXIT
    trap 'cleanup; exit 130' INT

    # Start in dependency order: terminal (egress listener) → uxch (engine,
    # connects to terminal + listens for ingress) → ingress (connects to uxch).
    # Each side retries its downstream, so a little startup skew is fine.
    UXCH_EGRESS_ADDR="$EGRESS_ADDR" \
        target/debug/uxch-egress-terminal \
        > >(awk -v l="[terminal]" '{ print l, $0; fflush() }' >> "$LOG") 2>&1 &
    TERM_PID=$!
    sleep 0.5
    UXCH_INGRESS_ADDR="$INGRESS_ADDR" UXCH_EGRESS_ADDR="$EGRESS_ADDR" \
        target/debug/uxch \
        > >(awk -v l="[uxch]" '{ print l, $0; fflush() }' >> "$LOG") 2>&1 &
    UXCH_PID=$!
    sleep 0.5
    UXCH_INGRESS_ADDR="$INGRESS_ADDR" \
        target/debug/uxch-ingress-binance "$symbol" \
        > >(awk -v l="[ingress]" '{ print l, $0; fflush() }' >> "$LOG") 2>&1 &
    ING_PID=$!

    echo "smoke: pids terminal=$TERM_PID uxch=$UXCH_PID ingress=$ING_PID"
    echo "smoke: running for ${duration}s (Ctrl-C to stop early); tail -f $LOG"

    sleep "$duration"
    echo "smoke: done — see $LOG"

# Record a live Binance order stream to a tape file (NDJSON OrderMsg per line).
[group("binance")]
binance-record:
    #!/usr/bin/env bash
    set -euo pipefail

    duration="${RECORD_DURATION:-20}"
    symbol="${RECORD_SYMBOL:-bnbusdt}"

    cargo build -p uxch-ingress-binance
    mkdir -p target/tapes
    TAPE="target/tapes/${symbol}.tape"
    : > "$TAPE"

    echo "record: saving ${symbol}@depth → $TAPE for ${duration}s"
    target/debug/uxch-ingress-binance --record "$TAPE" "$symbol" &
    ING_PID=$!
    cleanup() { kill "$ING_PID" 2>/dev/null || true; wait "$ING_PID" 2>/dev/null || true; }
    trap cleanup EXIT INT

    sleep "$duration"
    echo "record: stopping ingress (pid $ING_PID)"
    kill "$ING_PID" 2>/dev/null || true
    wait "$ING_PID" 2>/dev/null || true
    trap - EXIT INT

    lines=$(wc -l < "$TAPE" | tr -d ' ')
    echo "record: wrote $TAPE ($lines lines)"

# Convergence / determinism check
[group("binance")]
binance-replay-check:
    #!/usr/bin/env bash
    set -euo pipefail

    tape="${REPLAY_TAPE:-target/tapes/bnbusdt.tape}"
    bin_a="${UXCH_BIN_A:-target/debug/uxch}"
    bin_b="${UXCH_BIN_B:-target/debug/uxch}"
    egress_addr="${UXCH_EGRESS_ADDR:-127.0.0.1:9998}"
    ingress_addr="${UXCH_INGRESS_ADDR:-127.0.0.1:9999}"

    if [ ! -f "$tape" ]; then
        echo "replay-check: tape not found: $tape (run 'just binance-record' first)" >&2
        exit 1
    fi
    for b in "$bin_a" "$bin_b"; do
        if [ ! -x "$b" ]; then
            echo "replay-check: uxch binary not found/executable: $b" >&2
            exit 1
        fi
    done

    cargo build -p uxch-egress-terminal -p uxch-ingress-binance

    mkdir -p target
    tmp=$(mktemp -d target/replay-check.XXXXXX)
    events_a="$tmp/eventsA.jsonl"
    events_b="$tmp/eventsB.jsonl"
    echo "replay-check: tape=$tape bin_a=$bin_a bin_b=$bin_b tmp=$tmp"

    run_once() {
        # $1 = uxch binary, $2 = events output file
        local uxch_bin="$1"
        local out="$2"

        UXCH_EGRESS_ADDR="$egress_addr" target/debug/uxch-egress-terminal --save "$out" \
            >"$tmp/terminal.log" 2>&1 &
        local term_pid=$!
        sleep 0.5

        UXCH_INGRESS_ADDR="$ingress_addr" UXCH_EGRESS_ADDR="$egress_addr" "$uxch_bin" \
            >"$tmp/uxch.log" 2>&1 &
        local uxch_pid=$!
        sleep 0.5

        # Replay runs in the foreground; it exits at EOF.
        UXCH_INGRESS_ADDR="$ingress_addr" target/debug/uxch-ingress-binance --replay "$tape" \
            >"$tmp/ingress.log" 2>&1 || true

        # Let the terminal flush the last events, then tear down.
        sleep 0.5
        kill "$uxch_pid" "$term_pid" 2>/dev/null || true
        wait "$uxch_pid" "$term_pid" 2>/dev/null || true
    }

    run_once "$bin_a" "$events_a"
    run_once "$bin_b" "$events_b"

    if diff -u "$events_a" "$events_b"; then
        echo "CONVERGENT (identical MarketEvent egress: $(wc -l < "$events_a" | tr -d ' ') lines)"
        exit 0
    else
        echo "DIVERGENT (see diff above)" >&2
        exit 1
    fi

# HA failover stress test. Options: UXCH_HA_ITERS, UXCH_HA_SEED
[group("test")]
ha:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p uxch -p uxch-egress-terminal -p uxch-ingress-binance
    UXCH_HA_ITERS="${UXCH_HA_ITERS:-1}" \
    UXCH_HA_SEED="${UXCH_HA_SEED:-0}" \
    UXCH_HA_TAPE_SIZE="${UXCH_HA_TAPE_SIZE:-20000}" \
    UXCH_HA_PACE_US="${UXCH_HA_PACE_US:-100}" \
    cargo test -p uxch --test ha -- --ignored --nocapture
