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
