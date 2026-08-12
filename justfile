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
