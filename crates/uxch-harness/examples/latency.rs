//! Run the latency tests over two load and print a JSON report.
//!
//! `cargo run --release -p uxch-harness --example latency`

use uxch_harness::{Report, build_then_sweep, run, steady_crossing};

fn main() {
    let reports = [
        run("steady_crossing", &steady_crossing(100_000)),
        run("build_then_sweep", &build_then_sweep(64)),
    ];

    let body = reports
        .iter()
        .map(Report::to_json)
        .collect::<Vec<_>>()
        .join(",\n");
    println!("[\n{body}\n]");
}
