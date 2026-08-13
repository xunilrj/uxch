//! Run HA integration test with chaos and save artifacts at `target/ha-test/`
//! Env:
//!   `UXCH_HA_ITERS`      — kill-iterations to run (default 1).
//!   `UXCH_HA_SEED`       — RNG seed for reproducible kill schedules (default 0).
//!   `UXCH_HA_TAPE_SIZE`  — synthesized tape length in orders (default 20_000).
//!   `UXCH_HA_PACE_US`    — microsecs slept between replayed orders (default 100).
//!                         Pacing keeps the egress sink from backpressuring so the
//!                         no-kill baseline is deterministic; the resulting ~2s
//!                         drain also gives scheduled kills room to land mid-stream.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use uxch_core::{OrderMsg, Side};

/// Fixed-point scale (8 decimals), matching the rest of uxch.
const SCALE: i64 = 100_000_000;
/// Same scale as `u64` for quantity math.
const SCALE_U: u64 = 100_000_000;

const SETTLE: Duration = Duration::from_millis(1200);
const SPAWN_GAP: Duration = Duration::from_millis(300);
const POLL: Duration = Duration::from_millis(20);
const RUN_TIMEOUT: Duration = Duration::from_secs(30);
const STALL_SECS: u64 = 1;

const KILL_AFTER_MS: u64 = 700;
const KILL_WINDOW_MS: u64 = 1000;
const KILL_FIRST_JITTER_MS: u64 = 200;

#[test]
#[ignore = "slow: spawns the full pipeline; run via `just ha` or --ignored"]
fn ha_failover_preserves_canonical_stream() -> Result<(), Box<dyn std::error::Error>> {
    let iters = env_u64("UXCH_HA_ITERS", 1);
    let seed = env_u64("UXCH_HA_SEED", 0);
    let tape_size = env_u64("UXCH_HA_TAPE_SIZE", 20_000);
    let pace_us = env_u64("UXCH_HA_PACE_US", 100);

    let bins = Bins::resolve()?;
    let work = WorkDir::new();
    eprintln!("ha: work dir {}", work.dir.display());

    let tape = work.dir.join("tape.jsonl");
    write_synthetic_tape(&tape, tape_size as usize, seed)?;
    eprintln!("ha: tape {} ({} orders)", tape.display(), tape_size);

    // Canonical: a single pipeline, no kills. Its `--save` is the reference
    // `MarketEvent` stream every other run is checked against.
    let canonical = run_canonical(&bins, &work, &tape, pace_us)?;
    eprintln!("ha: canonical = {} events", canonical.len());

    // Baseline: HA topology, no kills → must match canonical. This proves the
    // harness/binaries are sound before we introduce chaos.
    let baseline = run_ha(
        &bins,
        &work,
        &tape,
        &ChaosSchedule::none(),
        "baseline",
        pace_us,
    )?;
    let baseline_cmp = compare(&work, "baseline", &canonical, &baseline.save)?;
    if baseline_cmp.diverged() {
        // A baseline divergence is a harness/binary bug (not the failover gap),
        // so we fail fast — but still emit the report so it is diagnosable.
        let report = write_report(&work, &canonical, &baseline, &baseline_cmp, &[])?;
        return Err(format!(
            "ha: BASELINE diverged from canonical (harness bug, not failover) — \
             canonical {can} vs baseline {b}; diff → {diff}; report → {report}",
            can = canonical.len(),
            b = baseline.save.len(),
            diff = work.iter_dir("baseline").join("diff.txt").display(),
            report = report.display(),
        )
        .into());
    }
    eprintln!("ha: baseline (no kills) matches canonical");

    // Chaotic iterations
    let mut outcomes: Vec<IterOutcome> = Vec::with_capacity(iters as usize);
    for i in 0..iters {
        let label = format!("iter{i}");
        let sched = ChaosSchedule::seeded(seed.wrapping_add(i + 1));
        let ha = run_ha(&bins, &work, &tape, &sched, &label, pace_us)?;
        eprintln!(
            "ha: {label} = {} events (canonical {})",
            ha.save.len(),
            canonical.len()
        );
        let cmp = compare(&work, &label, &canonical, &ha.save)?;
        if cmp.diverged() {
            eprintln!(
                "ha: {label} DIVERGENT (first diff @{})",
                cmp.first_diff.unwrap()
            );
        } else {
            eprintln!("ha: {label} matches canonical");
        }
        outcomes.push(IterOutcome {
            label: label.clone(),
            schedule: sched.events.clone(),
            comparison: cmp,
            ports: ha.per_port,
        });
    }

    // Write report
    let report = write_report(&work, &canonical, &baseline, &baseline_cmp, &outcomes)?;
    eprintln!("ha: report → {}", report.display());

    let diverged: Vec<&IterOutcome> = outcomes
        .iter()
        .filter(|o| o.comparison.diverged())
        .collect();
    if diverged.is_empty() {
        eprintln!("ha: all {iters} iteration(s) matched canonical");
        return Ok(());
    }
    let summary = diverged
        .iter()
        .map(|o| {
            format!(
                "  {}: first diff @{} (canonical {} vs ha {})",
                o.label,
                o.comparison.first_diff.unwrap(),
                o.comparison.canon_len,
                o.comparison.ha_len
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    Err(format!(
        "ha: {}/{} kill-iteration(s) diverged (failover is not gap-free); \
         full report → {report}\n{summary}",
        diverged.len(),
        iters,
        report = report.display()
    )
    .into())
}

struct Bins {
    uxch: PathBuf,
    terminal: PathBuf,
    ingress: PathBuf,
}

impl Bins {
    fn resolve() -> Result<Self, Box<dyn std::error::Error>> {
        // `CARGO_BIN_EXE_uxch` is this crate's own binary; its parent is the
        // profile dir (e.g. `target/debug`), which also holds the sibling bins.
        let uxch = PathBuf::from(env!("CARGO_BIN_EXE_uxch"));
        let prof = uxch.parent().ok_or("no parent of uxch bin")?;
        let terminal = prof.join("uxch-egress-terminal");
        let ingress = prof.join("uxch-ingress-binance");
        for (name, p) in [
            ("uxch", &uxch),
            ("uxch-egress-terminal", &terminal),
            ("uxch-ingress-binance", &ingress),
        ] {
            if !p.is_file() {
                return Err(format!(
                    "binary `{name}` not found at {} — build first: cargo build -p uxch -p uxch-egress-terminal -p uxch-ingress-binance",
                    p.display()
                )
                .into());
            }
        }
        Ok(Self {
            uxch,
            terminal,
            ingress,
        })
    }
}

struct WorkDir {
    dir: PathBuf,
}

impl WorkDir {
    fn new() -> Self {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/ha-test");
        let _ = fs::create_dir_all(&dir);
        Self { dir }
    }

    fn iter_dir(&self, label: &str) -> PathBuf {
        let d = self.dir.join(label);
        let _ = fs::create_dir_all(&d);
        d
    }

    fn save_path(&self, label: &str) -> PathBuf {
        self.dir.join(format!("{label}.jsonl"))
    }
}

struct ChaosSchedule {
    events: Vec<(Duration, String)>,
}

impl ChaosSchedule {
    fn none() -> Self {
        Self { events: Vec::new() }
    }

    fn seeded(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let roles = ["ing-a", "uxch-a", "ing-b", "uxch-b"];
        let pick = |rng: &mut Rng| roles[rng.u32(roles.len() as u32) as usize].to_string();
        let mut events = Vec::new();
        // First kill is always early (tight jitter) so it lands mid-drain —
        // without this a fast drain can finish before any kill fires and the
        // iteration would trivially match canonical.
        events.push((
            Duration::from_millis(KILL_AFTER_MS + rng.u32(KILL_FIRST_JITTER_MS as u32) as u64),
            pick(&mut rng),
        ));
        // 0..=2 extra kills spread across the wider window.
        for _ in 0..rng.u32(3) {
            events.push((
                Duration::from_millis(KILL_AFTER_MS + rng.u32(KILL_WINDOW_MS as u32) as u64),
                pick(&mut rng),
            ));
        }
        events.sort_by_key(|(t, _)| *t);
        Self { events }
    }
}

fn stamp_line_prefix(start: Instant) -> String {
    let rel_ms = start.elapsed().as_millis();
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    let ms = dur.subsec_millis();
    format!("t+{rel_ms:>6}ms {h:02}:{m:02}:{s:02}.{ms:03}")
}

fn is_terminal_event(line: &str) -> bool {
    !line.starts_with("uxch-egress-terminal:")
}

struct MergedLog {
    out: Arc<Mutex<BufWriter<File>>>,
    start: Instant,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl MergedLog {
    fn new(path: &Path, start: Instant) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let out = Arc::new(Mutex::new(BufWriter::new(file)));
        let stop = Arc::new(AtomicBool::new(false));
        // Flusher thread: periodic flush so `tail -f` is near-real-time without
        // per-line syscalls on the hot path.
        let (out_c, stop_c) = (Arc::clone(&out), Arc::clone(&stop));
        let flusher = std::thread::spawn(move || {
            while !stop_c.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(200));
                if let Ok(mut g) = out_c.lock() {
                    let _ = g.flush();
                }
            }
            if let Ok(mut g) = out_c.lock() {
                let _ = g.flush();
            }
        });
        Ok(Self {
            out,
            start,
            stop,
            threads: vec![flusher],
        })
    }

    /// Write one prefixed line. Empty lines are dropped.
    fn emit(&self, app: &str, line: &str) {
        let line = line.trim_end_matches(['\n', '\r']);
        if line.is_empty() {
            return;
        }
        let s = stamp_line_prefix(self.start);
        if let Ok(mut g) = self.out.lock() {
            let _ = writeln!(g, "{s} [{app}] {line}");
        }
    }

    /// Pipe a child stdout/stderr stream
    fn pipe_stream(
        &mut self,
        app: &str,
        stream: Box<dyn Read + Send>,
        count: Option<fn(&str) -> bool>,
    ) {
        let out = Arc::clone(&self.out);
        let start = self.start;
        let app = app.to_string();
        let t = std::thread::spawn(move || {
            let mut r = BufReader::new(stream);
            let mut buf = String::new();
            let mut n: u64 = 0;
            loop {
                buf.clear();
                match r.read_line(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {
                        let line = buf.trim_end_matches(['\n', '\r']);
                        if line.is_empty() {
                            continue;
                        }
                        let tag = match count {
                            Some(pred) if pred(line) => {
                                n += 1;
                                format!(" #{n}")
                            }
                            _ => String::new(),
                        };
                        let s = stamp_line_prefix(start);
                        if let Ok(mut g) = out.lock() {
                            let _ = writeln!(g, "{s} [{app}]{tag} {line}");
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        self.threads.push(t);
    }

    fn tail_file(&mut self, app: &str, path: &Path) {
        let out = Arc::clone(&self.out);
        let stop = Arc::clone(&self.stop);
        let start = self.start;
        let app = app.to_string();
        let path = path.to_path_buf();
        let t = std::thread::spawn(move || {
            // Wait for the child to create the file (up to ~5s).
            for _ in 0..250 {
                if path.exists() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            let Ok(file) = File::open(&path) else {
                return;
            };
            let mut r = BufReader::new(file);
            let mut buf = String::new();
            // Per-port event counter: a `PORT <p>: <ndjson>` line gets `#N` where
            // N is that port's Nth event — i.e. that pipeline's own stream index
            // (the index the report's "prefix of canonical[0..N]" refers to).
            let mut counts: HashMap<u16, u64> = HashMap::new();
            let emit = |buf: &str, counts: &mut HashMap<u16, u64>| {
                let line = buf.trim_end_matches(['\n', '\r']);
                if line.is_empty() {
                    return;
                }
                let n = line
                    .strip_prefix("PORT ")
                    .and_then(|rest| rest.split_once(':'))
                    .and_then(|(p, _)| p.trim().parse::<u16>().ok())
                    .map(|port| {
                        let e = counts.entry(port).or_insert(0);
                        *e += 1;
                        *e
                    });
                let tag = match n {
                    Some(m) => format!(" #{m}"),
                    None => String::new(),
                };
                let s = stamp_line_prefix(start);
                if let Ok(mut g) = out.lock() {
                    let _ = writeln!(g, "{s} [{app}]{tag} {line}");
                }
            };
            loop {
                buf.clear();
                match r.read_line(&mut buf) {
                    Ok(0) => {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Ok(_) => emit(&buf, &mut counts),
                    Err(_) => break,
                }
            }
            // Final drain: the file is static now (child killed) — read any
            // lines appended between the last poll and `stop`.
            loop {
                buf.clear();
                match r.read_line(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => emit(&buf, &mut counts),
                    Err(_) => break,
                }
            }
        });
        self.threads.push(t);
    }

    fn close(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
        if let Ok(mut g) = self.out.lock() {
            let _ = g.flush();
        }
    }
}

impl Drop for MergedLog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct Supervisor {
    merged: MergedLog,
    children: Vec<(String, Child)>,
    start: Instant,
}

impl Supervisor {
    fn new(log_path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let start = Instant::now();
        let merged = MergedLog::new(log_path, start)?;
        Ok(Self {
            merged,
            children: Vec::new(),
            start,
        })
    }

    fn logln(&self, line: &str) {
        self.merged.emit("actions", line);
    }

    fn tail_file(&mut self, app: &str, path: &Path) {
        self.merged.tail_file(app, path);
    }

    fn spawn(
        &mut self,
        role: &str,
        mut cmd: Command,
        count_stdout: Option<fn(&str) -> bool>,
    ) -> std::io::Result<()> {
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn()?;
        let pid = child.id();
        self.logln(&format!("SPAWN role={role} pid={pid}"));
        if let Some(out) = child.stdout.take() {
            self.merged.pipe_stream(role, Box::new(out), count_stdout);
        }
        if let Some(err) = child.stderr.take() {
            self.merged.pipe_stream(role, Box::new(err), None);
        }
        self.children.push((role.to_string(), child));
        Ok(())
    }

    fn kill(&mut self, role: &str) -> bool {
        let Some(child) = self.children.iter_mut().find(|(r, _)| r == role) else {
            self.logln(&format!("KILL role={role} (not present — skipped)"));
            return false;
        };
        let pid = child.1.id();
        let _ = child.1.kill();
        self.logln(&format!("KILL role={role} pid={pid}"));
        true
    }

    fn poll(&mut self, role: &str) -> Option<ExitStatus> {
        let child = self.children.iter_mut().find(|(r, _)| r == role)?;
        child.1.try_wait().ok().flatten()
    }

    fn alive(&mut self, role: &str) -> bool {
        self.poll(role).is_none()
    }

    fn kill_all_and_wait(&mut self) {
        // Kill first, then log (can't borrow `self` for logln while iterating
        // `self.children` mutably).
        let mut msgs = Vec::new();
        for (role, child) in self.children.iter_mut() {
            let pid = child.id();
            let _ = child.kill();
            msgs.push(format!("CLEANUP kill role={role} pid={pid}"));
        }
        for m in &msgs {
            self.logln(m);
        }
        let mut reaped = Vec::new();
        for (role, child) in self.children.iter_mut() {
            let _ = child.wait();
            reaped.push(format!("CLEANUP reaped role={role}"));
        }
        for m in &reaped {
            self.logln(m);
        }
    }

    fn shutdown(&mut self) {
        self.merged.close();
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        for (_, child) in self.children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn run_canonical(
    bins: &Bins,
    work: &WorkDir,
    tape: &Path,
    pace_us: u64,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let dir = work.iter_dir("canonical");
    let ports = free_ports(2);
    let save = work.save_path("canonical");
    let mut sup = Supervisor::new(&dir.join("timeline.log"))?;

    let mut t = Command::new(&bins.terminal);
    t.env("UXCH_EGRESS_ADDR", format!("127.0.0.1:{}", ports[0]))
        .env("UXCH_FAILOVER_STALL_SECS", STALL_SECS.to_string())
        .arg("--save")
        .arg(&save);
    sup.spawn("terminal", t, Some(is_terminal_event))?;
    std::thread::sleep(SPAWN_GAP);

    let mut u = Command::new(&bins.uxch);
    u.env("UXCH_INGRESS_ADDR", format!("127.0.0.1:{}", ports[1]))
        .env("UXCH_EGRESS_ADDR", format!("127.0.0.1:{}", ports[0]));
    sup.spawn("uxch", u, None)?;
    std::thread::sleep(SPAWN_GAP);

    let mut ing = Command::new(&bins.ingress);
    ing.env("UXCH_INGRESS_ADDR", format!("127.0.0.1:{}", ports[1]))
        .arg("--replay")
        .arg(tape)
        .arg("--pace")
        .arg(pace_us.to_string());
    sup.spawn("ingress", ing, None)?;

    health_check(&mut sup, &["terminal", "uxch"], "canonical")?;

    // Wait for the replay to hit EOF (the ingress exits on its own).
    wait_for(&mut sup, "ingress", "canonical")?;

    std::thread::sleep(SETTLE);
    sup.kill_all_and_wait();

    let lines = read_lines(&save)?;
    sup.logln(&format!("COMPARE canonical save={} events", lines.len()));
    sup.shutdown();
    Ok(lines)
}

struct HaRun {
    save: Vec<String>,
    per_port: Vec<PortStream>,
}

struct PortStream {
    port: u16,
    role: String,
    lines: Vec<String>,
}

fn run_ha(
    bins: &Bins,
    work: &WorkDir,
    tape: &Path,
    sched: &ChaosSchedule,
    label: &str,
    pace_us: u64,
) -> Result<HaRun, Box<dyn std::error::Error>> {
    let dir = work.iter_dir(label);
    let ports = free_ports(4);
    let save = work.save_path(label);
    let record = dir.join("record.txt");
    let mut sup = Supervisor::new(&dir.join("timeline.log"))?;
    sup.logln(&format!(
        "RUN label={label} pace={pace_us}µs ports=term0:{} term1:{} ing-a:{} ing-b:{}",
        ports[0], ports[1], ports[2], ports[3]
    ));
    for (t, role) in &sched.events {
        sup.logln(&format!(
            "SCHEDULE kill role={role} at t+{}ms",
            t.as_millis()
        ));
    }

    let _ = fs::remove_file(&record);
    let mut t = Command::new(&bins.terminal);
    t.env(
        "UXCH_EGRESS_ADDR",
        format!("127.0.0.1:{},127.0.0.1:{}", ports[0], ports[1]),
    )
    .env("UXCH_FAILOVER_STALL_SECS", STALL_SECS.to_string())
    .arg("--save")
    .arg(&save)
    .arg("--record")
    .arg(&record);
    sup.spawn("terminal", t, Some(is_terminal_event))?;
    // Tail the terminal's `--record` file into the timeline so the per-port
    // egress appears on the same timeline as everything else.
    sup.tail_file("record", &record);
    std::thread::sleep(SPAWN_GAP);

    // Pipeline A (primary, source 0) and pipeline B (standby, source 1).
    let mut ua = Command::new(&bins.uxch);
    ua.env("UXCH_INGRESS_ADDR", format!("127.0.0.1:{}", ports[2]))
        .env("UXCH_EGRESS_ADDR", format!("127.0.0.1:{}", ports[0]));
    sup.spawn("uxch-a", ua, None)?;

    let mut ub = Command::new(&bins.uxch);
    ub.env("UXCH_INGRESS_ADDR", format!("127.0.0.1:{}", ports[3]))
        .env("UXCH_EGRESS_ADDR", format!("127.0.0.1:{}", ports[1]));
    sup.spawn("uxch-b", ub, None)?;
    std::thread::sleep(SPAWN_GAP);

    let mut ia = Command::new(&bins.ingress);
    ia.env("UXCH_INGRESS_ADDR", format!("127.0.0.1:{}", ports[2]))
        .arg("--replay")
        .arg(tape)
        .arg("--pace")
        .arg(pace_us.to_string());
    sup.spawn("ing-a", ia, None)?;

    let mut ib = Command::new(&bins.ingress);
    ib.env("UXCH_INGRESS_ADDR", format!("127.0.0.1:{}", ports[3]))
        .arg("--replay")
        .arg(tape)
        .arg("--pace")
        .arg(pace_us.to_string());
    sup.spawn("ing-b", ib, None)?;

    // Drive the run: poll the two replays to EOF while firing scheduled kills.
    let mut pending = sched.events.clone();
    let deadline = sup.start + RUN_TIMEOUT;
    let mut gated = false;
    loop {
        let now = Instant::now();
        let elapsed = now.duration_since(sup.start);

        // One-time startup gate: a bind/port-race failure exits within
        // milliseconds, so the first poll catches it. Deliberately no sleep —
        // we must not push kill firing past the drain.
        if !gated {
            gated = true;
            for role in ["terminal", "uxch-a", "uxch-b"] {
                if !sup.alive(role) {
                    sup.logln(&format!("HEALTH_CHECK role={role} exited early"));
                    let msg = format!(
                        "ha {label}: `{role}` exited during startup — see {}",
                        sup_log_dir(label).join("timeline.log").display()
                    );
                    sup.kill_all_and_wait();
                    sup.shutdown();
                    return Err(msg.into());
                }
            }
            sup.logln("HEALTH_CHECK ok");
        }

        while let Some((t, _)) = pending.first() {
            if *t <= elapsed {
                let (_, role) = pending.remove(0);
                sup.kill(&role);
            } else {
                break;
            }
        }

        let a_done = sup.poll("ing-a").is_some();
        let b_done = sup.poll("ing-b").is_some();
        if a_done && b_done {
            sup.logln("both ingresses exited (EOF or killed)");
            break;
        }
        if now >= deadline {
            sup.logln("RUN_TIMEOUT reached — stopping");
            break;
        }
        std::thread::sleep(POLL);
    }

    // Let the terminal drain TCP buffers / finish any pending stall failover,
    // then tear everything down. `--save` flushes per line, so it is kill-safe.
    std::thread::sleep(SETTLE);
    sup.kill_all_and_wait();

    let save_lines = read_lines(&save)?;
    sup.logln(&format!("COMPARE {label} save={} events", save_lines.len()));

    let per_port = parse_record(
        &record,
        &[(ports[0], "A (primary)"), (ports[1], "B (standby)")],
    )?;
    for ps in &per_port {
        sup.logln(&format!(
            "COMPARE {label} port={} ({}) = {} events",
            ps.port,
            ps.role,
            ps.lines.len()
        ));
    }

    sup.shutdown();
    Ok(HaRun {
        save: save_lines,
        per_port,
    })
}

/// Fail fast if a long-lived process died on startup (usually a bind failure /
/// port race). Dumps the dead role's log path so the failure is diagnosable.
fn health_check(
    sup: &mut Supervisor,
    roles: &[&str],
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::thread::sleep(Duration::from_millis(400));
    for role in roles {
        if !sup.alive(role) {
            sup.logln(&format!("HEALTH_CHECK role={role} exited early"));
            return Err(format!(
                "ha {label}: `{role}` exited during startup — see {}",
                sup_log_dir(label).join("timeline.log").display()
            )
            .into());
        }
    }
    sup.logln("HEALTH_CHECK ok");
    Ok(())
}

/// Poll a single role until it exits or the run timeout elapses.
fn wait_for(
    sup: &mut Supervisor,
    role: &str,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = sup.start + RUN_TIMEOUT;
    while Instant::now() < deadline {
        if sup.poll(role).is_some() {
            return Ok(());
        }
        std::thread::sleep(POLL);
    }
    Err(format!(
        "ha {label}: `{role}` did not exit within {}s",
        RUN_TIMEOUT.as_secs()
    )
    .into())
}

struct Comparison {
    canon_len: usize,
    ha_len: usize,
    first_diff: Option<usize>,
    diff_snippet: String,
}

impl Comparison {
    fn diverged(&self) -> bool {
        self.first_diff.is_some()
    }
}

fn compare(
    work: &WorkDir,
    label: &str,
    canonical: &[String],
    ha: &[String],
) -> Result<Comparison, Box<dyn std::error::Error>> {
    if canonical == ha {
        return Ok(Comparison {
            canon_len: canonical.len(),
            ha_len: ha.len(),
            first_diff: None,
            diff_snippet: String::new(),
        });
    }
    let first_diff = canonical.iter().zip(ha.iter()).position(|(a, b)| a != b);
    let idx = first_diff.unwrap_or_else(|| canonical.len().min(ha.len()));

    let diff_path = work.iter_dir(label).join("diff.txt");
    let mut w = BufWriter::new(File::create(&diff_path)?);
    writeln!(w, "DIVERGENT: {label}")?;
    writeln!(w, "canonical: {} events", canonical.len())?;
    writeln!(w, "ha:        {} events", ha.len())?;
    writeln!(w, "first differing index: {idx}")?;
    writeln!(w)?;
    writeln!(w, "---- context (canonical | ha) ----")?;
    let lo = idx.saturating_sub(5);
    let hi = (idx + 5).min(canonical.len().max(ha.len()));
    let mut snippet = String::new();
    for i in lo..hi {
        let c = canonical
            .get(i)
            .cloned()
            .unwrap_or_else(|| "<missing>".into());
        let h = ha.get(i).cloned().unwrap_or_else(|| "<missing>".into());
        let mark = if c == h { "  " } else { "!=" };
        let lc = format!("{mark} [{i}] canon: {c}");
        let lh = format!("{mark}      ha:   {h}");
        writeln!(w, "{lc}")?;
        writeln!(w, "{lh}")?;
        snippet.push_str(&lc);
        snippet.push('\n');
        snippet.push_str(&lh);
        snippet.push('\n');
    }
    w.flush()?;

    Ok(Comparison {
        canon_len: canonical.len(),
        ha_len: ha.len(),
        first_diff: Some(idx),
        diff_snippet: snippet,
    })
}

/// How one pipeline's own egress relates to canonical.
enum PortMatch {
    /// Identical to canonical for the whole tape — the engine is deterministic.
    Full,
    /// A prefix of canonical: the pipeline was killed mid-tape, so its egress
    /// stops cleanly at index `k` (no divergence before it).
    Prefix(usize),
    /// Never produced anything (killed before any egress, or never connected).
    Empty,
    /// Genuinely diverges from canonical at `index` — an engine/harness bug,
    /// not a clean kill truncation.
    Divergent(usize),
}

fn classify_port(canonical: &[String], port: &[String]) -> PortMatch {
    if port.is_empty() {
        return PortMatch::Empty;
    }
    if port == canonical {
        return PortMatch::Full;
    }
    let k = port.len();
    if k <= canonical.len() && canonical[..k] == port[..] {
        return PortMatch::Prefix(k);
    }
    let idx = canonical
        .iter()
        .zip(port.iter())
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| canonical.len().min(port.len()));
    PortMatch::Divergent(idx)
}

fn port_match_text(canonical_len: usize, m: &PortMatch) -> String {
    match m {
        PortMatch::Full => format!("full canonical ({canonical_len} events) ✓"),
        PortMatch::Prefix(k) => format!("prefix of canonical[0..{k}] (truncated by a kill)"),
        PortMatch::Empty => "empty (killed before any egress / never connected)".into(),
        PortMatch::Divergent(i) => format!("DIVERGENT from canonical at {i} (engine/harness bug)"),
    }
}

/// Everything the report needs for one kill iteration.
struct IterOutcome {
    label: String,
    schedule: Vec<(Duration, String)>,
    comparison: Comparison,
    ports: Vec<PortStream>,
}

fn write_report(
    work: &WorkDir,
    canonical: &[String],
    baseline: &HaRun,
    baseline_cmp: &Comparison,
    outcomes: &[IterOutcome],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let canonical_len = canonical.len();
    let path = work.dir.join("REPORT.md");
    let mut s = String::new();

    s.push_str("# HA test report\n\n");

    // Baseline.
    s.push_str("## Baseline (no kills)\n\n");
    if let Some(i) = baseline_cmp.first_diff {
        s.push_str(&format!(
            "DIVERGENT at {i} (canonical {canonical_len} vs baseline {})\n",
            baseline_cmp.ha_len
        ));
    } else {
        s.push_str(&format!("{canonical_len} events — MATCH canonical ✓\n"));
    }
    for ps in &baseline.per_port {
        let m = classify_port(canonical, &ps.lines);
        s.push_str(&format!(
            "- port {} ({}): {} events → {}\n",
            ps.port,
            ps.role,
            ps.lines.len(),
            port_match_text(canonical_len, &m)
        ));
    }
    s.push('\n');

    // Per-iteration.
    let diverged: usize = outcomes.iter().filter(|o| o.comparison.diverged()).count();
    s.push_str(&format!(
        "## Kill iterations: {diverged}/{} diverged\n\n",
        outcomes.len()
    ));

    for o in outcomes {
        if o.comparison.diverged() {
            s.push_str(&format!("### {} — DIVERGENT\n\n", o.label));
        } else {
            s.push_str(&format!("### {} — MATCH ✓\n\n", o.label));
        }

        // Kill schedule.
        if o.schedule.is_empty() {
            s.push_str("**Kill schedule:** none\n\n");
        } else {
            s.push_str("**Kill schedule:**\n\n| t+ | role |\n|---:|---|\n");
            for (t, role) in &o.schedule {
                s.push_str(&format!("| {}ms | {} |\n", t.as_millis(), role));
            }
            s.push('\n');
        }

        // Forwarded egress vs canonical.
        if let Some(i) = o.comparison.first_diff {
            s.push_str(&format!(
                "**Forwarded egress vs canonical (`--save`):** DIVERGENT — \
                canonical {} events vs ha {} events; first diff at index {i}.\n\n",
                o.comparison.canon_len, o.comparison.ha_len
            ));
        } else {
            s.push_str(&format!(
                "**Forwarded egress vs canonical (`--save`):** MATCH ({} events) ✓\n\n",
                o.comparison.ha_len
            ));
        }

        // Per-pipeline egress vs canonical.
        s.push_str("**Per-pipeline egress vs canonical (`--record`):**\n\n");
        if o.ports.is_empty() {
            s.push_str("- (no `--record` captured)\n");
        }
        for ps in &o.ports {
            let m = classify_port(canonical, &ps.lines);
            s.push_str(&format!(
                "- port {} ({}): {} events → {}\n",
                ps.port,
                ps.role,
                ps.lines.len(),
                port_match_text(canonical_len, &m)
            ));
        }
        s.push('\n');

        // Diff.
        if o.comparison.diverged() {
            s.push_str("**Diff (forwarded `--save` vs canonical):**\n\n");
            s.push_str("```diff\n");
            s.push_str(&o.comparison.diff_snippet);
            s.push_str("```\n\n");
            s.push_str(&format!(
                "Full diff: `{}/diff.txt`\n\n",
                work.iter_dir(&o.label)
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| o.label.clone())
            ));
        }
    }

    fs::write(&path, s)?;
    Ok(path)
}

/// Grab `n` ephemeral ports
fn free_ports(n: usize) -> Vec<u16> {
    (0..n)
        .map(|_| {
            let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
            l.local_addr().expect("local addr").port()
        })
        .collect()
}

fn read_lines(path: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    Ok(BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter(|l| !l.is_empty())
        .collect())
}

fn parse_record(
    path: &Path,
    roles: &[(u16, &str)],
) -> Result<Vec<PortStream>, Box<dyn std::error::Error>> {
    let mut by_port: HashMap<u16, Vec<String>> = HashMap::new();
    if path.is_file() {
        for line in read_lines(path)? {
            let Some(rest) = line.strip_prefix("PORT ") else {
                continue;
            };
            let Some((port_s, json)) = rest.split_once(':') else {
                continue;
            };
            let Ok(port) = port_s.trim().parse::<u16>() else {
                continue;
            };
            by_port
                .entry(port)
                .or_default()
                .push(json.trim_start().to_string());
        }
    }
    Ok(roles
        .iter()
        .map(|(port, role)| PortStream {
            port: *port,
            role: role.to_string(),
            lines: by_port.remove(port).unwrap_or_default(),
        })
        .collect())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn sup_log_dir(label: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/ha-test")
        .join(label)
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn u32(&mut self, n: u32) -> u32 {
        // Numerical Recipes LCG; take the high bits for better uniformity.
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as u32) % n
    }
}

fn write_synthetic_tape(path: &Path, size: usize, seed: u64) -> std::io::Result<()> {
    let mut rng = Rng::new(seed);
    let mut w = BufWriter::new(File::create(path)?);
    let mid: i64 = 100 * SCALE; // 100.00000000
    let mut id: u64 = 1;
    let mut live: Vec<u64> = Vec::new();

    for _ in 0..size {
        let r = rng.u32(100);
        if r < 5 && !live.is_empty() {
            // Cancel a resting limit order (no-op if it already matched away).
            let idx = rng.u32(live.len() as u32) as usize;
            let oid = live.swap_remove(idx);
            let msg = OrderMsg::cancel(oid);
            writeln!(w, "{}", serde_json::to_string(&msg).expect("serialize"))?;
        } else if r < 12 {
            // Market order → crosses the book and emits trades.
            let side = if rng.u32(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let qty = (1 + rng.u32(10) as u64) * SCALE_U;
            let msg = OrderMsg::new_market(id, side, qty);
            writeln!(w, "{}", serde_json::to_string(&msg).expect("serialize"))?;
            id += 1;
        } else {
            // Resting limit order around the mid → builds depth, emits BBO.
            let side = if rng.u32(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let offset = (1 + rng.u32(20) as i64) * SCALE;
            let price = if side == Side::Buy {
                mid - offset
            } else {
                mid + offset
            };
            let qty = (1 + rng.u32(10) as u64) * SCALE_U;
            let msg = OrderMsg::new_limit(id, side, price, qty);
            writeln!(w, "{}", serde_json::to_string(&msg).expect("serialize"))?;
            live.push(id);
            id += 1;
        }
    }
    w.flush()?;
    Ok(())
}
