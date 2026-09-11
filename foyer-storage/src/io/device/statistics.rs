// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use fastant::{Atomic, Instant};

use crate::Throttle;

#[derive(Debug)]
struct Metric {
    value: AtomicUsize,
    throttle: f64,
    quota: AtomicU64,
    update: Atomic,
}

impl Metric {
    fn new(throttle: f64) -> Self {
        Self {
            value: AtomicUsize::new(0),
            throttle,
            quota: AtomicU64::new(0.0f64.to_bits()),
            update: Atomic::new(Instant::now()),
        }
    }

    fn load(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }

    fn record(&self, value: usize) {
        self.value.fetch_add(value, Ordering::Relaxed);
        // If throttle is set, update the quota.
        if self.throttle != 0.0 {
            let dec = value as f64;
            let mut prev = self.quota.load(Ordering::Relaxed);
            loop {
                let new = (f64::from_bits(prev) - dec).to_bits();
                match self
                    .quota
                    .compare_exchange_weak(prev, new, Ordering::Relaxed, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(actual) => prev = actual,
                }
            }
        }
    }

    /// Get the nearest time to retry to check if there is quota.
    ///
    /// Return `Duration::ZERO` if no need to wait.
    fn throttle(&self) -> Duration {
        // If throttle is not set, no need to wait.
        if self.throttle == 0.0 {
            return Duration::ZERO;
        }

        let now = Instant::now();
        let update = self.update.load(Ordering::Relaxed);

        let dur = now.duration_since(update).as_secs_f64();
        let fill = dur * self.throttle;

        let mut prev = self.quota.load(Ordering::Relaxed);
        loop {
            let quota = f64::min(self.throttle, f64::from_bits(prev) + fill);
            match self
                .quota
                .compare_exchange_weak(prev, quota.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.update.fetch_max(now, Ordering::Relaxed);
                    if quota >= 0.0 {
                        return Duration::ZERO;
                    } else {
                        return Duration::from_secs_f64(-quota / self.throttle);
                    }
                }
                Err(actual) => prev = actual,
            }
        }
    }
}

/// The statistics of the device.
#[derive(Debug)]
pub struct Statistics {
    throttle: Throttle,

    disk_write_bytes: Metric,
    disk_read_bytes: Metric,
    disk_write_ios: Metric,
    disk_read_ios: Metric,
}

impl Statistics {
    /// Create a new statistics.
    pub fn new(throttle: Throttle) -> Self {
        let disk_write_bytes = Metric::new(throttle.write_throughput.map(|v| v.get()).unwrap_or_default() as f64);
        let disk_read_bytes = Metric::new(throttle.read_throughput.map(|v| v.get()).unwrap_or_default() as f64);
        let disk_write_ios = Metric::new(throttle.write_iops.map(|v| v.get()).unwrap_or_default() as f64);
        let disk_read_ios = Metric::new(throttle.read_iops.map(|v| v.get()).unwrap_or_default() as f64);
        Self {
            throttle,
            disk_write_bytes,
            disk_read_bytes,
            disk_write_ios,
            disk_read_ios,
        }
    }

    /// Get the disk cache written bytes.
    pub fn disk_write_bytes(&self) -> usize {
        self.disk_write_bytes.load()
    }

    /// Get the disk cache read bytes.
    pub fn disk_read_bytes(&self) -> usize {
        self.disk_read_bytes.load()
    }

    /// Get the disk cache written ios.
    pub fn disk_write_ios(&self) -> usize {
        self.disk_write_ios.load()
    }

    /// Get the disk cache read bytes.
    pub fn disk_read_ios(&self) -> usize {
        self.disk_read_ios.load()
    }

    /// Record the write IO and update the statistics.
    pub fn record_disk_write(&self, bytes: usize) {
        self.disk_write_bytes.record(bytes);
        self.disk_write_ios.record(self.throttle.iops_counter.count(bytes));
    }

    /// Record the read IO and update the statistics.
    pub fn record_disk_read(&self, bytes: usize) {
        self.disk_read_bytes.record(bytes);
        self.disk_read_ios.record(self.throttle.iops_counter.count(bytes));
    }

    /// Get the nearest time to retry to check if there is quota for read ops.
    ///
    /// Return `Duration::ZERO` if no need to wait.
    pub fn read_throttle(&self) -> Duration {
        std::cmp::max(self.disk_read_bytes.throttle(), self.disk_read_ios.throttle())
    }

    /// Get the nearest time to retry to check if there is quota for write ops.
    ///
    /// Return `Duration::ZERO` if no need to wait.
    pub fn write_throttle(&self) -> Duration {
        std::cmp::max(self.disk_write_bytes.throttle(), self.disk_write_ios.throttle())
    }

    /// Check if the read ops are throttled.
    pub fn is_read_throttled(&self) -> bool {
        self.read_throttle() > Duration::ZERO
    }

    /// Check if the write ops are throttled.
    pub fn is_write_throttled(&self) -> bool {
        self.write_throttle() > Duration::ZERO
    }

    /// Get the throttle configuration.
    pub fn throttle(&self) -> &Throttle {
        &self.throttle
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use super::{Metric, Statistics};
    use crate::Throttle;

    // === Metric: lossless quota preservation (the core bug) ===

    /// A single IO under a 1 iops/s limit must keep the throttle held for ~1 second.
    /// Rapid re-probes — which is exactly what the production read path does, since
    /// it discards the returned `Duration` and polls again on the next `load` —
    /// must NOT erode the debt.
    #[test]
    fn test_iops_single_io_debt_survives_rapid_probes() {
        let metric = Metric::new(1.0); // 1 iops/s
        metric.record(1); // quota -> -1.0

        // 2000 probes span microseconds to low milliseconds; with a 1-second debt,
        // every single probe must still report throttled. The truncation bug would
        // wipe the debt on the first probe (storing `-0.999... as isize = 0`).
        for i in 0..2000 {
            assert!(
                metric.throttle() > Duration::ZERO,
                "probe {i} must still be throttled while the 1-second debt persists"
            );
        }
    }

    /// The fix must not *over*-throttle: after enough wall-clock time has elapsed,
    /// the token bucket refills and the throttle releases.
    #[test]
    fn test_iops_debt_releases_after_refill_window() {
        let metric = Metric::new(2.0); // 2 iops/s
        metric.record(1); // quota -> -1.0 (holds for ~0.5s)
        assert!(metric.throttle() > Duration::ZERO);

        // Sleep past the refill window with a comfortable margin.
        thread::sleep(Duration::from_millis(700));
        assert_eq!(
            metric.throttle(),
            Duration::ZERO,
            "throttle must release once the debt has refilled"
        );
    }

    /// Directly verify that a fractional negative debt is preserved losslessly
    /// across a probe — the exact behavior the `as isize` truncation destroyed.
    #[test]
    fn test_fractional_negative_debt_preserved_losslessly() {
        let metric = Metric::new(1.0);
        // `-0.999` would be truncated to `0` by the old `as isize` cast.
        metric.quota.store((-0.999_f64).to_bits(), Ordering::Relaxed);
        assert!(
            metric.throttle() > Duration::ZERO,
            "fractional debt must remain throttled"
        );
        let stored = f64::from_bits(metric.quota.load(Ordering::Relaxed));
        assert!(
            stored < -0.5,
            "fractional debt must be preserved losslessly (got {stored})"
        );
    }

    // === Metric: CAS loop correctness under concurrency ===

    /// Concurrent `record()` and `throttle()` must not corrupt the bit-encoded
    /// quota (no NaN / infinity) and must not lose records.
    #[test]
    fn test_concurrent_record_and_throttle_is_consistent() {
        let metric = Arc::new(Metric::new(1000.0)); // 1000 iops/s
        let corrupted = Arc::new(AtomicUsize::new(0));
        let nthreads = 8;
        let per_thread = 2000;

        let mut handles = Vec::new();
        for _ in 0..nthreads {
            let m = metric.clone();
            let c = corrupted.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..per_thread {
                    // The production interleaving: probe, then record on completion.
                    let _ = m.throttle();
                    m.record(1);
                    let q = f64::from_bits(m.quota.load(Ordering::Relaxed));
                    if !q.is_finite() {
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            corrupted.load(Ordering::Relaxed),
            0,
            "quota must never become non-finite"
        );
        assert_eq!(
            metric.load(),
            nthreads * per_thread,
            "cumulative counter must equal total records"
        );
    }

    // === Statistics: production-pattern admission rate respects the configured limit ===

    /// Reproduces the production call pattern: probe -> (if admitted) simulate
    /// device latency -> record on the completion callback. With the truncation
    /// bug, every configured limit admitted ~500/1000 (the coin-flip gate). With
    /// the fix, the configured rate must dominate.
    fn simulate_admission(limit: usize) -> usize {
        let stats = Statistics::new(Throttle::new().with_read_iops(limit));
        let mut admitted = 0;
        for _ in 0..1000 {
            if !stats.is_read_throttled() {
                admitted += 1;
                // Simulate device read latency, then record on completion callback.
                thread::sleep(Duration::from_micros(50));
                stats.record_disk_read(4096);
            }
        }
        admitted
    }

    #[test]
    fn test_production_pattern_admission_respects_configured_limit() {
        // The buggy behavior admitted ~500/1000 regardless of the configured limit.
        // The fix must make the configured limit matter: a 1-iops/s limit admits
        // ~1 (only the initial free credit), while a high limit admits many.
        let low = simulate_admission(1);
        assert!(
            low < 100,
            "1 iops/s must not admit a large fraction of 1000 rapid attempts (got {low})"
        );
    }

    // === E2E: real device read path with a throttled FsDevice ===
    //
    // These drive the *real* read path the throttle protects: a `MonitoredIoEngine`
    // over a `FsDevice` (psync), where each completed read calls
    // `statistics.record_disk_read()` via the IO completion callback (monitor.rs),
    // and admission is gated by `statistics.is_read_throttled()` — exactly the
    // production call pattern in `engine.rs:630`.
    //
    // Marked `#[ignore]` because they are wall-clock rate-accuracy tests against a
    // real filesystem device (cf. the old `IoThrottler` rate tests, which were also
    // `#[ignore]`d). Run explicitly via:
    //   cargo nextest run -p foyer-storage --run-ignored all -E "test(io::device::statistics::tests::test_e2e_)"

    use foyer_common::{metrics::Metrics, spawn::Spawner};
    use rand::{Fill, rng};
    use tempfile::tempdir;

    use crate::io::{
        PAGE,
        bytes::IoSliceMut,
        device::{DeviceBuilder, fs::FsDeviceBuilder},
        engine::{
            IoEngine, IoEngineBuildContext, IoEngineConfig, monitor::MonitoredIoEngine, psync::PsyncIoEngineConfig,
        },
    };

    const E2E_MIB: usize = 1024 * 1024;

    /// Drive the real read path against a throttled `FsDevice` for `window` and
    /// return `(admitted_reads, total_attempts)`.
    async fn e2e_real_read_path(read_iops: usize, window: Duration) -> (usize, usize) {
        let dir = tempdir().unwrap();
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(16 * E2E_MIB)
            .with_throttle(Throttle::new().with_read_iops(read_iops))
            .build()
            .unwrap();
        device.create_partition(4 * E2E_MIB).unwrap();
        let partition = device.partition(0);
        let stats = partition.statistics().clone();

        let io_engine = PsyncIoEngineConfig::new()
            .boxed()
            .build(IoEngineBuildContext {
                spawner: Spawner::current(),
            })
            .await
            .unwrap();
        let engine = MonitoredIoEngine::new(io_engine, Arc::new(Metrics::noop()));

        // Seed the block at offset 0 so reads return data and fire the completion callback.
        let mut seed = Box::new(IoSliceMut::new(PAGE));
        Fill::fill_slice(&mut seed[..], &mut rng());
        let (_, wres) = engine.write(seed, partition.as_ref(), 0).await;
        wres.unwrap();

        let start = Instant::now();
        let mut attempts = 0usize;
        let mut admitted = 0usize;
        while start.elapsed() < window {
            attempts += 1;
            if !stats.is_read_throttled() {
                let buf = Box::new(IoSliceMut::new(PAGE));
                let (_, rres) = engine.read(buf, partition.as_ref(), 0).await;
                rres.unwrap();
                admitted += 1;
            } else {
                // Cooperative yield so the tokio runtime stays responsive while throttled.
                tokio::task::yield_now().await;
            }
        }
        // The completion callback fires before `.await` returns (engine/mod.rs), so
        // every admitted read must have been recorded in the statistics.
        assert_eq!(
            stats.disk_read_ios(),
            admitted,
            "disk_read_ios must equal admitted reads after the window"
        );
        (admitted, attempts)
    }

    /// A 10 iops/s limit must cap admitted real reads to ~10/sec, not the
    /// ~attempt-rate/2 the truncation bug produced.
    #[ignore = "wall-clock rate-accuracy test against a real fs device"]
    #[test_log::test(tokio::test)]
    async fn test_e2e_real_read_path_low_limit_enforced() {
        let (admitted, attempts) = e2e_real_read_path(10, Duration::from_millis(500)).await;
        assert!(attempts > 100, "sanity: loop must make many attempts (got {attempts})");
        // 10 iops/s over 0.5s => ~5 admitted (capped at ~10/sec). The bug admitted ~attempts/2.
        assert!(
            admitted <= 30,
            "10 iops/s must not admit more than ~30 reads in 0.5s; the configured limit must hold (got {admitted}, attempts {attempts})"
        );
    }

    /// A 100 iops/s limit must admit ~50 reads in 0.5s (more than the 10 iops/s
    /// case), but still be capped — proving the configured rate now governs.
    #[ignore = "wall-clock rate-accuracy test against a real fs device"]
    #[test_log::test(tokio::test)]
    async fn test_e2e_real_read_path_high_limit_scales() {
        let (admitted, attempts) = e2e_real_read_path(100, Duration::from_millis(500)).await;
        assert!(attempts > 100, "sanity: loop must make many attempts (got {attempts})");
        assert!(
            (35..=120).contains(&admitted),
            "100 iops/s over 0.5s should admit ~50, capped under ~120 and above ~35; got {admitted}, attempts {attempts}"
        );
    }
}
