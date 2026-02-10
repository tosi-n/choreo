//! Metrics - OpenTelemetry and Prometheus compatible metrics export
//!
//! Tracks:
//! - Function execution counts, durations, and outcomes
//! - Step execution metrics
//! - Queue depths and wait times
//! - Concurrency utilization

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metrics registry
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Arc<Counter>>>,
    gauges: RwLock<HashMap<String, Arc<Gauge>>>,
    histograms: RwLock<HashMap<String, Arc<Histogram>>>,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a counter
    pub fn counter(&self, name: &str) -> Arc<Counter> {
        {
            let counters = self.counters.read();
            if let Some(c) = counters.get(name) {
                return c.clone();
            }
        }

        let mut counters = self.counters.write();
        counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Counter::new(name)))
            .clone()
    }

    /// Get or create a gauge
    pub fn gauge(&self, name: &str) -> Arc<Gauge> {
        {
            let gauges = self.gauges.read();
            if let Some(g) = gauges.get(name) {
                return g.clone();
            }
        }

        let mut gauges = self.gauges.write();
        gauges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Gauge::new(name)))
            .clone()
    }

    /// Get or create a histogram
    pub fn histogram(&self, name: &str) -> Arc<Histogram> {
        {
            let histograms = self.histograms.read();
            if let Some(h) = histograms.get(name) {
                return h.clone();
            }
        }

        let mut histograms = self.histograms.write();
        histograms
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Histogram::new(name)))
            .clone()
    }

    /// Export all metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Counters
        for (name, counter) in self.counters.read().iter() {
            output.push_str(&format!(
                "# TYPE {} counter\n{} {}\n",
                name,
                name,
                counter.get()
            ));
        }

        // Gauges
        for (name, gauge) in self.gauges.read().iter() {
            output.push_str(&format!(
                "# TYPE {} gauge\n{} {}\n",
                name,
                name,
                gauge.get()
            ));
        }

        // Histograms
        for (name, histogram) in self.histograms.read().iter() {
            let snapshot = histogram.snapshot();
            output.push_str(&format!("# TYPE {} histogram\n", name));
            output.push_str(&format!("{}_count {}\n", name, snapshot.count));
            output.push_str(&format!("{}_sum {}\n", name, snapshot.sum));

            // Buckets
            for (le, count) in &snapshot.buckets {
                output.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", name, le, count));
            }
            output.push_str(&format!(
                "{}_bucket{{le=\"+Inf\"}} {}\n",
                name, snapshot.count
            ));
        }

        output
    }

    /// Export metrics as JSON
    pub fn export_json(&self) -> serde_json::Value {
        let mut metrics = serde_json::Map::new();

        // Counters
        let mut counters = serde_json::Map::new();
        for (name, counter) in self.counters.read().iter() {
            counters.insert(name.clone(), serde_json::json!(counter.get()));
        }
        metrics.insert("counters".to_string(), serde_json::Value::Object(counters));

        // Gauges
        let mut gauges = serde_json::Map::new();
        for (name, gauge) in self.gauges.read().iter() {
            gauges.insert(name.clone(), serde_json::json!(gauge.get()));
        }
        metrics.insert("gauges".to_string(), serde_json::Value::Object(gauges));

        // Histograms
        let mut histograms = serde_json::Map::new();
        for (name, histogram) in self.histograms.read().iter() {
            let snapshot = histogram.snapshot();
            histograms.insert(
                name.clone(),
                serde_json::json!({
                    "count": snapshot.count,
                    "sum": snapshot.sum,
                    "mean": snapshot.mean(),
                    "min": snapshot.min,
                    "max": snapshot.max,
                    "p50": snapshot.percentile(50.0),
                    "p95": snapshot.percentile(95.0),
                    "p99": snapshot.percentile(99.0),
                }),
            );
        }
        metrics.insert(
            "histograms".to_string(),
            serde_json::Value::Object(histograms),
        );

        serde_json::Value::Object(metrics)
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Counter - monotonically increasing value
pub struct Counter {
    #[allow(dead_code)]
    name: String,
    value: AtomicU64,
}

impl Counter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: AtomicU64::new(0),
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

/// Gauge - value that can go up or down
pub struct Gauge {
    #[allow(dead_code)]
    name: String,
    value: AtomicU64,
}

impl Gauge {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: AtomicU64::new(0),
        }
    }

    pub fn set(&self, v: u64) {
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

/// Histogram - tracks distribution of values
pub struct Histogram {
    #[allow(dead_code)]
    name: String,
    values: RwLock<Vec<f64>>,
    count: AtomicU64,
    sum: RwLock<f64>,
    /// Bucket boundaries
    buckets: Vec<f64>,
}

impl Histogram {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: RwLock::new(Vec::new()),
            count: AtomicU64::new(0),
            sum: RwLock::new(0.0),
            buckets: vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        }
    }

    pub fn with_buckets(name: &str, buckets: Vec<f64>) -> Self {
        Self {
            name: name.to_string(),
            values: RwLock::new(Vec::new()),
            count: AtomicU64::new(0),
            sum: RwLock::new(0.0),
            buckets,
        }
    }

    pub fn observe(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        *self.sum.write() += value;
        self.values.write().push(value);
    }

    /// Observe a duration
    pub fn observe_duration(&self, duration: Duration) {
        self.observe(duration.as_secs_f64());
    }

    /// Time a closure and record the duration
    pub fn time<F, T>(&self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let start = Instant::now();
        let result = f();
        self.observe_duration(start.elapsed());
        result
    }

    /// Get a snapshot of the histogram
    pub fn snapshot(&self) -> HistogramSnapshot {
        let values = self.values.read();
        let count = values.len() as u64;
        let sum = *self.sum.read();

        if count == 0 {
            return HistogramSnapshot {
                count: 0,
                sum: 0.0,
                min: 0.0,
                max: 0.0,
                buckets: self.buckets.iter().map(|b| (*b, 0)).collect(),
                values: vec![],
            };
        }

        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        // Calculate bucket counts
        let bucket_counts: Vec<(f64, u64)> = self
            .buckets
            .iter()
            .map(|b| (*b, values.iter().filter(|v| **v <= *b).count() as u64))
            .collect();

        HistogramSnapshot {
            count,
            sum,
            min,
            max,
            buckets: bucket_counts,
            values: values.clone(),
        }
    }
}

/// Snapshot of histogram state
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub buckets: Vec<(f64, u64)>,
    values: Vec<f64>,
}

impl HistogramSnapshot {
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    pub fn percentile(&self, p: f64) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).floor() as usize;
        sorted[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_new() {
        let counter = Counter::new("test_counter");
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_counter_inc() {
        let counter = Counter::new("test_counter");
        counter.inc();
        assert_eq!(counter.get(), 1);
        counter.inc();
        assert_eq!(counter.get(), 2);
    }

    #[test]
    fn test_counter_add() {
        let counter = Counter::new("test_counter");
        counter.add(5);
        assert_eq!(counter.get(), 5);
        counter.add(10);
        assert_eq!(counter.get(), 15);
    }

    #[test]
    fn test_counter_reset() {
        let counter = Counter::new("test_counter");
        counter.add(100);
        counter.reset();
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_gauge_new() {
        let gauge = Gauge::new("test_gauge");
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn test_gauge_set() {
        let gauge = Gauge::new("test_gauge");
        gauge.set(100);
        assert_eq!(gauge.get(), 100);
    }

    #[test]
    fn test_gauge_inc() {
        let gauge = Gauge::new("test_gauge");
        gauge.inc();
        assert_eq!(gauge.get(), 1);
    }

    #[test]
    fn test_gauge_dec() {
        let gauge = Gauge::new("test_gauge");
        gauge.set(10);
        gauge.dec();
        assert_eq!(gauge.get(), 9);
    }

    #[test]
    fn test_gauge_reset() {
        let gauge = Gauge::new("test_gauge");
        gauge.set(100);
        gauge.reset();
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn test_histogram_new() {
        let histogram = Histogram::new("test_histogram");
        assert_eq!(histogram.snapshot().count, 0);
    }

    #[test]
    fn test_histogram_observe() {
        let histogram = Histogram::new("test_histogram");
        histogram.observe(100.0);
        histogram.observe(200.0);
        histogram.observe(300.0);

        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 3);
        assert!((snapshot.min - 100.0).abs() < 0.001);
        assert!((snapshot.max - 300.0).abs() < 0.001);
    }

    #[test]
    fn test_histogram_observe_duration() {
        let histogram = Histogram::new("test_duration");
        histogram.observe_duration(Duration::from_millis(100));
        histogram.observe_duration(Duration::from_millis(200));

        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 2);
    }

    #[test]
    fn test_histogram_time() {
        let histogram = Histogram::new("test_time");
        let result = histogram.time(|| {
            std::thread::sleep(Duration::from_millis(10));
            "done"
        });

        assert_eq!(result, "done");
        assert!(histogram.snapshot().count >= 1);
    }

    #[test]
    fn test_histogram_snapshot_mean() {
        let histogram = Histogram::new("test_mean");
        histogram.observe(100.0);
        histogram.observe(200.0);
        histogram.observe(300.0);

        let snapshot = histogram.snapshot();
        assert!((snapshot.mean() - 200.0).abs() < 0.001);
    }

    #[test]
    fn test_histogram_snapshot_percentile() {
        let histogram = Histogram::new("test_percentile");
        for i in 1..=100 {
            histogram.observe((i * 10) as f64);
        }

        let snapshot = histogram.snapshot();
        // 50th percentile should be around 500
        let p50 = snapshot.percentile(50.0);
        assert!(p50 >= 450.0 && p50 <= 550.0);

        // 95th percentile should be around 950
        let p95 = snapshot.percentile(95.0);
        assert!(p95 >= 900.0 && p95 <= 1000.0);
    }

    #[test]
    fn test_histogram_snapshot_empty() {
        let histogram = Histogram::new("test_empty");
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 0);
        assert_eq!(snapshot.mean(), 0.0);
        assert_eq!(snapshot.percentile(50.0), 0.0);
    }

    #[test]
    fn test_metrics_registry_new() {
        let registry = MetricsRegistry::new();
        let _counter = registry.counter("test");
        let _gauge = registry.gauge("test");
        let _histogram = registry.histogram("test");
    }

    #[test]
    fn test_metrics_registry_get_or_create() {
        let registry = MetricsRegistry::new();
        let counter1 = registry.counter("my_counter");
        let counter2 = registry.counter("my_counter");

        // Same counter should be returned
        counter1.inc();
        assert_eq!(counter2.get(), 1);
    }

    #[test]
    fn test_metrics_registry_export_prometheus() {
        let registry = MetricsRegistry::new();
        let counter = registry.counter("test_counter");
        counter.inc();
        counter.inc();

        let output = registry.export_prometheus();
        assert!(output.contains("test_counter"));
        assert!(output.contains("2"));
    }

    #[test]
    fn test_choreo_metrics_new() {
        let metrics = ChoreoMetrics::new();
        metrics.runs_started.inc();
        metrics.steps_executed.inc();
        metrics.queue_depth.set(5);
        metrics.active_runs.set(10);

        assert_eq!(metrics.runs_started.get(), 1);
        assert_eq!(metrics.steps_executed.get(), 1);
        assert_eq!(metrics.queue_depth.get(), 5);
        assert_eq!(metrics.active_runs.get(), 10);
    }

    #[test]
    fn test_choreo_metrics_counters() {
        let metrics = ChoreoMetrics::new();

        metrics.runs_completed.add(10);
        metrics.runs_failed.add(2);

        assert_eq!(metrics.runs_completed.get(), 10);
        assert_eq!(metrics.runs_failed.get(), 2);
    }

    #[test]
    fn test_choreo_metrics_histograms() {
        let metrics = ChoreoMetrics::new();

        (*metrics.run_duration).observe(1.0);
        (*metrics.run_duration).observe(2.0);
        (*metrics.step_duration).observe(0.5);

        assert_eq!(metrics.run_duration.snapshot().count, 2);
        assert_eq!(metrics.step_duration.snapshot().count, 1);
    }

    #[test]
    fn test_metrics_registry_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let registry = Arc::new(MetricsRegistry::new());
        let counter = registry.counter("concurrent_counter");

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let registry = Arc::clone(&registry);
                thread::spawn(move || {
                    for _ in 0..100 {
                        registry.counter("concurrent_counter").inc();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // All threads should have incremented the counter
        assert_eq!(counter.get(), 1000);
    }
}

// === Choreo-specific metrics ===

/// Pre-defined metrics for Choreo
pub struct ChoreoMetrics {
    pub registry: Arc<MetricsRegistry>,

    // Function metrics
    pub runs_started: Arc<Counter>,
    pub runs_completed: Arc<Counter>,
    pub runs_failed: Arc<Counter>,
    pub run_duration: Arc<Histogram>,

    // Step metrics
    pub steps_executed: Arc<Counter>,
    pub steps_cached: Arc<Counter>,
    pub step_duration: Arc<Histogram>,

    // Queue metrics
    pub queue_depth: Arc<Gauge>,
    pub queue_wait_time: Arc<Histogram>,

    // Concurrency metrics
    pub active_runs: Arc<Gauge>,
    pub concurrency_rejections: Arc<Counter>,
}

impl ChoreoMetrics {
    pub fn new() -> Self {
        let registry = Arc::new(MetricsRegistry::new());

        Self {
            runs_started: registry.counter("choreo_runs_started_total"),
            runs_completed: registry.counter("choreo_runs_completed_total"),
            runs_failed: registry.counter("choreo_runs_failed_total"),
            run_duration: registry.histogram("choreo_run_duration_seconds"),
            steps_executed: registry.counter("choreo_steps_executed_total"),
            steps_cached: registry.counter("choreo_steps_cached_total"),
            step_duration: registry.histogram("choreo_step_duration_seconds"),
            queue_depth: registry.gauge("choreo_queue_depth"),
            queue_wait_time: registry.histogram("choreo_queue_wait_seconds"),
            active_runs: registry.gauge("choreo_active_runs"),
            concurrency_rejections: registry.counter("choreo_concurrency_rejections_total"),
            registry,
        }
    }
}

impl Default for ChoreoMetrics {
    fn default() -> Self {
        Self::new()
    }
}
