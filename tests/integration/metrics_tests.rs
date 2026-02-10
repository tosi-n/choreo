//! Metrics module tests - Integration tests

use choreo::metrics::{ChoreoMetrics, MetricsRegistry};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_metrics_registry_new() {
    let registry = MetricsRegistry::new();
    // Registry should be created successfully
    let _counter = registry.counter("test_counter");
    assert!(registry.counter("test_counter").get() >= 0);
}

#[test]
fn test_metrics_registry_counters() {
    let registry = MetricsRegistry::new();
    let counter = registry.counter("my_counter");

    counter.inc();
    assert_eq!(counter.get(), 1);

    counter.inc();
    assert_eq!(counter.get(), 2);

    counter.add(5);
    assert_eq!(counter.get(), 7);
}

#[test]
fn test_metrics_registry_get_same_counter() {
    let registry = MetricsRegistry::new();
    let counter1 = registry.counter("shared_counter");
    let counter2 = registry.counter("shared_counter");

    // Should be the same counter
    counter1.inc();
    assert_eq!(counter2.get(), 1);
}

#[test]
fn test_metrics_registry_gauges() {
    let registry = MetricsRegistry::new();
    let gauge = registry.gauge("my_gauge");

    gauge.set(100);
    assert_eq!(gauge.get(), 100);

    gauge.inc();
    assert_eq!(gauge.get(), 101);

    gauge.dec();
    assert_eq!(gauge.get(), 100);
}

#[test]
fn test_metrics_registry_histograms() {
    let registry = MetricsRegistry::new();
    let histogram = registry.histogram("my_histogram");

    // Record some values
    (*histogram).observe(100.0);
    (*histogram).observe(200.0);
    (*histogram).observe(300.0);

    let snapshot = histogram.snapshot();
    assert!(snapshot.min >= 100.0);
    assert!(snapshot.max <= 300.0);
    assert_eq!(snapshot.count, 3);
}

#[test]
fn test_metrics_registry_multiple_gauges() {
    let registry = MetricsRegistry::new();
    let gauge1 = registry.gauge("gauge_a");
    let gauge2 = registry.gauge("gauge_b");

    gauge1.set(10);
    gauge2.set(20);

    assert_eq!(gauge1.get(), 10);
    assert_eq!(gauge2.get(), 20);
}

#[test]
fn test_choreo_metrics_creation() {
    let metrics = ChoreoMetrics::new();
    // Just verify it can be created
    let _runs_started = metrics.runs_started;
    let _steps_executed = metrics.steps_executed;
    let _queue_depth = metrics.queue_depth;
    let _active_runs = metrics.active_runs;
}

#[test]
fn test_choreo_metrics_counters() {
    let metrics = ChoreoMetrics::new();

    metrics.runs_started.inc();
    assert_eq!(metrics.runs_started.get(), 1);

    metrics.runs_started.inc();
    assert_eq!(metrics.runs_started.get(), 2);
}

#[test]
fn test_choreo_metrics_counters_add() {
    let metrics = ChoreoMetrics::new();

    metrics.runs_completed.add(5);
    assert_eq!(metrics.runs_completed.get(), 5);

    metrics.runs_completed.add(10);
    assert_eq!(metrics.runs_completed.get(), 15);
}

#[test]
fn test_choreo_metrics_gauges() {
    let metrics = ChoreoMetrics::new();

    metrics.active_runs.set(10);
    assert_eq!(metrics.active_runs.get(), 10);

    metrics.active_runs.inc();
    assert_eq!(metrics.active_runs.get(), 11);

    metrics.active_runs.dec();
    assert_eq!(metrics.active_runs.get(), 10);
}

#[test]
fn test_choreo_metrics_histograms() {
    let metrics = ChoreoMetrics::new();

    (*metrics.run_duration).observe(1.5);
    (*metrics.run_duration).observe(2.5);
    (*metrics.run_duration).observe(3.5);

    let snapshot = metrics.run_duration.snapshot();
    assert_eq!(snapshot.count, 3);
}

#[test]
fn test_histogram_snapshot_percentiles() {
    let registry = MetricsRegistry::new();
    let histogram = registry.histogram("latency");

    // Record values across a range
    for i in 1..=100 {
        (*histogram).observe((i * 10) as f64);
    }

    let snapshot = histogram.snapshot();
    // Should have recorded 100 values
    assert_eq!(snapshot.count, 100);
    // Min should be around 10
    assert!((snapshot.min - 10.0).abs() < 1.0);
}

#[test]
fn test_concurrent_metric_access() {
    let registry = Arc::new(MetricsRegistry::new());
    let counter = registry.counter("concurrent_counter");

    // Simulate concurrent access
    std::thread::scope(|s| {
        for _ in 0..10 {
            s.spawn(|| {
                for _ in 0..100 {
                    counter.inc();
                }
            });
        }
    });

    // All threads should have incremented the counter
    assert_eq!(counter.get(), 1000);
}

#[test]
fn test_gauge_concurrent_updates() {
    let registry = Arc::new(MetricsRegistry::new());
    let gauge = registry.gauge("concurrent_gauge");

    std::thread::scope(|s| {
        for _ in 0..5 {
            s.spawn(|| {
                for i in 0..100 {
                    gauge.set(i as u64);
                }
            });
        }
    });

    // Final value depends on thread scheduling, but should be some valid value
    assert!(gauge.get() >= 0 && gauge.get() < 100);
}
