//! Cron Scheduler - Schedule functions to run on cron expressions

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::error::Result;
use crate::models::{Event, FunctionRun};
use crate::registry::Registry;
use crate::storage::StateStore;

/// Cron scheduler for time-based triggers
pub struct CronScheduler<S: StateStore> {
    store: S,
    registry: Arc<Registry<S>>,
    /// Track last run time for each cron job
    last_runs: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
    /// Check interval
    check_interval: Duration,
}

impl<S: StateStore + Clone + 'static> CronScheduler<S> {
    /// Create a new cron scheduler
    pub fn new(store: S, registry: Arc<Registry<S>>) -> Self {
        Self {
            store,
            registry,
            last_runs: Arc::new(Mutex::new(HashMap::new())),
            check_interval: Duration::from_secs(60), // Check every minute
        }
    }

    /// Set check interval
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Run the cron scheduler loop
    pub async fn run(&self) -> Result<()> {
        info!("Starting cron scheduler");

        let mut ticker = interval(self.check_interval);

        loop {
            ticker.tick().await;

            if let Err(e) = self.check_and_trigger().await {
                error!(error = %e, "Error checking cron triggers");
            }
        }
    }

    /// Check all cron triggers and fire those that are due
    async fn check_and_trigger(&self) -> Result<()> {
        let now = Utc::now();
        let triggers = self.registry.get_cron_triggers();

        for (schedule, function_id) in triggers {
            if self.should_trigger(&schedule, &function_id, now).await {
                info!(function_id = %function_id, schedule = %schedule, "Triggering cron function");

                if let Err(e) = self.trigger_function(&function_id, now).await {
                    error!(
                        function_id = %function_id,
                        error = %e,
                        "Failed to trigger cron function"
                    );
                }
            }
        }

        Ok(())
    }

    /// Check if a cron job should trigger now
    async fn should_trigger(&self, schedule: &str, function_id: &str, now: DateTime<Utc>) -> bool {
        // Parse cron expression
        let cron = match parse_cron(schedule) {
            Ok(c) => c,
            Err(e) => {
                warn!(schedule = %schedule, error = %e, "Invalid cron expression");
                return false;
            }
        };

        // Get last run time
        let last_runs = self.last_runs.lock().await;
        let last_run = last_runs.get(function_id).copied();
        drop(last_runs);

        // Check if we should run
        match last_run {
            None => true, // Never run before
            Some(last) => {
                // Check if a scheduled time has passed since last run
                if let Some(next) = cron.next_after(last) {
                    next <= now
                } else {
                    false
                }
            }
        }
    }

    /// Trigger a function execution
    async fn trigger_function(&self, function_id: &str, now: DateTime<Utc>) -> Result<()> {
        // Create cron trigger event
        let event = Event::new(
            format!("cron.{}", function_id),
            serde_json::json!({
                "triggered_at": now.to_rfc3339(),
                "function_id": function_id,
            }),
        );

        // Insert event
        self.store.insert_event(&event).await?;

        // Create run
        let run = FunctionRun::new(function_id, event.id, event.data.clone());
        self.store.insert_run(&run).await?;

        // Update last run time
        let mut last_runs = self.last_runs.lock().await;
        last_runs.insert(function_id.to_string(), now);

        debug!(function_id = %function_id, run_id = %run.id, "Created cron run");

        Ok(())
    }

    /// Manually trigger a cron function (for testing)
    pub async fn trigger_now(&self, function_id: &str) -> Result<()> {
        self.trigger_function(function_id, Utc::now()).await
    }
}

/// Simple cron expression parser
/// Supports: minute hour day month weekday
/// Example: "0 9 * * *" = 9am daily
struct CronExpr {
    minute: CronField,
    hour: CronField,
    day: CronField,
    month: CronField,
    weekday: CronField,
}

#[derive(Clone)]
enum CronField {
    Any,
    Value(u32),
    Range(u32, u32),
    List(Vec<u32>),
    Step(u32),
}

impl CronExpr {
    fn matches(&self, dt: DateTime<Utc>) -> bool {
        self.minute
            .matches(dt.format("%M").to_string().parse().unwrap_or(0))
            && self
                .hour
                .matches(dt.format("%H").to_string().parse().unwrap_or(0))
            && self
                .day
                .matches(dt.format("%d").to_string().parse().unwrap_or(1))
            && self
                .month
                .matches(dt.format("%m").to_string().parse().unwrap_or(1))
            && self
                .weekday
                .matches(dt.format("%u").to_string().parse().unwrap_or(1))
    }

    fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Simple implementation: scan forward minute by minute
        // In production, use a proper cron library
        let mut candidate = after + chrono::Duration::minutes(1);

        // Scan up to 1 year
        for _ in 0..(365 * 24 * 60) {
            if self.matches(candidate) {
                return Some(candidate);
            }
            candidate += chrono::Duration::minutes(1);
        }

        None
    }
}

impl CronField {
    fn matches(&self, value: u32) -> bool {
        match self {
            CronField::Any => true,
            CronField::Value(v) => *v == value,
            CronField::Range(start, end) => value >= *start && value <= *end,
            CronField::List(values) => values.contains(&value),
            CronField::Step(step) => value % step == 0,
        }
    }
}

fn parse_cron(expr: &str) -> std::result::Result<CronExpr, String> {
    let parts: Vec<&str> = expr.split_whitespace().collect();

    if parts.len() != 5 {
        return Err(format!("Expected 5 fields, got {}", parts.len()));
    }

    Ok(CronExpr {
        minute: parse_field(parts[0])?,
        hour: parse_field(parts[1])?,
        day: parse_field(parts[2])?,
        month: parse_field(parts[3])?,
        weekday: parse_field(parts[4])?,
    })
}

fn parse_field(field: &str) -> std::result::Result<CronField, String> {
    if field == "*" {
        return Ok(CronField::Any);
    }

    if field.contains('/') {
        let parts: Vec<&str> = field.split('/').collect();
        if parts.len() == 2 {
            if let Ok(step) = parts[1].parse() {
                return Ok(CronField::Step(step));
            }
        }
    }

    if field.contains('-') {
        let parts: Vec<&str> = field.split('-').collect();
        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (parts[0].parse(), parts[1].parse()) {
                return Ok(CronField::Range(start, end));
            }
        }
    }

    if field.contains(',') {
        let values: std::result::Result<Vec<u32>, _> =
            field.split(',').map(|s| s.parse()).collect();
        if let Ok(v) = values {
            return Ok(CronField::List(v));
        }
    }

    if let Ok(v) = field.parse() {
        return Ok(CronField::Value(v));
    }

    Err(format!("Invalid cron field: {}", field))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike, Utc};
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_valid_cron_expressions() {
        let test_cases = vec![
            ("0 9 * * *", "Daily at 9am"),
            ("*/5 * * * *", "Every 5 minutes"),
            ("0 0 1 * *", "First day of month at midnight"),
            ("30 8 * * 1-5", "8:30am weekdays"),
            ("0 12 * * 0", "Noon on Sundays"),
            ("0,30 * * * *", "Every hour at :00 and :30"),
            ("0 9-17 * * 1-5", "9am-5pm weekdays"),
        ];

        for (expr, description) in test_cases {
            let result = parse_cron(expr);
            assert!(
                result.is_ok(),
                "Failed to parse '{}' ({})",
                expr,
                description
            );
        }
    }

    #[test]
    fn test_parse_invalid_cron_expressions() {
        let invalid_expressions = vec!["* * *", "* * * * * *", "abc * * * *"];

        for expr in invalid_expressions {
            let result = parse_cron(expr);
            assert!(
                result.is_err(),
                "Should reject invalid expression: {}",
                expr
            );
        }
    }

    #[test]
    fn test_cron_expr_matches_specific_time() {
        let cron = parse_cron("30 9 * * *").unwrap();

        let jan_15_2024_093000 = Utc.with_ymd_and_hms(2024, 1, 15, 9, 30, 0).unwrap();
        assert!(cron.matches(jan_15_2024_093000));

        let jan_15_2024_092900 = Utc.with_ymd_and_hms(2024, 1, 15, 9, 29, 0).unwrap();
        assert!(!cron.matches(jan_15_2024_092900));
    }

    #[test]
    fn test_cron_expr_next_after_simple() {
        let cron = parse_cron("0 9 * * *").unwrap();

        let before = Utc.with_ymd_and_hms(2024, 1, 15, 8, 30, 0).unwrap();
        let next = cron.next_after(before);

        assert!(next.is_some());
        let next = next.unwrap();
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn test_cron_expr_range_matching() {
        let cron = parse_cron("0 9-17 * * *").unwrap();

        let _9am = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();
        let _12pm = Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();
        let _5pm = Utc.with_ymd_and_hms(2024, 1, 15, 17, 0, 0).unwrap();
        let _6pm = Utc.with_ymd_and_hms(2024, 1, 15, 18, 0, 0).unwrap();

        assert!(cron.matches(_9am));
        assert!(cron.matches(_12pm));
        assert!(cron.matches(_5pm));
        assert!(!cron.matches(_6pm));
    }

    #[test]
    fn test_cron_expr_list_matching() {
        let cron = parse_cron("0 0,12 * * *").unwrap();

        let midnight = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let noon = Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();
        let _3am = Utc.with_ymd_and_hms(2024, 1, 15, 3, 0, 0).unwrap();

        assert!(cron.matches(midnight));
        assert!(cron.matches(noon));
        assert!(!cron.matches(_3am));
    }

    #[test]
    fn test_cron_expr_step_matching() {
        let cron = parse_cron("*/15 * * * *").unwrap();

        let _0 = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();
        let _15 = Utc.with_ymd_and_hms(2024, 1, 15, 9, 15, 0).unwrap();
        let _10 = Utc.with_ymd_and_hms(2024, 1, 15, 9, 10, 0).unwrap();

        assert!(cron.matches(_0));
        assert!(cron.matches(_15));
        assert!(!cron.matches(_10));
    }
}
