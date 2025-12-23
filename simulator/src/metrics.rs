//! Metrics collection and analysis

use std::collections::HashMap;

use tokio::time::{Duration, Instant};

use crate::processor::{SimulatedInput, SimulatedOutput};

/// A single data point for requests per second over time
#[derive(Debug, Clone)]
pub struct RpsPoint {
    pub time: Duration,
    pub rps: f64,
}

/// Resource usage at a point in time
#[derive(Debug, Clone)]
pub struct ResourceUsagePoint {
    pub time: Duration,
    pub in_use: usize,
    pub available: usize,
}

/// Latency statistics for a time bucket
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub time: Duration,
    pub mean: Duration,
    pub p50: Duration,
    pub p99: Duration,
}

/// Outcome of processing an item
#[derive(Debug, Clone)]
enum ItemOutcome {
    /// Item was successfully processed
    Success { completed_at: Instant },
    /// Item was rejected (e.g., queue full, policy rejection)
    Rejected { rejected_at: Instant },
    /// Item processing resulted in an error
    Errored { errored_at: Instant },
}

/// Metrics for a single processed item
#[derive(Debug, Clone)]
pub struct ItemMetrics {
    submitted_at: Instant,
    outcome: ItemOutcome,
}

impl ItemMetrics {
    pub fn success(output: SimulatedOutput) -> Self {
        Self {
            submitted_at: output.submitted_at,
            outcome: ItemOutcome::Success {
                completed_at: output.completed_at,
            },
        }
    }

    pub fn rejection(input: SimulatedInput, rejected_at: Instant) -> Self {
        Self {
            submitted_at: input.submitted_at,
            outcome: ItemOutcome::Rejected { rejected_at },
        }
    }

    pub fn failure(input: SimulatedInput, errored_at: Instant) -> Self {
        Self {
            submitted_at: input.submitted_at,
            outcome: ItemOutcome::Errored { errored_at },
        }
    }

    /// Total end-to-end latency from submission to outcome
    pub fn total_latency(&self) -> Duration {
        self.end_time() - self.submitted_at
    }

    /// Get the end time of this item (completed, rejected, or failed)
    pub fn end_time(&self) -> Instant {
        match &self.outcome {
            ItemOutcome::Success { completed_at, .. } => *completed_at,
            ItemOutcome::Rejected { rejected_at, .. } => *rejected_at,
            ItemOutcome::Errored { errored_at, .. } => *errored_at,
        }
    }

    /// Check if this item was successfully processed
    pub fn is_success(&self) -> bool {
        matches!(self.outcome, ItemOutcome::Success { .. })
    }
}

/// Metrics for a processed batch
#[derive(Debug, Clone)]
pub struct BatchMetrics {
    pub batch_id: usize,
    pub key: String,
    pub batch_size: usize,
    pub processing_started_at: Instant,
    pub completed_at: Instant,
}

impl BatchMetrics {
    /// Time spent processing this batch
    pub fn processing_duration(&self) -> Duration {
        self.completed_at - self.processing_started_at
    }
}

/// Snapshot of resource pool state at a point in time
#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub timestamp: Instant,
    pub total: usize,
    pub in_use: usize,
    pub available: usize,
}

/// Collects metrics during simulation
#[derive(Debug)]
pub struct MetricsCollector {
    start_time: Instant,
    items: Vec<ItemMetrics>,
    batches: Vec<BatchMetrics>,
    resource_snapshots: Vec<ResourceSnapshot>,
}

impl MetricsCollector {
    pub fn new(start_time: Instant) -> Self {
        Self {
            start_time,
            items: Vec::new(),
            batches: Vec::new(),
            resource_snapshots: Vec::new(),
        }
    }

    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    pub fn record_item(&mut self, metrics: ItemMetrics) {
        self.items.push(metrics);
    }

    pub fn record_batch(&mut self, metrics: BatchMetrics) {
        self.batches.push(metrics);
    }

    pub fn record_resource_snapshot(&mut self, snapshot: ResourceSnapshot) {
        self.resource_snapshots.push(snapshot);
    }

    pub fn items(&self) -> &[ItemMetrics] {
        &self.items
    }

    pub fn error_count(&self) -> usize {
        self.items.iter().filter(|item| !item.is_success()).count()
    }

    /// Calculate the total simulated duration from first item submission to last outcome
    pub fn simulated_duration(&self) -> Option<Duration> {
        if self.items.is_empty() {
            return None;
        }

        let first_submitted = self.items.iter().map(|i| i.submitted_at).min()?;
        let last_outcome = self.items.iter().map(|i| i.end_time()).max()?;

        Some(last_outcome - first_submitted)
    }

    pub fn batches(&self) -> &[BatchMetrics] {
        &self.batches
    }

    pub fn resource_snapshots(&self) -> &[ResourceSnapshot] {
        &self.resource_snapshots
    }

    /// Helper to bucket items by time.
    ///
    /// Returns (bucket_size, buckets) where buckets is a Vec of item collections.
    fn bucket_items_by_time(
        &self,
        bucket_size: Option<Duration>,
    ) -> Option<(Duration, Vec<Vec<&ItemMetrics>>)> {
        if self.items.is_empty() {
            return None;
        }

        let mut items: Vec<_> = self.items.iter().collect();
        items.sort_by_key(|item| item.submitted_at);

        let start_time = items[0].submitted_at;
        let end_time = items.last().unwrap().submitted_at;
        let duration = end_time - start_time;

        // Calculate bucket size: aim for 100 buckets by default
        let bucket_size = bucket_size.unwrap_or_else(|| {
            let target_buckets = 100.0;
            let default_size = duration.as_secs_f64() / target_buckets;
            Duration::from_secs_f64(default_size.max(0.1)) // At least 100ms buckets
        });

        // Group items into buckets
        let num_buckets = (duration.as_secs_f64() / bucket_size.as_secs_f64()).ceil() as usize;
        let mut buckets: Vec<Vec<&ItemMetrics>> = vec![Vec::new(); num_buckets];

        for item in &items {
            let elapsed = item.submitted_at - start_time;
            let bucket_idx = (elapsed.as_secs_f64() / bucket_size.as_secs_f64()).floor() as usize;
            let bucket_idx = bucket_idx.min(num_buckets - 1);
            buckets[bucket_idx].push(item);
        }

        Some((bucket_size, buckets))
    }

    /// Get requests per second (RPS) over time.
    ///
    /// Returns data points where time is relative to start.
    ///
    /// Automatically buckets arrivals to produce 100 data points by default, or uses the specified
    /// bucket size.
    pub fn rps_over_time(&self, bucket_size: Option<Duration>) -> Vec<RpsPoint> {
        let Some((bucket_size, buckets)) = self.bucket_items_by_time(bucket_size) else {
            return Vec::new();
        };

        // Convert to RPS (items per second)
        buckets
            .into_iter()
            .enumerate()
            .map(|(idx, items)| {
                let time = bucket_size * idx as u32;
                let rps = items.len() as f64 / bucket_size.as_secs_f64();
                RpsPoint { time, rps }
            })
            .collect()
    }

    /// Get processing latency samples.
    pub fn latency_samples(&self) -> Vec<Duration> {
        self.items.iter().map(|item| item.total_latency()).collect()
    }

    /// Get resource usage over time.
    ///
    /// Returns data points for each snapshot where time is relative to start.
    pub fn resource_usage_over_time(&self) -> Vec<ResourceUsagePoint> {
        self.resource_snapshots
            .iter()
            .map(|snapshot| {
                let time = snapshot.timestamp - self.start_time;
                ResourceUsagePoint {
                    time,
                    in_use: snapshot.in_use,
                    available: snapshot.available,
                }
            })
            .collect()
    }

    /// Analyze batch efficiency.
    pub fn batch_efficiency(&self) -> BatchEfficiency {
        if self.batches.is_empty() {
            return BatchEfficiency::default();
        }

        let total_batches = self.batches.len();
        let mut batch_sizes: Vec<usize> = self.batches.iter().map(|b| b.batch_size).collect();
        batch_sizes.sort_unstable();

        let smallest_batch_size = *batch_sizes.first().unwrap_or(&0);
        let batches_at_smallest = self
            .batches
            .iter()
            .filter(|b| b.batch_size == smallest_batch_size)
            .count();

        let largest_batch_size = *batch_sizes.iter().max().unwrap_or(&0);
        let batches_at_largest = self
            .batches
            .iter()
            .filter(|b| b.batch_size == largest_batch_size)
            .count();

        let sum: usize = batch_sizes.iter().sum();
        let mean = sum as f64 / total_batches as f64;

        let median = calculate_median(&batch_sizes);
        let size_distribution = build_frequency_map(&batch_sizes);

        BatchEfficiency {
            total_batches,
            mean_batch_size: mean,
            median_batch_size: median,
            smallest_batch_size,
            largest_batch_size,
            batches_at_smallest_size: batches_at_smallest,
            batches_at_largest_size: batches_at_largest,
            size_distribution,
        }
    }

    /// Calculate mean latency.
    ///
    /// Returns Duration::ZERO if no items have been processed.
    pub fn mean_latency(&self) -> Duration {
        if self.items.is_empty() {
            return Duration::ZERO;
        }

        let total: Duration = self.items.iter().map(|item| item.total_latency()).sum();
        total / self.items.len() as u32
    }

    /// Calculate mean error latency.
    ///
    /// Returns Duration::ZERO if no items have been processed.
    pub fn mean_error_latency(&self) -> Duration {
        let error_items: Vec<&ItemMetrics> = self
            .items
            .iter()
            .filter(|item| !item.is_success())
            .collect();

        if error_items.is_empty() {
            return Duration::ZERO;
        }

        let total: Duration = error_items.iter().map(|item| item.total_latency()).sum();
        total / error_items.len() as u32
    }

    /// Calculate mean requests per second (RPS) across all time buckets.
    ///
    /// Returns 0.0 if no items have been processed.
    pub fn mean_rps(&self) -> f64 {
        let rps_data = self.rps_over_time(None);
        if rps_data.is_empty() {
            0.0
        } else {
            rps_data.iter().map(|point| point.rps).sum::<f64>() / rps_data.len() as f64
        }
    }

    /// Get latency statistics over time.
    ///
    /// Returns statistics for each time bucket where time is relative to start.
    ///
    /// Automatically buckets items to produce 100 data points by default, or uses the specified
    /// bucket size.
    pub fn latency_over_time(&self, bucket_size: Option<Duration>) -> Vec<LatencyStats> {
        let Some((bucket_size, buckets)) = self.bucket_items_by_time(bucket_size) else {
            return Vec::new();
        };

        // Calculate statistics for each bucket
        buckets
            .into_iter()
            .enumerate()
            .filter_map(|(idx, items)| {
                if items.is_empty() {
                    return None;
                }

                // Extract and sort latencies
                let mut latencies: Vec<Duration> =
                    items.iter().map(|item| item.total_latency()).collect();
                latencies.sort();

                let time = bucket_size * idx as u32;
                let mean = duration_mean(&latencies);
                let p50 = duration_percentile(&latencies, 0.5);
                let p99 = duration_percentile(&latencies, 0.99);

                Some(LatencyStats {
                    time,
                    mean,
                    p50,
                    p99,
                })
            })
            .collect()
    }
}

/// Calculate mean duration from a collection
fn duration_mean(durations: &[Duration]) -> Duration {
    if durations.is_empty() {
        return Duration::ZERO;
    }
    let total: Duration = durations.iter().copied().sum();
    total / durations.len() as u32
}

/// Calculate percentile from sorted duration data
fn duration_percentile(sorted_data: &[Duration], p: f64) -> Duration {
    if sorted_data.is_empty() {
        return Duration::ZERO;
    }
    if sorted_data.len() == 1 {
        return sorted_data[0];
    }

    let rank = p * (sorted_data.len() - 1) as f64;
    let lower_idx = rank.floor() as usize;
    let upper_idx = rank.ceil() as usize;
    let weight = rank - lower_idx as f64;

    let lower = sorted_data[lower_idx].as_secs_f64();
    let upper = sorted_data[upper_idx].as_secs_f64();
    Duration::from_secs_f64(lower * (1.0 - weight) + upper * weight)
}

/// Calculate median from sorted values
fn calculate_median(sorted_values: &[usize]) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let len = sorted_values.len();
    if len.is_multiple_of(2) {
        let mid = len / 2;
        (sorted_values[mid - 1] + sorted_values[mid]) as f64 / 2.0
    } else {
        sorted_values[len / 2] as f64
    }
}

/// Build a frequency map counting occurrences of each value
fn build_frequency_map<T: Eq + std::hash::Hash + Copy>(values: &[T]) -> HashMap<T, usize> {
    let mut map = HashMap::new();
    for value in values {
        *map.entry(*value).or_insert(0) += 1;
    }
    map
}

/// Analysis of batch efficiency
#[derive(Debug, Default)]
pub struct BatchEfficiency {
    pub total_batches: usize,
    pub mean_batch_size: f64,
    pub median_batch_size: f64,
    pub smallest_batch_size: usize,
    pub largest_batch_size: usize,
    pub batches_at_smallest_size: usize,
    pub batches_at_largest_size: usize,
    pub size_distribution: HashMap<usize, usize>,
}

impl BatchEfficiency {
    pub fn percentage_at_smallest(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            (self.batches_at_smallest_size as f64 / self.total_batches as f64) * 100.0
        }
    }

    pub fn percentage_at_largest(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            (self.batches_at_largest_size as f64 / self.total_batches as f64) * 100.0
        }
    }

    /// Export batch size distribution as gnuplot-compatible data
    /// Returns (batch_size, count) pairs sorted by batch size
    pub fn size_distribution_sorted(&self) -> Vec<(usize, usize)> {
        let mut data: Vec<_> = self
            .size_distribution
            .iter()
            .map(|(size, count)| (*size, *count))
            .collect();
        data.sort_by_key(|(size, _)| *size);
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_median_empty() {
        assert_eq!(calculate_median(&[]), 0.0);
    }

    #[test]
    fn test_calculate_median_single() {
        assert_eq!(calculate_median(&[5]), 5.0);
    }

    #[test]
    fn test_calculate_median_two_values() {
        assert_eq!(calculate_median(&[3, 7]), 5.0);
    }

    #[test]
    fn test_calculate_median_odd_count() {
        assert_eq!(calculate_median(&[1, 2, 3, 4, 5]), 3.0);
    }

    #[test]
    fn test_calculate_median_even_count() {
        assert_eq!(calculate_median(&[1, 2, 3, 4]), 2.5);
    }

    #[test]
    fn test_build_frequency_map_empty() {
        let map: HashMap<usize, usize> = build_frequency_map(&[]);
        assert!(map.is_empty());
    }

    #[test]
    fn test_build_frequency_map_single() {
        let map = build_frequency_map(&[5]);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&5), Some(&1));
    }

    #[test]
    fn test_build_frequency_map_all_same() {
        let map = build_frequency_map(&[7, 7, 7, 7]);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&7), Some(&4));
    }

    #[test]
    fn test_build_frequency_map_varied() {
        let map = build_frequency_map(&[1, 2, 2, 3, 3, 3]);
        assert_eq!(map.len(), 3);
        assert_eq!(map.get(&1), Some(&1));
        assert_eq!(map.get(&2), Some(&2));
        assert_eq!(map.get(&3), Some(&3));
    }

    #[test]
    fn test_rps_calculation() {
        let start = tokio::time::Instant::now();
        let mut collector = MetricsCollector::new(start);

        // Simulate 1000 items arriving at exactly 100 RPS (10ms intervals)
        let start = tokio::time::Instant::now();
        for i in 0..1000 {
            let submitted_at = start + tokio::time::Duration::from_millis(i * 10);
            collector.record_item(ItemMetrics {
                submitted_at,
                outcome: ItemOutcome::Success {
                    completed_at: submitted_at,
                },
            });
        }

        // Total duration: 0 to 9.99 seconds (1000 items * 10ms)
        // Get arrival rate with 1-second buckets
        let rate_data = collector.rps_over_time(Some(Duration::from_secs(1)));

        // Check each bucket has the expected RPS
        for point in &rate_data {
            assert!(
                (point.rps - 100.0).abs() < 1.0,
                "RPS at time {:.1}s should be ~100, got {:.1}",
                point.time.as_secs_f64(),
                point.rps
            );
        }

        // Check the time span matches duration
        let first_point = rate_data.first().unwrap();
        let last_point = rate_data.last().unwrap();
        assert_eq!(
            first_point.time,
            Duration::ZERO,
            "First bucket should start at 0"
        );

        // Duration is 9.99s, so with 1s buckets we expect buckets at 0,1,2,...,9
        // Last bucket starts at 9.0
        let last_time_secs = last_point.time.as_secs_f64();
        assert!(
            (9.0..10.0).contains(&last_time_secs),
            "Last bucket should start around 9s, got {}s",
            last_time_secs
        );
    }
}
