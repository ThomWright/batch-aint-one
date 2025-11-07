//! Metrics collection and analysis

use std::collections::HashMap;
use tokio::time::{Duration, Instant};

/// Metrics for a single processed item
#[derive(Debug, Clone)]
pub struct ItemMetrics {
    pub item_id: usize,
    pub key: String,
    pub submitted_at: Instant,
    pub completed_at: Instant,
    pub batch_id: usize,
    pub batch_size: usize,
}

impl ItemMetrics {
    /// Total end-to-end latency
    pub fn total_latency(&self) -> Duration {
        self.completed_at - self.submitted_at
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

    /// Calculate the total simulated duration from first item submission to last completion
    pub fn simulated_duration(&self) -> Option<Duration> {
        if self.items.is_empty() {
            return None;
        }

        let first_submitted = self.items.iter().map(|i| i.submitted_at).min()?;
        let last_completed = self.items.iter().map(|i| i.completed_at).max()?;

        Some(last_completed - first_submitted)
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
        bucket_size_secs: Option<f64>,
    ) -> Option<(f64, Vec<Vec<&ItemMetrics>>)> {
        if self.items.is_empty() {
            return None;
        }

        let mut items: Vec<_> = self.items.iter().collect();
        items.sort_by_key(|item| item.submitted_at);

        let start_time = items[0].submitted_at;
        let end_time = items.last().unwrap().submitted_at;
        let duration = (end_time - start_time).as_secs_f64();

        // Calculate bucket size: aim for 100 buckets by default
        let bucket_size = bucket_size_secs.unwrap_or_else(|| {
            let target_buckets = 100.0;
            (duration / target_buckets).max(0.1) // At least 100ms buckets
        });

        // Group items into buckets
        let num_buckets = (duration / bucket_size).ceil() as usize;
        let mut buckets: Vec<Vec<&ItemMetrics>> = vec![Vec::new(); num_buckets];

        for item in &items {
            let elapsed = (item.submitted_at - start_time).as_secs_f64();
            let bucket_idx = ((elapsed / bucket_size).floor() as usize).min(num_buckets - 1);
            buckets[bucket_idx].push(item);
        }

        Some((bucket_size, buckets))
    }

    /// Get requests per second (RPS) over time.
    ///
    /// Returns (time_seconds, requests_per_second) pairs.
    ///
    /// Automatically buckets arrivals to produce 100 data points by default, or uses the specified
    /// bucket size.
    pub fn rps_over_time(&self, bucket_size_secs: Option<f64>) -> Vec<(f64, f64)> {
        let Some((bucket_size, buckets)) = self.bucket_items_by_time(bucket_size_secs) else {
            return Vec::new();
        };

        // Convert to RPS (items per second)
        buckets
            .into_iter()
            .enumerate()
            .map(|(idx, items)| {
                let time = idx as f64 * bucket_size;
                let rps = items.len() as f64 / bucket_size;
                (time, rps)
            })
            .collect()
    }

    /// Get processing latency samples.
    ///
    /// Returns latencies in milliseconds.
    pub fn latency_samples(&self) -> Vec<f64> {
        self.items
            .iter()
            .map(|item| item.total_latency().as_secs_f64() * 1000.0)
            .collect()
    }

    /// Get resource usage over time.
    ///
    /// Returns (time_seconds, in_use, available) tuples for each snapshot.
    pub fn resource_usage_over_time(&self) -> Vec<(f64, usize, usize)> {
        self.resource_snapshots
            .iter()
            .map(|snapshot| {
                let time = (snapshot.timestamp - self.start_time).as_secs_f64();
                (time, snapshot.in_use, snapshot.available)
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

        let median = if total_batches % 2 == 0 {
            let mid = total_batches / 2;
            (batch_sizes[mid - 1] + batch_sizes[mid]) as f64 / 2.0
        } else {
            batch_sizes[total_batches / 2] as f64
        };

        // Build histogram
        let mut size_distribution: HashMap<usize, usize> = HashMap::new();
        for size in &batch_sizes {
            *size_distribution.entry(*size).or_insert(0) += 1;
        }

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

    /// Calculate mean latency in milliseconds.
    ///
    /// Returns 0.0 if no items have been processed.
    pub fn mean_latency_ms(&self) -> f64 {
        let samples = self.latency_samples();
        if samples.is_empty() {
            0.0
        } else {
            samples.iter().sum::<f64>() / samples.len() as f64
        }
    }

    /// Calculate mean requests per second (RPS) across all time buckets.
    ///
    /// Returns 0.0 if no items have been processed.
    pub fn mean_rps(&self) -> f64 {
        let rps_data = self.rps_over_time(None);
        if rps_data.is_empty() {
            0.0
        } else {
            rps_data.iter().map(|&(_, rps)| rps).sum::<f64>() / rps_data.len() as f64
        }
    }

    /// Get latency statistics over time.
    ///
    /// Returns (time_seconds, mean_ms, p50_ms, p99_ms) tuples for each time bucket.
    ///
    /// Automatically buckets items to produce 100 data points by default, or uses the specified
    /// bucket size.
    pub fn latency_over_time(&self, bucket_size_secs: Option<f64>) -> Vec<(f64, f64, f64, f64)> {
        let Some((bucket_size, buckets)) = self.bucket_items_by_time(bucket_size_secs) else {
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
                let mut latencies: Vec<f64> = items
                    .iter()
                    .map(|item| item.total_latency().as_secs_f64() * 1000.0)
                    .collect();
                latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let time = idx as f64 * bucket_size;
                let mean = latencies.iter().sum::<f64>() / latencies.len() as f64;
                let p50 = percentile(&latencies, 0.5);
                let p99 = percentile(&latencies, 0.99);

                Some((time, mean, p50, p99))
            })
            .collect()
    }
}

/// Calculate percentile from sorted data
fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    if sorted_data.len() == 1 {
        return sorted_data[0];
    }

    let rank = p * (sorted_data.len() - 1) as f64;
    let lower_idx = rank.floor() as usize;
    let upper_idx = rank.ceil() as usize;
    let weight = rank - lower_idx as f64;

    sorted_data[lower_idx] * (1.0 - weight) + sorted_data[upper_idx] * weight
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
    fn test_rps_calculation() {
        let start = tokio::time::Instant::now();
        let mut collector = MetricsCollector::new(start);

        // Simulate 1000 items arriving at exactly 100 RPS (10ms intervals)
        let start = tokio::time::Instant::now();
        for i in 0..1000 {
            let submitted_at = start + tokio::time::Duration::from_millis(i * 10);
            collector.record_item(ItemMetrics {
                item_id: i as usize,
                key: "test".to_string(),
                submitted_at,
                completed_at: submitted_at,
                batch_id: 0,
                batch_size: 1,
            });
        }

        // Total duration: 0 to 9.99 seconds (1000 items * 10ms)
        // Get arrival rate with 1-second buckets
        let rate_data = collector.rps_over_time(Some(1.0));

        // Check each bucket has the expected RPS
        for (time, rps) in &rate_data {
            assert!(
                (rps - 100.0).abs() < 1.0,
                "RPS at time {:.1}s should be ~100, got {:.1}",
                time,
                rps
            );
        }

        // Check the time span matches duration
        let first_time = rate_data.first().unwrap().0;
        let last_time = rate_data.last().unwrap().0;
        assert_eq!(first_time, 0.0, "First bucket should start at 0");

        // Duration is 9.99s, so with 1s buckets we expect buckets at 0,1,2,...,9
        // Last bucket starts at 9.0
        assert!(
            last_time >= 9.0 && last_time < 10.0,
            "Last bucket should start around 9s, got {}s",
            last_time
        );
    }
}
