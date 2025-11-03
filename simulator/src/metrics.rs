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

/// Collects metrics during simulation
#[derive(Debug, Default)]
pub struct MetricsCollector {
    items: Vec<ItemMetrics>,
    batches: Vec<BatchMetrics>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_item(&mut self, metrics: ItemMetrics) {
        self.items.push(metrics);
    }

    pub fn record_batch(&mut self, metrics: BatchMetrics) {
        self.batches.push(metrics);
    }

    pub fn items(&self) -> &[ItemMetrics] {
        &self.items
    }

    pub fn batches(&self) -> &[BatchMetrics] {
        &self.batches
    }

    /// Get requests per second (RPS) over time.
    ///
    /// Returns (time_seconds, requests_per_second) pairs.
    ///
    /// Automatically buckets arrivals to produce 100 data points by default, or uses the specified
    /// bucket size.
    pub fn rps_over_time(&self, bucket_size_secs: Option<f64>) -> Vec<(f64, f64)> {
        if self.items.is_empty() {
            return Vec::new();
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

        // Count items per bucket
        let num_buckets = (duration / bucket_size).ceil() as usize;
        let mut buckets = vec![0usize; num_buckets];

        for item in &items {
            let elapsed = (item.submitted_at - start_time).as_secs_f64();
            let bucket_idx = ((elapsed / bucket_size).floor() as usize).min(num_buckets - 1);
            buckets[bucket_idx] += 1;
        }

        // Convert to RPS (items per second)
        buckets
            .into_iter()
            .enumerate()
            .map(|(idx, count)| {
                let time = idx as f64 * bucket_size;
                let rps = count as f64 / bucket_size;
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

    /// Analyze batch efficiency.
    pub fn batch_efficiency(&self) -> BatchEfficiency {
        if self.batches.is_empty() {
            return BatchEfficiency::default();
        }

        let total_batches = self.batches.len();
        let mut batch_sizes: Vec<usize> = self.batches.iter().map(|b| b.batch_size).collect();
        batch_sizes.sort_unstable();

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
            smallest_batch_size: *batch_sizes.first().unwrap_or(&0),
            largest_batch_size,
            batches_at_largest_size: batches_at_largest,
            size_distribution,
        }
    }
}

/// Analysis of batch efficiency
#[derive(Debug, Default)]
pub struct BatchEfficiency {
    pub total_batches: usize,
    pub mean_batch_size: f64,
    pub median_batch_size: f64,
    pub smallest_batch_size: usize,
    pub largest_batch_size: usize,
    pub batches_at_largest_size: usize,
    pub size_distribution: HashMap<usize, usize>,
}

impl BatchEfficiency {
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
        let mut collector = MetricsCollector::new();

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
