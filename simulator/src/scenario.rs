//! Scenario runner for orchestrating simulations

use crate::arrival::PoissonArrivals;
use crate::keys::{KeyDistributionConfig, KeyGenerator};
use crate::latency::LatencyProfile;
use crate::metrics::MetricsCollector;
use crate::pool::ConnectionPool;
use crate::processor::{SimProcessor, SimulatedInput};
use batch_aint_one::{Batcher, BatchingPolicy, Limits};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;

/// Configuration for a simulation scenario
pub struct ScenarioConfig {
    /// Scenario name for identification
    pub name: String,

    /// When to stop the simulation
    pub termination: TerminationCondition,

    /// Key distribution across items
    pub key_distribution: KeyDistributionConfig,

    /// Arrival rate (items/sec)
    pub arrival_rate: f64,

    /// Seed for reproducibility (used for arrivals, latencies, keys, etc.)
    pub seed: Option<u64>,

    /// Processing latency profile
    pub processing_latency: LatencyProfile,

    /// Optional connection pool configuration
    pub pool_config: Option<PoolConfig>,

    /// Batching policy
    pub batching_policy: BatchingPolicy,

    /// Batching limits
    pub limits: Limits,
}

/// When to terminate the simulation
#[derive(Debug, Clone, Copy)]
pub enum TerminationCondition {
    /// Stop after processing this many items
    ItemCount(usize),
    /// Stop after this duration of simulated time
    Duration(Duration),
}

/// Configuration for connection pool
pub struct PoolConfig {
    /// Initial number of connections in pool
    pub initial_size: usize,
    /// Latency profile for opening new connections
    pub connect_latency: LatencyProfile,
}

/// Error types for scenario execution
#[derive(Debug)]
pub enum ScenarioError {
    /// Item processing failed
    ProcessingError(String),
}

impl std::fmt::Display for ScenarioError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScenarioError::ProcessingError(msg) => write!(f, "Processing error: {}", msg),
        }
    }
}

impl std::error::Error for ScenarioError {}

/// Orchestrates a simulation scenario
pub struct ScenarioRunner {
    config: ScenarioConfig,
}

impl ScenarioRunner {
    /// Create a new scenario runner
    pub fn new(config: ScenarioConfig) -> Self {
        Self { config }
    }

    /// Run the scenario and return collected metrics
    pub async fn run(self) -> Result<MetricsCollector, ScenarioError> {
        let metrics = Arc::new(Mutex::new(MetricsCollector::new()));

        // Create connection pool if configured
        let pool = self.config.pool_config.map(|pool_config| {
            Arc::new(ConnectionPool::new(
                pool_config.initial_size,
                pool_config.connect_latency,
                metrics.clone(),
            ))
        });

        // Create processor
        let processor = SimProcessor::builder()
            .processing_latency(self.config.processing_latency)
            .metrics(metrics.clone())
            .maybe_pool(pool)
            .build();

        // Create batcher
        let batcher = Batcher::builder()
            .name(&self.config.name)
            .processor(processor)
            .limits(self.config.limits)
            .batching_policy(self.config.batching_policy)
            .build();

        // Determine number of items based on termination condition
        let num_items = match self.config.termination {
            TerminationCondition::ItemCount(count) => count,
            TerminationCondition::Duration(duration) => {
                // Calculate approximate number of items that will arrive in the duration
                let duration_secs = duration.as_secs_f64();
                (self.config.arrival_rate * duration_secs).ceil() as usize
            }
        };

        // Spawn arrival generator
        let mut arrivals = PoissonArrivals::new(self.config.arrival_rate, self.config.seed);
        let mut key_generator = KeyGenerator::new(self.config.key_distribution, self.config.seed);
        let batcher_clone = batcher.clone();

        let arrival_task = tokio::spawn(async move {
            let mut results = Vec::new();
            let sim_start = tokio::time::Instant::now();
            let mut precise_time_offset = Duration::ZERO;

            for item_id in 0..num_items {
                let next_inter_arrival = arrivals.next_inter_arrival_duration();
                precise_time_offset += next_inter_arrival;

                // Calculate what millisecond this item should arrive in
                let target_ms_offset = precise_time_offset.as_millis();
                let current_ms_offset = (tokio::time::Instant::now() - sim_start).as_millis();

                // If we need to advance time, sleep until target millisecond
                if target_ms_offset > current_ms_offset {
                    let sleep_duration =
                        Duration::from_millis((target_ms_offset - current_ms_offset) as u64);
                    tokio::time::sleep(sleep_duration).await;
                }

                // Generate key for this item
                let key = key_generator.next_key();

                // Now submit with precise timestamp
                let submitted_at = sim_start + precise_time_offset;
                let batcher = batcher_clone.clone();

                let task = tokio::spawn(async move {
                    let input = SimulatedInput {
                        item_id,
                        submitted_at,
                    };
                    batcher.add(key, input).await
                });

                results.push(task);
            }

            results
        });

        // Wait for all arrivals and collect results
        let tasks = arrival_task.await.map_err(|e| {
            ScenarioError::ProcessingError(format!("Arrival task failed: {}", e))
        })?;

        // Collect all results - don't stop on individual failures
        // TODO: Record rejections/failures in metrics
        for task in tasks {
            let result = task.await.map_err(|e| {
                ScenarioError::ProcessingError(format!("Item task failed: {}", e))
            })?;

            // Ignore individual item failures - they should be tracked in metrics
            let _ = result;
        }

        batcher.worker_handle().shut_down().await;
        batcher.worker_handle().wait_for_shutdown().await;
        drop(batcher);

        // Extract metrics from Arc<Mutex<>>
        let metrics = Arc::try_unwrap(metrics)
            .map_err(|_| {
                ScenarioError::ProcessingError(
                    "Failed to unwrap metrics (still has references)".to_string(),
                )
            })?
            .into_inner()
            .map_err(|e| {
                ScenarioError::ProcessingError(format!("Failed to lock metrics: {}", e))
            })?;

        Ok(metrics)
    }
}
