//! Scenario runner for orchestrating simulations

use crate::arrival::PoissonArrivals;
use crate::keys::{KeyDistributionConfig, KeyGenerator};
use crate::latency::LatencyProfile;
use crate::metrics::ItemMetrics;
use crate::metrics::MetricsCollector;
use crate::pool::ConnectionPool;
use crate::processor::{SimProcessor, SimulatedInput, SimulatedOutput};

use batch_aint_one::{BatchError, Batcher, BatchingPolicy, Limits};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;

/// Configuration for a simulation scenario
#[derive(Debug, Clone)]
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

    /// Simulated processing error rate (0.0 to 1.0)
    pub processing_error_rate: f64,

    /// Simulated resource acquisition error rate (0.0 to 1.0)
    pub resource_acquisition_error_rate: f64,

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
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Initial number of connections in pool
    pub initial_size: usize,
    /// Latency profile for acquiring a connection
    pub acquire_latency: LatencyProfile,
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
        let start_time = tokio::time::Instant::now();
        let metrics = Arc::new(Mutex::new(MetricsCollector::new(start_time)));

        let batcher = self.create_batcher(metrics.clone())?;
        let tasks = self.spawn_arrivals(batcher.clone()).await?;
        self.await_all_items(tasks, metrics.clone()).await?;
        self.shut_down_batcher(batcher).await;
        self.extract_metrics(metrics)
    }

    /// Create batcher with processor and optional connection pool
    fn create_batcher(
        &self,
        metrics: Arc<Mutex<MetricsCollector>>,
    ) -> Result<Batcher<SimProcessor>, ScenarioError> {
        // Create connection pool if configured
        let pool = self.config.pool_config.as_ref().map(|pool_config| {
            Arc::new(ConnectionPool::new(
                pool_config.initial_size,
                pool_config.acquire_latency.clone(),
                pool_config.connect_latency.clone(),
                metrics.clone(),
            ))
        });

        // Create processor
        let processor = SimProcessor::builder()
            .processing_latency(self.config.processing_latency.clone())
            .metrics(metrics)
            .maybe_pool(pool)
            .acquire_error_rate(self.config.resource_acquisition_error_rate)
            .process_error_rate(self.config.processing_error_rate)
            .build();

        // Create batcher
        let batcher = Batcher::builder()
            .name(&self.config.name)
            .processor(processor)
            .limits(self.config.limits)
            .batching_policy(self.config.batching_policy.clone())
            .build();

        Ok(batcher)
    }

    /// Spawn arrival generator task that submits items to the batcher
    async fn spawn_arrivals(
        &self,
        batcher: Batcher<SimProcessor>,
    ) -> Result<
        Vec<
            tokio::task::JoinHandle<(
                SimulatedInput,
                Result<SimulatedOutput, (batch_aint_one::BatchError<String>, tokio::time::Instant)>,
            )>,
        >,
        ScenarioError,
    > {
        let mut arrivals = PoissonArrivals::new(self.config.arrival_rate, self.config.seed);
        let mut key_generator =
            KeyGenerator::new(self.config.key_distribution.clone(), self.config.seed);
        let termination = self.config.termination;

        let arrival_task = tokio::spawn(async move {
            let mut results = Vec::new();
            let sim_start = tokio::time::Instant::now();
            let mut precise_time_offset = Duration::ZERO;
            let mut item_id = 0;

            loop {
                let next_inter_arrival = arrivals.next_inter_arrival_duration();
                precise_time_offset += next_inter_arrival;

                // Check termination condition
                match termination {
                    TerminationCondition::ItemCount(count) if item_id >= count => break,
                    TerminationCondition::Duration(duration) if precise_time_offset > duration => {
                        break;
                    }
                    _ => {}
                }

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
                let batcher = batcher.clone();
                let current_item_id = item_id;

                let task = tokio::spawn(async move {
                    let input = SimulatedInput {
                        item_id: current_item_id,
                        submitted_at,
                    };
                    let result = batcher.add(key, input.clone()).await;
                    let result = result.map_err(|e| (e, tokio::time::Instant::now()));
                    (input, result)
                });

                results.push(task);
                item_id += 1;
            }

            results
        });

        arrival_task
            .await
            .map_err(|e| ScenarioError::ProcessingError(format!("Arrival task failed: {}", e)))
    }

    /// Wait for all item processing tasks to complete and record all outcomes
    async fn await_all_items(
        &self,
        tasks: Vec<
            tokio::task::JoinHandle<(
                SimulatedInput,
                Result<SimulatedOutput, (batch_aint_one::BatchError<String>, tokio::time::Instant)>,
            )>,
        >,
        metrics: Arc<Mutex<MetricsCollector>>,
    ) -> Result<(), ScenarioError> {
        // Collect all results and record metrics for each outcome
        // let mut tasks: FuturesOrdered<_> = tasks.into_iter().collect();
        let tasks = tasks.into_iter();

        for result in tasks {
            let result = result
                .await
                .map_err(|e| ScenarioError::ProcessingError(format!("Item task failed: {}", e)))?;

            // Record item metrics based on outcome
            let (input, result) = result;
            let item_metrics = match result {
                Ok(output) => ItemMetrics::success(output),
                Err((BatchError::Rejected(_reason), rejected_at)) => {
                    ItemMetrics::rejection(input, rejected_at)
                }
                Err((BatchError::ResourceAcquisitionFailed(_err_msg), errored_at)) => {
                    ItemMetrics::failure(input, errored_at)
                }
                Err((BatchError::BatchFailed(_e), errored_at)) => {
                    ItemMetrics::failure(input, errored_at)
                }
                Err(e) => {
                    panic!("Item processing error: {:?}", e);
                }
            };

            metrics
                .lock()
                .expect("should not panic while holding lock")
                .record_item(item_metrics);
        }

        Ok(())
    }

    /// Shut down batcher and wait for all work to complete
    async fn shut_down_batcher(&self, batcher: Batcher<SimProcessor>) {
        batcher.worker_handle().shut_down().await;
        batcher.worker_handle().wait_for_shutdown().await;
        drop(batcher);
    }

    /// Extract metrics from Arc<Mutex<>>
    fn extract_metrics(
        &self,
        metrics: Arc<Mutex<MetricsCollector>>,
    ) -> Result<MetricsCollector, ScenarioError> {
        Arc::try_unwrap(metrics)
            .map_err(|_| {
                ScenarioError::ProcessingError(
                    "Failed to unwrap metrics (still has references)".to_string(),
                )
            })?
            .into_inner()
            .map_err(|e| ScenarioError::ProcessingError(format!("Failed to lock metrics: {}", e)))
    }
}
