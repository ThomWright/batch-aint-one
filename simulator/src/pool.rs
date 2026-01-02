//! Connection pool simulation

use bon::bon;
use rand::{Rng, SeedableRng};

use crate::latency::LatencyProfile;
use crate::metrics::{MetricsCollector, ResourceSnapshot};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// A connection pool that grows on demand
pub struct ConnectionPool {
    total: AtomicUsize,
    available: AtomicUsize,
    acquire_latency: Arc<Mutex<LatencyProfile>>,
    connect_latency: Arc<Mutex<LatencyProfile>>,
    metrics: Arc<Mutex<MetricsCollector>>,
    rng: Arc<Mutex<rand::rngs::StdRng>>,
}

#[bon]
impl ConnectionPool {
    #[builder]
    pub fn new(
        /// Initial number of connections in the pool
        initial_size: usize,
        /// Latency profile for acquiring a connection from the pool
        acquire_latency: LatencyProfile,
        /// Latency profile for opening new connections
        connect_latency: LatencyProfile,
        metrics: Arc<Mutex<MetricsCollector>>,
        /// Optional seed for reproducibility
        seed: Option<u64>,
    ) -> Self {
        let rng = Arc::new(Mutex::new(match seed {
            Some(s) => rand::rngs::StdRng::seed_from_u64(s),
            None => rand::rngs::StdRng::from_os_rng(),
        }));

        let pool = Self {
            total: AtomicUsize::new(initial_size),
            available: AtomicUsize::new(initial_size),
            acquire_latency: Arc::new(Mutex::new(acquire_latency)),
            connect_latency: Arc::new(Mutex::new(connect_latency)),
            metrics,
            rng,
        };

        // Record initial state
        pool.record_snapshot();

        pool
    }

    /// Record a snapshot of the current pool state
    fn record_snapshot(&self) {
        let snapshot = ResourceSnapshot {
            timestamp: tokio::time::Instant::now(),
            total: self.total(),
            in_use: self.in_use(),
            available: self.available(),
        };

        self.metrics
            .lock()
            .expect("should not panic while holding lock")
            .record_resource_snapshot(snapshot);
    }

    /// Acquire a connection from the pool
    ///
    /// If no connections are available, opens a new one (which takes time based on connect_latency)
    pub async fn acquire(self: &Arc<Self>) -> Connection {
        let acquire_latency = self.acquire_latency.lock().unwrap().sample();
        tokio::time::sleep(acquire_latency).await;

        let prev_available = self
            .available
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |available| {
                if available > 0 {
                    Some(available - 1)
                } else {
                    Some(available)
                }
            })
            .expect("should always update");

        if prev_available == 0 {
            // No connections available, need to open a new one
            let latency = self.connect_latency.lock().unwrap().sample();
            tokio::time::sleep(latency).await;

            self.total.fetch_add(1, Ordering::SeqCst);
        }

        // Record snapshot after state change
        self.record_snapshot();

        Connection {
            pool: Arc::clone(self),
        }
    }

    fn return_connection(&self) {
        // Destroy the connection 1% of the time to simulate churn
        if self
            .rng
            .lock()
            .expect("should not panic while holding lock")
            .random_bool(0.01)
        {
            self.total.fetch_sub(1, Ordering::SeqCst);
        } else {
            self.available.fetch_add(1, Ordering::SeqCst);
        }

        // Record snapshot after state change
        self.record_snapshot();
    }

    /// Get the number of connections currently available (idle)
    pub fn available(&self) -> usize {
        self.available.load(Ordering::Relaxed)
    }

    /// Get the number of connections currently in use
    pub fn in_use(&self) -> usize {
        self.total() - self.available()
    }

    /// Get the total number of connections (in use + available)
    pub fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }
}

/// A connection from the pool
///
/// Automatically returned to the pool when dropped
pub struct Connection {
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.return_connection();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TEST_SEED;

    #[tokio::test(start_paused = true)]
    async fn test_pool_grows_on_demand() {
        let acquire_latency = LatencyProfile::builder()
            .tasks(1)
            .task_rate(1000.0)
            .seed(*TEST_SEED)
            .build(); // ~1ms mean
        let connect_latency = LatencyProfile::builder()
            .tasks(2)
            .task_rate(10.0)
            .seed(*TEST_SEED)
            .build(); // ~100ms mean

        let start_time = tokio::time::Instant::now();
        let metrics = Arc::new(Mutex::new(MetricsCollector::new(start_time)));
        let pool = Arc::new(
            ConnectionPool::builder()
                .initial_size(2)
                .acquire_latency(acquire_latency)
                .connect_latency(connect_latency)
                .metrics(metrics.clone())
                .seed(*TEST_SEED)
                .build(),
        );

        // Initially: 2 total, 2 available, 0 in use
        assert_eq!(pool.total(), 2);
        assert_eq!(pool.available(), 2);
        assert_eq!(pool.in_use(), 0);

        // Acquire 2 connections (should use existing ones)
        let _conn1 = pool.acquire().await;
        assert_eq!(pool.total(), 2);
        assert_eq!(pool.available(), 1);
        assert_eq!(pool.in_use(), 1);

        let _conn2 = pool.acquire().await;
        assert_eq!(pool.total(), 2);
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.in_use(), 2);

        // Acquire a 3rd connection (should create a new one)
        let _conn3 = pool.acquire().await;
        assert_eq!(pool.total(), 3);
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.in_use(), 3);

        // Drop one connection
        drop(_conn1);
        assert_eq!(pool.total(), 3);
        assert_eq!(pool.available(), 1);
        assert_eq!(pool.in_use(), 2);

        // Drop all connections
        drop(_conn2);
        drop(_conn3);
        assert_eq!(pool.total(), 3);
        assert_eq!(pool.available(), 3);
        assert_eq!(pool.in_use(), 0);

        // Verify snapshots were recorded
        let metrics_guard = metrics.lock().unwrap();
        let snapshots = metrics_guard.resource_snapshots();
        assert!(!snapshots.is_empty(), "Should have recorded snapshots");
    }

    #[tokio::test(start_paused = true)]
    async fn test_connection_latency() {
        let acquire_latency = LatencyProfile::builder()
            .tasks(1)
            .task_rate(1000.0)
            .seed(*TEST_SEED)
            .build(); // ~1ms mean
        let connect_latency = LatencyProfile::builder()
            .tasks(5)
            .task_rate(1.0)
            .seed(*TEST_SEED)
            .build(); // ~1s mean
        let start_time = tokio::time::Instant::now();
        let metrics = Arc::new(Mutex::new(MetricsCollector::new(start_time)));
        let pool = Arc::new(
            ConnectionPool::builder()
                .initial_size(0)
                .acquire_latency(acquire_latency)
                .connect_latency(connect_latency)
                .metrics(metrics.clone())
                .seed(*TEST_SEED)
                .build(),
        );

        let start = tokio::time::Instant::now();
        let _conn = pool.acquire().await;
        let elapsed = start.elapsed();

        // Should take some time to open the connection
        assert!(
            elapsed >= tokio::time::Duration::from_millis(2),
            "Connection opening should take time"
        );
    }
}
