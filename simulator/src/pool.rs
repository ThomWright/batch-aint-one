//! Connection pool simulation

use crate::latency::LatencyProfile;
use crate::metrics::{MetricsCollector, ResourceSnapshot};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// A connection pool that grows on demand
pub struct ConnectionPool {
    total: AtomicUsize,
    available: AtomicUsize,
    connect_latency: Arc<Mutex<LatencyProfile>>,
    metrics: Arc<Mutex<MetricsCollector>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    ///
    /// # Arguments
    /// * `initial_size` - Initial number of connections in the pool
    /// * `connect_latency` - Latency profile for opening new connections
    /// * `metrics` - Metrics collector for recording pool state changes
    pub fn new(
        initial_size: usize,
        connect_latency: LatencyProfile,
        metrics: Arc<Mutex<MetricsCollector>>,
    ) -> Self {
        let pool = Self {
            total: AtomicUsize::new(initial_size),
            available: AtomicUsize::new(initial_size),
            connect_latency: Arc::new(Mutex::new(connect_latency)),
            metrics,
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
        let prev_available = self.available.fetch_sub(1, Ordering::SeqCst);

        if prev_available == 0 {
            // No connections available, need to open a new one
            self.available.fetch_add(1, Ordering::SeqCst); // Undo the decrement

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
        self.pool.available.fetch_add(1, Ordering::SeqCst);

        // Record snapshot after state change
        self.pool.record_snapshot();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TEST_SEED;

    #[tokio::test(start_paused = true)]
    async fn test_pool_grows_on_demand() {
        let connect_latency = LatencyProfile::builder()
            .tasks(2)
            .task_rate(100.0)
            .seed(*TEST_SEED)
            .build(); // ~10ms mean
        let metrics = Arc::new(Mutex::new(MetricsCollector::new()));
        let pool = Arc::new(ConnectionPool::new(2, connect_latency, metrics.clone()));

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
        let connect_latency = LatencyProfile::builder()
            .tasks(1)
            .task_rate(1.0)
            .seed(*TEST_SEED)
            .build(); // ~1s mean
        let metrics = Arc::new(Mutex::new(MetricsCollector::new()));
        let pool = Arc::new(ConnectionPool::new(0, connect_latency, metrics));

        let start = tokio::time::Instant::now();
        let _conn = pool.acquire().await;
        let elapsed = start.elapsed();

        // Should take > 10ms to open the connection
        assert!(
            elapsed >= tokio::time::Duration::from_millis(10),
            "Connection opening should take time"
        );
    }
}
