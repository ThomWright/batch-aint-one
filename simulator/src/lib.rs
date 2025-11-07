//! Simulation framework for testing batch-aint-one under various workloads.

pub mod arrival;
pub mod keys;
pub mod latency;
pub mod metrics;
pub mod pool;
pub mod processor;
pub mod reporter;
pub mod scenario;
pub mod visualise;

#[cfg(test)]
pub mod test_utils {
    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};
    use std::sync::LazyLock;

    pub static TEST_SEED: LazyLock<u64> = LazyLock::new(|| {
        let seed = StdRng::from_os_rng().next_u64();
        println!("Using test seed: {}", seed);
        seed
    });
}
