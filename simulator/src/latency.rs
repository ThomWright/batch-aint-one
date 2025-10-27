//! Latency distribution modelling using Gamma/Erlang distribution

use rand::SeedableRng;
use rand_distr::{Distribution, Gamma};

/// Latency profile configured by number of tasks and task completion rate.
///
/// Uses Erlang distribution to model latency as the sum of multiple independent
/// exponentially-distributed task durations.
#[derive(Debug, Clone)]
pub struct LatencyProfile {
    tasks: u64,
    task_rate: f64,

    distribution: Gamma<f64>,
    /// RNG for reproducibility
    rng: rand::rngs::StdRng,
}

impl LatencyProfile {
    /// Create a new latency profile
    ///
    /// # Arguments
    /// * `tasks` - Number of sequential tasks
    /// * `task_rate` - Tasks completed per second
    /// * `seed` - Optional seed for reproducibility
    pub fn new(tasks: u64, task_rate: f64, seed: Option<u64>) -> Self {
        // Erlang(k, λ) is equivalent to Gamma(k, 1/λ)
        // Gamma uses shape parameter k and scale parameter θ = 1/λ
        let shape = tasks as f64;
        let scale = 1.0 / task_rate;
        let distribution = Gamma::new(shape, scale).unwrap();

        let rng = match seed {
            Some(s) => rand::rngs::StdRng::seed_from_u64(s),
            None => rand::rngs::StdRng::from_os_rng(),
        };

        Self {
            tasks,
            task_rate,
            distribution,
            rng,
        }
    }

    /// Sample a duration from this latency profile
    pub fn sample(&mut self) -> tokio::time::Duration {
        let seconds = self.distribution.sample(&mut self.rng);
        tokio::time::Duration::from_secs_f64(seconds)
    }

    pub fn mean(&self) -> tokio::time::Duration {
        let mean_seconds = (self.tasks as f64) / self.task_rate;
        tokio::time::Duration::from_secs_f64(mean_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean_calculation() {
        // 2 tasks at 10/sec = 200ms average
        let profile = LatencyProfile::new(2, 10.0, Some(42));
        let mean = profile.mean();
        assert_eq!(mean.as_millis(), 200);
    }
}
