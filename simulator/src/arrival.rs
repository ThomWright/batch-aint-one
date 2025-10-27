//! Arrival pattern implementations (Poisson, etc.)

use rand::SeedableRng;
use rand_distr::{Distribution, Exp};

/// Poisson arrival pattern generator
///
/// Generates inter-arrival times following an exponential distribution,
/// which results in Poisson-distributed arrivals.
pub struct PoissonArrivals {
    /// Mean arrival rate (items per second)
    rate: f64,
    /// Exponential distribution for inter-arrival times
    exp_dist: Exp<f64>,
    /// RNG for reproducibility
    rng: rand::rngs::StdRng,
}

impl PoissonArrivals {
    /// Create a new Poisson arrival pattern
    ///
    /// # Arguments
    /// * `rate` - Mean arrival rate in items per second
    /// * `seed` - Optional seed for reproducibility
    pub fn new(rate: f64, seed: Option<u64>) -> Self {
        let exp_dist = Exp::new(rate).unwrap();
        let rng = match seed {
            Some(s) => rand::rngs::StdRng::seed_from_u64(s),
            None => rand::rngs::StdRng::from_os_rng(),
        };

        Self {
            rate,
            exp_dist,
            rng,
        }
    }

    /// Get the mean arrival rate
    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// Sample the next inter-arrival time
    pub fn next_inter_arrival_time(&mut self) -> tokio::time::Duration {
        let seconds = self.exp_dist.sample(&mut self.rng);
        tokio::time::Duration::from_secs_f64(seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean_inter_arrival_time() {
        let rate = 10.0; // 10 items/sec = 100ms average inter-arrival
        let mut arrivals = PoissonArrivals::new(rate, Some(42));

        // Sample many times and check mean is close to expected
        let samples: Vec<_> = (0..1000)
            .map(|_| arrivals.next_inter_arrival_time().as_secs_f64())
            .collect();

        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let expected = 1.0 / rate;

        // Should be within 10% of expected (with 1000 samples)
        let tolerance = expected * 0.1;
        assert!(
            (mean - expected).abs() < tolerance,
            "Mean {:.4} not within {:.4} of expected {:.4}",
            mean,
            tolerance,
            expected
        );
    }

    #[test]
    fn test_reproducibility() {
        let mut arrivals1 = PoissonArrivals::new(10.0, Some(42));
        let mut arrivals2 = PoissonArrivals::new(10.0, Some(42));

        for _ in 0..10 {
            let t1 = arrivals1.next_inter_arrival_time();
            let t2 = arrivals2.next_inter_arrival_time();
            assert_eq!(t1, t2, "Same seed should produce same sequence");
        }
    }
}
