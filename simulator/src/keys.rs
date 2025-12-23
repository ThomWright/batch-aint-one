//! Key distribution for multi-key scenarios

use rand::Rng;
use rand::SeedableRng;
use rand_distr::{Distribution, Zipf};

/// Distribution of keys for a scenario
#[derive(Debug, Clone)]
pub enum KeyDistributionConfig {
    /// Always use the same key
    Single,
    /// Uniform random selection across num_keys
    Uniform { num_keys: usize },
    /// Zipfian distribution (power law)
    /// - num_keys: number of distinct keys
    /// - s: exponent parameter (typically 0.5 to 2.0)
    ///   - s=1.0 is classic Zipf (80/20-ish)
    ///   - Higher s = more skew toward popular keys
    Zipf { num_keys: usize, s: f64 },
}

enum KeyDistribution {
    Single,
    Uniform { num_keys: usize },
    Zipf { zipf: Zipf<f64> },
}

/// Generator for producing keys according to a distribution
pub struct KeyGenerator {
    distribution: KeyDistribution,
    rng: rand::rngs::StdRng,
}

impl KeyGenerator {
    /// Create a new key generator
    pub fn new(distribution: KeyDistributionConfig, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => rand::rngs::StdRng::seed_from_u64(s),
            None => rand::rngs::StdRng::from_os_rng(),
        };

        Self {
            distribution: distribution.into(),
            rng,
        }
    }

    /// Generate the next key
    pub fn next_key(&mut self) -> String {
        match &self.distribution {
            KeyDistribution::Single => "1".to_string(),
            KeyDistribution::Uniform { num_keys } => {
                let idx = self.rng.random_range(1..=*num_keys);
                idx.to_string()
            }
            KeyDistribution::Zipf { zipf } => {
                // Zipf returns values from 1..=n, convert to 0-indexed
                let sample = zipf.sample(&mut self.rng) as usize;
                sample.to_string()
            }
        }
    }
}

impl From<KeyDistributionConfig> for KeyDistribution {
    fn from(config: KeyDistributionConfig) -> Self {
        match config {
            KeyDistributionConfig::Single => KeyDistribution::Single,
            KeyDistributionConfig::Uniform { num_keys } => KeyDistribution::Uniform { num_keys },
            KeyDistributionConfig::Zipf { num_keys, s } => KeyDistribution::Zipf {
                zipf: Zipf::new(num_keys as f64, s).expect("Invalid Zipf parameters"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::test_utils::TEST_SEED;

    #[test]
    fn test_single_key() {
        let mut key_gen = KeyGenerator::new(KeyDistributionConfig::Single, Some(*TEST_SEED));

        for _ in 0..100 {
            assert_eq!(key_gen.next_key(), "1");
        }
    }

    #[test]
    fn test_uniform_distribution() {
        let num_keys = 10;
        let mut key_gen = KeyGenerator::new(
            KeyDistributionConfig::Uniform { num_keys },
            Some(*TEST_SEED),
        );

        let mut counts: HashMap<String, usize> = HashMap::new();
        let samples = 10_000;

        for _ in 0..samples {
            let key = key_gen.next_key();
            *counts.entry(key).or_insert(0) += 1;
        }

        // Should see all keys
        assert_eq!(counts.len(), num_keys);

        // Each key should appear roughly samples/num_keys times
        let expected = samples / num_keys;
        for (_, count) in counts {
            let ratio = count as f64 / expected as f64;
            // Allow 20% deviation
            assert!(ratio > 0.8 && ratio < 1.2, "Ratio: {}", ratio);
        }
    }

    #[test]
    fn test_zipf_distribution() {
        let num_keys = 10;
        let mut key_gen = KeyGenerator::new(
            KeyDistributionConfig::Zipf { num_keys, s: 1.0 },
            Some(*TEST_SEED),
        );

        let mut counts: HashMap<String, usize> = HashMap::new();
        let samples = 10_000;

        for _ in 0..samples {
            let key = key_gen.next_key();
            *counts.entry(key).or_insert(0) += 1;
        }

        // Should see all keys (or most)
        assert!(
            counts.len() >= num_keys / 2,
            "Should see at least half the keys"
        );

        // Key "1" should be most popular (Zipf returns 1-indexed)
        let key1_count = counts.get("1").copied().unwrap_or(0);
        for (key, count) in &counts {
            if key != "1" {
                assert!(
                    key1_count >= *count,
                    "Key 1 should be most popular, but key {} has more: {} vs {}",
                    key,
                    count,
                    key1_count
                );
            }
        }

        // Top key should have significantly more than average
        let avg = samples / num_keys;
        assert!(
            key1_count > avg * 2,
            "Top key should have >2x average, got {} vs avg {}",
            key1_count,
            avg
        );
    }
}
