mod strategies;
mod types;

#[macro_export]
macro_rules! assert_elapsed {
    ($start:expr, $dur:expr, $tolerance:expr) => {{
        let elapsed = $start.elapsed();
        let lower: std::time::Duration = $dur;

        // Handles ms rounding
        assert!(
            elapsed >= lower && elapsed <= lower + $tolerance,
            "actual = {:?}, expected = {:?}",
            elapsed,
            lower
        );
    }};
}

#[macro_export]
macro_rules! assert_duration {
    ($actual:expr, $expected:expr, $tolerance:expr) => {{
        let lower: std::time::Duration = $expected;

        // Handles ms rounding
        assert!(
            $actual >= lower && $actual <= lower + $tolerance,
            "actual = {:?}, expected = {:?}",
            $actual,
            lower
        );
    }};
}
