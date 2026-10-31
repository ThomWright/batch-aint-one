use batcher_harness::{config, op, run};
use proptest::{prelude::*, test_runner::FileFailurePersistence};

proptest! {
    #![proptest_config(proptest_config(256))]

    #[test]
    fn batcher_behaves_as_documented(config in config(), ops in prop::collection::vec(op(), 0..50)) {
        run(config, ops);
    }
}

/// A longer run, for soak testing locally.
#[test]
#[ignore]
fn batcher_behaves_as_documented_for_longer_sequences() {
    proptest!(proptest_config(10_000), |(
        config in config(),
        ops in prop::collection::vec(op(), 0..500),
    )| {
        run(config, ops);
    });
}

fn proptest_config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        // The default looks for the crate's `lib.rs`, which integration tests don't have.
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
        ..ProptestConfig::default()
    }
}
