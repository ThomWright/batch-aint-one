# Batcher harness

Checks that `batch-aint-one`'s `Batcher` does what its docs say, under generated sequences of operations. Unpublished.

The batcher reacts to many interleaved events: items arriving, callers giving up, acquisitions and batches finishing or failing, deadlines passing, and shutdown. Its bugs tend to need a particular ordering of them. Generated sequences cover far more orderings than hand-written tests can.

## How it works

Each case is a configuration (batching policy, small limits, a few keys) and a sequence of operations:

| Operation | Effect |
| --- | --- |
| `Add` | Add an item for a key. |
| `GiveUp` | Stop waiting for a pending add. |
| `Release` | Let a held `acquire_resources()` or `process()` call return `Ok`, an error, a panic, or the wrong number of outputs. |
| `Advance` | Let time pass (tokio's paused clock, so runs are deterministic). |
| `ShutDown` | Shut the worker down. |

The test processor holds every call until a `Release`, so the sequence decides the order things finish in. After every step the harness checks the batcher against its documented behaviour: concurrency and batch size limits, oldest items first, rejections only when the queue is full, every caller's result, and that each key starts a batch exactly when its policy allows. A failed check panics.

## Running

```sh
cargo test -p batcher-harness                # 256 proptest cases, runs in CI
cargo test -p batcher-harness -- --ignored   # soak run: 10,000 longer cases, ~8 minutes
```

proptest shrinks a failing case to a minimal sequence and saves its seed under `tests/`, so the next run replays it first.
