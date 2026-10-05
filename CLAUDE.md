# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A Rust workspace for batching items from concurrent async requests into a single unit of processing (e.g. many single-row `INSERT`s become one multi-row `INSERT`).

| Crate | Purpose |
| --- | --- |
| `batch-aint-one` | Core library, published to crates.io |
| `batch-aint-one-prometheus` | `MetricsRecorderFactory` implementation for Prometheus, published separately |
| `simulator` | Unpublished, WIP tool for comparing policies/limits under simulated load (throughput, latency, resource usage) |

## Commands

These mirror CI (`.github/workflows/ci.yaml`):

```sh
cargo build
cargo doc
cargo test -- --test-threads 1                     # CI runs tests single-threaded
cargo fmt --all -- --check
cargo clippy --all-features -- --deny warnings
```

Narrower runs:

```sh
cargo test -p batch-aint-one --lib worker::test     # unit tests in a module
cargo test -p batch-aint-one --test tests strategies::balanced   # integration tests
cargo test -p batch-aint-one --doc                  # includes README.md example via doc-comment
cargo test -p simulator -- --ignored test_longer_simulation   # long run, writes plots to simulator/tests/output (needs gnuplot)
```

The minimum supported Rust version is 1.87.0 (CI's `pinned` build); avoid newer language/std features.

## Architecture (core crate)

The design is a single-writer actor: one background worker task owns all batch state, and everything else talks to it through channels.

```text
Batcher::add(key, input) ──item_tx──▶ Worker (owns HashMap<Key, BatchQueue>)
        ▲                                 │  consults BatchingPolicy on each event
        │ oneshot (output, batch span)    │  spawns tasks for timeouts, resource
        └─────────────────────────────────┘  acquisition and processing
                                          ▲
                spawned tasks ──msg_tx────┘  Message::{TimedOut, ResourcesAcquired,
                                                      ResourceAcquisitionFailed, Finished}
```

- `batcher.rs`: public handle. Cheap to clone; dropping the last clone aborts the worker (`WorkerDropGuard`). Graceful shutdown goes through `WorkerHandle`.
- `worker.rs`: the event loop. Spawned tasks never mutate state directly; they report back via `Message` and the worker applies the state transition, so all events are observed in message order. Idle keys' queues are removed in `process_next_and_clean_up` to bound memory.
- `batch_queue.rs`: per-key `VecDeque<Batch>` (always at least one, possibly empty, batch) plus `processing` / `pre_acquiring` counters that enforce `Limits::max_key_concurrency`.
- `batch.rs`: per-batch state machine (`New → Acquiring → ReadyForProcessing → Processing`), items, and timeout handle. `batch_inner.rs` does the actual resource acquisition and processing in a spawned task so panics are caught.
- `Generation` (in `batch_inner.rs`) tags batches so stale timer/resource messages can be matched to the right batch. Generations restart when a key's queue is recreated, so message handlers must re-check readiness (e.g. `is_generation_ready`) rather than trust a message alone.
- `policies/`: `BatchingPolicy` is a pure decision layer. `mod.rs` handles shared logic (rejection when the queue is full, timeout/resource events) and dispatches `on_add` / `on_finish` to one file per policy (`immediate`, `size`, `duration`, `balanced`), returning `OnAdd` / `OnGenerationEvent` / `OnFinish` actions for the worker to execute. Policies have unit tests using `policies/test_utils.rs`.
- `metrics.rs`: `MetricsRecorder` trait (all methods default to no-ops) and `MetricsRecorderFactory`, which the batcher calls with its name. The worker reports gauges after each state change.
- `soft_assert!` (in `lib.rs`) is for internal invariants: it panics in debug/tests but only logs a warning in release.
- Tracing: each processed batch gets one span that `follows_from` every requesting span, and each caller gets a "batch finished" span linking back (see comments in `Batcher::add`). `test_tracing` in `lib.rs` describes this span structure, but is `#[ignore]`d as flaky, so it isn't checked in CI.

Integration tests in `batch-aint-one/tests/` are a single test binary (`tests.rs`) with modules per area (strategies, resources, errors, panic, shutdown); many use tokio's paused time.

## Conventions

- `#![deny(missing_docs)]` is on in the core crate: every public item needs a doc comment.
- The core README's Rust example is compiled and run as a doctest, so keep it in sync with the API.
- Record user-facing changes in each crate's `CHANGELOG.md` (Keep a Changelog format) under `[Unreleased]`.
- Releases bump both crates together (commit titled e.g. `0.15.1 / 0.2.1`), including the prometheus crate's `batch-aint-one` dependency version. CI publishes to crates.io automatically on push to `main`, so a version bump landing on `main` is a release.
- `tmp/` is git-ignored and holds local design/plan docs.
