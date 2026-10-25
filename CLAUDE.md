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
        ▲                                 │  consults BatchingPolicy on each event,
        │ oneshot (output, batch span)    │  waits for batch deadlines in its loop,
        └─────────────────────────────────┘  spawns tasks for resource acquisition
                                          ▲  and processing
                spawned tasks ──msg_tx────┘  Message::{ResourcesAcquired,
                                                      ResourceAcquisitionFailed, Finished}
```

- `batcher.rs`: public handle. Cheap to clone; when the last clone is dropped, the item channel closes and the worker drops unstarted batches, waits for in-flight work (`Worker::finish_in_flight`), then exits. Graceful shutdown goes through `WorkerHandle`.
- `worker.rs`: the event loop. The module doc explains why tasks are only spawned for user code, and why the worker never cancels them. Spawned tasks never mutate state directly; they report back via `Message` and the worker applies the state transition, so all events are observed in message order. After each event for a key, `start_batches` asks the policy whether to start batches, and when to wake up; the `wake_ups` map only says when to re-check a key, so stale entries are harmless. Idle keys' queues are removed in `remove_if_idle` to bound memory.
- `batch_queue.rs`: per-key FIFO of items, plus `acquiring` / `processing` counters that enforce `Limits::max_key_concurrency`. Every batch starts by acquiring resources, without taking any items; when an acquisition reports back, it takes up to `max_batch_size` of the oldest items, so acquisitions aren't tied to items.
- `batch.rs`: a batch exists only once it's processing: its items, owned by the processing task. `user_code.rs` calls `Processor::acquire_resources` and `Processor::process`, each in its own spawned task so panics are caught.
- `policies/mod.rs`: `BatchingPolicy` is a pure decision layer over a key's `KeyState` (item and in-flight counts) and the triggering `Event`: whether to reject an item, whether to start a batch, and when to wake up. Items an acquisition will take are "covered"; policies start batches for uncovered items.
- `metrics.rs`: `MetricsRecorder` trait (all methods default to no-ops) and `MetricsRecorderFactory`, which the batcher calls with its name. The worker records gauges when the recorder's `gauge_recording()` says to.
- `soft_assert!` (in `lib.rs`) is for internal invariants: it panics in debug/tests but only logs a warning in release.
- Tracing: each processed batch gets one span that `follows_from` every requesting span, and each caller gets a "batch finished" span linking back (see comments in `Batcher::add`). `tests/tracing.rs` pins this span structure. It must stay in its own test binary, because tracing-core can cache a callsite as disabled when another test thread hits it first (tokio-rs/tracing#2874).

Integration tests in `batch-aint-one/tests/` are mostly one test binary (`tests.rs`) with modules per area (strategies, resources, errors, panic, shutdown); many use tokio's paused time. `tracing.rs` is separate (see above).

## Conventions

- `#![deny(missing_docs)]` is on in the core crate: every public item needs a doc comment.
- The core README's Rust example is compiled and run as a doctest, so keep it in sync with the API.
- Record user-facing changes in each crate's `CHANGELOG.md` (Keep a Changelog format) under `[Unreleased]`.
- Releases bump both crates together (commit titled e.g. `0.15.1 / 0.2.1`), including the prometheus crate's `batch-aint-one` dependency version. CI publishes to crates.io automatically on push to `main`, so a version bump landing on `main` is a release.
- `tmp/` is git-ignored and holds local design/plan docs.
