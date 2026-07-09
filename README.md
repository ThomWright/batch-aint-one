# Batch ain't one

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/ThomWright/batch-aint-one/ci.yaml?branch=main)](https://github.com/ThomWright/batch-aint-one/actions/workflows/ci.yaml)
[![License](https://img.shields.io/github/license/ThomWright/batch-aint-one)](https://github.com/ThomWright/batch-aint-one/blob/main/LICENSE-MIT)

_I got 99 problems, but a batch ain't one..._

Batch up multiple items for processing as a single unit.

## Crates

| Crate | Description |
|---|---|
| [`batch-aint-one`](./batch-aint-one) | Core batching library. [![Crates.io](https://img.shields.io/crates/v/batch-aint-one)](https://crates.io/crates/batch-aint-one) [![docs.rs](https://img.shields.io/docsrs/batch-aint-one)](https://docs.rs/batch-aint-one) |
| [`batch-aint-one-prometheus`](./batch-aint-one-prometheus) | Prometheus metrics integration. [![Crates.io](https://img.shields.io/crates/v/batch-aint-one-prometheus)](https://crates.io/crates/batch-aint-one-prometheus) [![docs.rs](https://img.shields.io/docsrs/batch-aint-one-prometheus)](https://docs.rs/batch-aint-one-prometheus) |

## Simulator

The [`simulator`](./simulator) directory contains a tool for testing different batching strategies and configurations in a controlled environment, measuring throughput, latency, and resource utilisation.
