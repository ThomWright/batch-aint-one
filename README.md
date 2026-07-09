# Batch ain't one

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/ThomWright/batch-aint-one/ci.yaml?branch=main)](https://github.com/ThomWright/batch-aint-one/actions/workflows/ci.yaml)
[![License](https://img.shields.io/github/license/ThomWright/batch-aint-one)](https://github.com/ThomWright/batch-aint-one/blob/main/LICENSE-MIT)

_I got 99 problems, but a batch ain't one..._

Batch up multiple items from concurrent requests for processing as a single unit.

## Crates

| Crate                                                      | Description                                                                                                           |
|------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| [`batch-aint-one`](./batch-aint-one)                       | Core batching library. [![crates.io][bao-crate-badge]][bao-crate] [![docs.rs][bao-docs-badge]][bao-docs]              |
| [`batch-aint-one-prometheus`](./batch-aint-one-prometheus) | Prometheus metrics integration. [![crates.io][prom-crate-badge]][prom-crate] [![docs.rs][prom-docs-badge]][prom-docs] |

[bao-crate-badge]: https://img.shields.io/crates/v/batch-aint-one
[bao-crate]: https://crates.io/crates/batch-aint-one
[bao-docs-badge]: https://img.shields.io/docsrs/batch-aint-one
[bao-docs]: https://docs.rs/batch-aint-one
[prom-crate-badge]: https://img.shields.io/crates/v/batch-aint-one-prometheus
[prom-crate]: https://crates.io/crates/batch-aint-one-prometheus
[prom-docs-badge]: https://img.shields.io/docsrs/batch-aint-one-prometheus
[prom-docs]: https://docs.rs/batch-aint-one-prometheus

## Simulator

The [`simulator`](./simulator) directory contains a tool for testing different batching strategies and configurations in a controlled environment, measuring throughput, latency, and resource utilisation.
