# Batch ain't one

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/ThomWright/batch-aint-one/ci.yaml?branch=main)
![GitHub](https://img.shields.io/github/license/ThomWright/batch-aint-one)
![Crates.io](https://img.shields.io/crates/v/batch-aint-one)
![docs.rs](https://img.shields.io/docsrs/batch-aint-one)

_I got 99 problems, but a batch ain't one..._

Batch up multiple items for processing as a single unit.

## Why

TODO:

## Example

```rust
use std::{marker::Send, sync::Arc};

use async_trait::async_trait;
use batch_aint_one::{
    limit::SizeLimit,
    Batch, BatcherBuilder, Processor,
};

#[derive(Debug, Clone)]
struct SimpleBatchProcessor;

#[async_trait]
impl Processor<String, String> for SimpleBatchProcessor {
    async fn process(&self, inputs: impl Iterator<Item = String> + Send) -> Vec<String> {
        inputs.map(|s| s + " processed").collect()
    }
}

tokio_test::block_on(async {
    let batcher = Arc::new(BatcherBuilder::new(SimpleBatchProcessor)
        .with_limit(SizeLimit::new(2))
        .build());

    // Request handler 1
    let b1 = batcher.clone();
    tokio::spawn(async move {
        let fo1 = b1.add("A".to_string(), "1".to_string()).await.unwrap();

        assert_eq!("1 processed".to_string(), fo1.await.unwrap());

    });

    // Request handler 2
    let b2 = batcher.clone();
    tokio::spawn(async move {
        let fo2 = b2.add("A".to_string(), "2".to_string()).await.unwrap();

        assert_eq!("2 processed".to_string(), fo2.await.unwrap());
    });
});
```

## Roadmap

- [x] Tests
- [x] Better error handling
- [ ] Garbage collection for old generation placeholders
- [ ] Docs
  - [ ] Why – motivating example
  - [x] Code examples
- [x] Observability
