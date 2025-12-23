use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, ProcessNextAction};

/// Immediate policy: Process batches as soon as resources are available.
///
/// Prioritises low latency by processing items immediately when possible.

pub(super) fn on_add<P: Processor>(batch_queue: &BatchQueue<P>) -> OnAdd {
    if batch_queue.at_max_total_processing_capacity() {
        OnAdd::Add
    } else if batch_queue.adding_to_new_batch() {
        OnAdd::AddAndAcquireResources
    } else {
        OnAdd::Add
    }
}

pub(super) fn on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
    if batch_queue.has_batch_ready() {
        ProcessNextAction::ProcessNextReady
    } else {
        ProcessNextAction::DoNothing
    }
}
