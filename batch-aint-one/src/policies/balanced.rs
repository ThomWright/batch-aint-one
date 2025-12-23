use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, ProcessNextAction};

/// Balanced policy: Balance between resource efficiency and latency based on system load.
///
/// Prioritises efficient resource usage while maintaining reasonable latency.

pub(super) fn on_add<P: Processor>(
    min_size_hint: usize,
    batch_queue: &BatchQueue<P>,
) -> OnAdd {
    if batch_queue.at_max_total_processing_capacity() {
        OnAdd::Add
    } else if batch_queue.adding_to_new_batch() && !batch_queue.is_processing() {
        // First item, nothing else processing
        OnAdd::AddAndAcquireResources
    } else if batch_queue.has_last_batch_reached_size(min_size_hint.saturating_sub(1))
        && !batch_queue.is_last_batch_acquiring_resources()
    {
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
