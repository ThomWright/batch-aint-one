use std::time::Duration;

use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, OnFull, ProcessNextAction};

/// Duration policy: Process batches after a given duration or when full.
///
/// Prioritises regularity.

pub(super) fn on_add<P: Processor>(
    duration: Duration,
    on_full: OnFull,
    batch_queue: &BatchQueue<P>,
    add_or_process: impl FnOnce(&BatchQueue<P>) -> OnAdd,
) -> OnAdd {
    if batch_queue.last_space_in_batch() {
        if matches!(on_full, OnFull::Process) {
            add_or_process(batch_queue)
        } else {
            OnAdd::Add
        }
    } else if batch_queue.adding_to_new_batch() {
        OnAdd::AddAndProcessAfter(duration)
    } else {
        OnAdd::Add
    }
}

pub(super) fn on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
    if batch_queue.has_next_batch_timeout_expired() {
        ProcessNextAction::ProcessNext
    } else if batch_queue.is_next_batch_full() {
        ProcessNextAction::ProcessNext
    } else {
        ProcessNextAction::DoNothing
    }
}
