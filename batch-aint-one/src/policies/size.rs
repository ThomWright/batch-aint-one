use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, ProcessNextAction};

/// Size policy: Process batches when they reach the maximum size.
///
/// Prioritises high batch utilisation.

pub(super) fn on_add<P: Processor>(
    batch_queue: &BatchQueue<P>,
    add_or_process: impl FnOnce(&BatchQueue<P>) -> OnAdd,
) -> OnAdd {
    if batch_queue.last_space_in_batch() {
        add_or_process(batch_queue)
    } else {
        OnAdd::Add
    }
}

pub(super) fn on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
    if batch_queue.is_next_batch_full() {
        ProcessNextAction::ProcessNext
    } else {
        ProcessNextAction::DoNothing
    }
}
