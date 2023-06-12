use std::{iter, sync::Arc};

use crossbeam_deque::{Injector, Stealer, Worker};

#[derive(Debug)]
pub(crate) struct Taker<T> {
    local: Worker<T>,
    stealers: Vec<Stealer<T>>,
    global: Arc<Injector<T>>,
}

impl<T> Taker<T> {
    pub(crate) fn pop(&self) -> Option<T> {
        // Pop a task from the local queue, if not empty.
        self.local.pop().or_else(|| {
            // Otherwise, we need to look for a task elsewhere.
            iter::repeat_with(|| {
                // Try stealing a batch of tasks from the global queue.
                self.global
                    .steal_batch_and_pop(&self.local)
                    // Or try stealing a task from one of the other threads.
                    .or_else(|| self.stealers.iter().map(|s| s.steal()).collect())
            })
            // Loop while no task was stolen and any steal operation needs to be retried.
            .find(|s| !s.is_retry())
            // Extract the stolen task, if there is one.
            .and_then(|s| s.success())
        })
    }

    pub(crate) fn push(&self, task: T) {
        self.local.push(task);
    }
}

pub(crate) struct Deque<T> {
    locals: Vec<Option<Worker<T>>>,
    global: Arc<Injector<T>>,
    stealers: Vec<Stealer<T>>,
}

impl<T> Deque<T> {
    pub(crate) fn new(size: usize) -> Self {
        let locals: Vec<_> = (0..size).map(|_| Some(Worker::new_fifo())).collect();
        let stealers = locals
            .iter()
            .map(|w| w.as_ref().unwrap().stealer())
            .collect();
        Self {
            locals,
            global: Arc::new(Injector::new()),
            stealers,
        }
    }

    pub(crate) fn take(&mut self, id: usize) -> Taker<T> {
        let mut local = None;
        std::mem::swap(&mut self.locals[id], &mut local);
        Taker {
            local: local.expect("worker is already taken"),
            stealers: self
                .stealers
                .iter()
                .cloned()
                .enumerate()
                .filter(|(i, _)| *i != id)
                .map(|(_, s)| s)
                .collect(),
            global: self.global.clone(),
        }
    }

    pub(crate) fn global(self) -> Arc<Injector<T>> {
        self.global
    }
}
