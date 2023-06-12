use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    panic::{RefUnwindSafe, UnwindSafe},
    sync::Arc,
};

use async_task::Runnable;
use crossbeam_queue::SegQueue;
use futures_lite::future;
use once_cell::unsync::OnceCell;

use super::io::Poller;
use crate::deque::Taker;

pub(crate) const NR_TASKS: usize = 256;

thread_local! {
    pub(crate) static CONTEXT: OnceCell<Context> = OnceCell::new();
}

#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) id: usize,
    pub(crate) local: RefCell<VecDeque<Runnable>>,
    pub(crate) assign: Arc<SegQueue<Runnable>>,
    pub(crate) global: Taker<Runnable>,
    pub(crate) poller: RefCell<Poller>,
    pub(crate) waker: Arc<mio::Waker>,
}

impl Context {
    fn new(
        id: usize,
        assign: Arc<SegQueue<Runnable>>,
        global: Taker<Runnable>,
        poller: Poller,
        waker: Arc<mio::Waker>,
    ) -> Self {
        Context {
            id,
            local: RefCell::new(VecDeque::new()),
            assign,
            global,
            poller: RefCell::new(poller),
            waker,
        }
    }
}

pub(crate) struct Worker;

impl UnwindSafe for Worker {}
impl RefUnwindSafe for Worker {}

impl Worker {
    pub(crate) fn new(
        id: usize,
        assign: Arc<SegQueue<Runnable>>,
        global: Taker<Runnable>,
        poller: Poller,
        waker: Arc<mio::Waker>,
    ) -> Self {
        CONTEXT.with(|context| {
            context
                .set(Context::new(id, assign, global, poller, waker))
                .expect("context can not be setted twice")
        });
        Worker
    }

    pub(crate) async fn run(&self, future: impl Future<Output = ()>) {
        // A future that runs tasks forever.
        let run_forever = async move {
            loop {
                CONTEXT.with(|context| {
                    let context = context.get().expect("context should be initialized");
                    let mut capacity = NR_TASKS;

                    while capacity > 0 {
                        let runnable = context.local.borrow_mut().pop_front();
                        if let Some(runnable) = runnable {
                            runnable.run();
                            capacity -= 1;
                        } else {
                            break;
                        }
                    }

                    while capacity > 0 {
                        if let Some(runnable) = context.assign.pop() {
                            runnable.run();
                            capacity -= 1;
                        } else {
                            break;
                        }
                    }

                    while capacity > 0 {
                        if let Some(runnable) = context.global.pop() {
                            runnable.run();
                            capacity -= 1;
                        } else {
                            break;
                        }
                    }
                });

                future::yield_now().await;

                {
                    use std::time::Duration;

                    CONTEXT.with(|context| {
                        let context = context.get().expect("context should be initialized");
                        let timeout =
                            if context.local.borrow().is_empty() && context.assign.is_empty() {
                                None
                            } else {
                                Some(Duration::ZERO)
                            };
                        context
                            .poller
                            .borrow_mut()
                            .poll(timeout)
                            .unwrap_or_else(|e| {
                                tracing::error!("async worker polling failed: {}", e)
                            });
                    })
                }
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future::or(future, run_forever).await;
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        CONTEXT.with(|context| {
            let context = context.get().expect("context should be initialized");
            while let Some(runnable) = context.assign.pop() {
                drop(runnable);
            }
            while let Some(runnable) = context.local.borrow_mut().pop_front() {
                drop(runnable);
            }
        });
    }
}

#[cfg(test)]
mod test {
    use core::future::Future;
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use async_task::Task;
    use crossbeam_queue::SegQueue;
    use futures_lite::{future, future::yield_now};

    use super::{Worker, CONTEXT};
    use crate::{deque::Deque, io::Poller};

    fn spawn_local<T>(future: impl Future<Output = T>) -> Task<T> {
        let schedule = move |runnable| {
            CONTEXT.with(|context| {
                context
                    .get()
                    .unwrap()
                    .local
                    .borrow_mut()
                    .push_back(runnable);
            });
        };
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) };
        runnable.schedule();
        task
    }

    #[test]
    fn worker_could_run() {
        let mut poller = Poller::with_capacity(1).unwrap();
        let waker = Arc::new(poller.waker().unwrap());
        let global = Deque::new(1).take(0);
        let ex = Worker::new(0, Arc::new(SegQueue::new()), global, poller, waker);

        let task = spawn_local(async { 1 + 2 });
        future::block_on(ex.run(async {
            let res = task.await * 2;
            assert_eq!(res, 6);
        }));
    }

    #[test]
    fn task_coud_be_yielded() {
        let mut poller = Poller::with_capacity(1).unwrap();
        let waker = Arc::new(poller.waker().unwrap());
        let global = Deque::new(1).take(0);
        let ex = Worker::new(0, Arc::new(SegQueue::new()), global, poller, waker);

        let counter = Rc::new(RefCell::new(0));
        let counter1 = Rc::clone(&counter);
        let task = spawn_local(async {
            {
                let mut c = counter1.borrow_mut();
                assert_eq!(*c, 0);
                *c = 1;
            }
            let counter_clone = Rc::clone(&counter1);
            let t = spawn_local(async {
                {
                    let mut c = counter_clone.borrow_mut();
                    assert_eq!(*c, 1);
                    *c = 2;
                }
                yield_now().await;
                {
                    let mut c = counter_clone.borrow_mut();
                    assert_eq!(*c, 3);
                    *c = 4;
                }
            });
            yield_now().await;
            {
                let mut c = counter1.borrow_mut();
                assert_eq!(*c, 2);
                *c = 3;
            }
            t.await;
        });
        future::block_on(ex.run(task));
        assert_eq!(*counter.as_ref().borrow(), 4);
    }
}
