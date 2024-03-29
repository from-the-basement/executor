use std::{
    future::Future,
    panic::{RefUnwindSafe, UnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use async_task::{Runnable, Task};
use crossbeam_deque::Injector;
use crossbeam_queue::SegQueue;
use event_listener::Event;
use futures_lite::future;
use futures_util::future::join_all;

use crate::{
    blocking,
    context::{self, Context, CONTEXT},
    deque::{Deque, Taker},
    io::Poller,
    EXECUTOR,
};

pub struct ExecutorBuilder {
    worker_num: usize,
    max_blocking_thread_num: usize,
}

impl Default for ExecutorBuilder {
    fn default() -> Self {
        Self {
            worker_num: std::thread::available_parallelism().unwrap().get(),
            max_blocking_thread_num: 512,
        }
    }
}

impl ExecutorBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn worker_num(self, num: usize) -> Self {
        Self {
            worker_num: num,
            max_blocking_thread_num: self.max_blocking_thread_num,
        }
    }

    pub fn max_blocking_thread_num(self, num: usize) -> Self {
        Self {
            worker_num: self.worker_num,
            max_blocking_thread_num: num,
        }
    }

    pub fn build(self) -> std::io::Result<Executor> {
        Executor::new(self.worker_num, self.max_blocking_thread_num)
    }
}

#[derive(Debug)]
pub(crate) struct WorkerHandler {
    join: JoinHandle<()>,
    pub(crate) assign: Arc<SegQueue<Runnable>>,
    pub(crate) waker: Arc<mio::Waker>,
}

#[derive(Debug)]
pub struct Executor {
    workers: Vec<WorkerHandler>,
    closer: Arc<Event>,
    global: Arc<Injector<Runnable>>,
    next_steal: Arc<AtomicUsize>,
}

unsafe impl Send for Executor {}
unsafe impl Sync for Executor {}

impl UnwindSafe for Executor {}
impl RefUnwindSafe for Executor {}

impl Drop for Executor {
    fn drop(&mut self) {
        self.closer.notify(self.workers.len());
        for worker in self.workers.drain(..) {
            worker
                .waker
                .wake()
                .expect("wake worker to accpet spawned task must be successed");
            worker
                .join
                .join()
                .expect("join worker should successfully exit");
        }
    }
}

impl Executor {
    pub fn builder() -> ExecutorBuilder {
        ExecutorBuilder::new()
    }

    fn new(worker_num: usize, max_blocking_thread_num: usize) -> std::io::Result<Self> {
        let closer = Arc::new(Event::new());
        let mut workers = Vec::with_capacity(worker_num);

        let mut deque = Deque::new(worker_num);

        for worker_id in 0..worker_num {
            let assign = Arc::new(SegQueue::new());
            let closer = closer.clone();

            workers.push(Self::create_worker_handler(
                worker_id,
                assign,
                closer,
                deque.take(worker_id),
            )?);
        }

        blocking::BLOCKING_EXECUTOR
            .get_or_init(|| blocking::Executor::new(max_blocking_thread_num));

        Ok(Self {
            workers,
            closer,
            global: deque.global(),
            next_steal: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn create_worker_handler(
        worker_id: usize,
        assign: Arc<SegQueue<Runnable>>,
        closer: Arc<Event>,
        global: Taker<Runnable>,
    ) -> std::io::Result<WorkerHandler> {
        use context::NR_TASKS;

        let a = assign.clone();
        let mut poller = Poller::with_capacity(NR_TASKS)?;
        let poller_waker = Arc::new(poller.waker()?);
        let pw = poller_waker.clone();

        let join = thread::Builder::new()
            .name(format!("async-worker-{}", worker_id))
            .spawn(move || {
                CONTEXT.with(move |context| {
                    context
                        .set(Context::new(worker_id, a, global, poller, pw))
                        .unwrap();

                    future::block_on(context.get().unwrap().run(async move {
                        closer.listen().await;
                    }));
                });
            })?;

        Ok(WorkerHandler {
            join,
            assign,
            waker: poller_waker,
        })
    }
}

impl Executor {
    pub fn run<MakeF, F>(self, maker: MakeF) -> Vec<F::Output>
    where
        F: Future,
        F::Output: Send,
        MakeF: FnOnce() -> F + Clone + Send,
    {
        Arc::new(self).init_workers(maker)
    }

    pub fn block_on<F>(self, future: F) -> F::Output
    where
        F: Future,
    {
        let executor = Arc::new(self);
        executor.init_workers(|| async {});
        future::block_on(async move {
            EXECUTOR.with(|ex| ex.set(executor).expect("set global executor must be ok"));
            future.await
        })
    }

    pub fn spawn_local<F>(&self, future: F) -> Task<F::Output>
    where
        F: 'static + Future,
        F::Output: 'static,
    {
        let (owned_thread, assign, waker) = CONTEXT.with(|context| {
            let context = context.get().expect("context should be initialized");
            (context.id, context.assign.clone(), context.waker.clone())
        });
        let schedule = schedule(owned_thread, assign, waker);

        let (runnable, task) = unsafe {
            async_task::Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(move |_| future, schedule)
        };

        runnable.schedule();

        task
    }

    pub fn spawn_to<F, Fut>(&self, to: usize, f: F) -> Task<Fut::Output>
    where
        F: 'static + Send + FnOnce() -> Fut,
        Fut: 'static + Future,
        Fut::Output: 'static + Send,
    {
        let handler = &self.workers[to];
        let schedule = schedule(to, handler.assign.clone(), handler.waker.clone());

        let (runnable, task) = unsafe {
            async_task::Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(move |_| async move { f().await }, schedule)
        };

        runnable.schedule();

        task
    }

    pub fn spawn<F>(self: &Arc<Self>, future: F) -> Task<F::Output>
    where
        F: 'static + Future + Send,
        F::Output: 'static + Send,
    {
        let executor = self.clone();
        let (runnable, task) = unsafe {
            async_task::Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(
                    |_| future,
                    move |runnable| {
                        CONTEXT.with(|context| {
                            if let Some(context) = context.get() {
                                context.global.push(runnable);
                            } else {
                                executor.global.push(runnable);
                                executor.workers[executor
                                    .next_steal
                                    .fetch_add(1, Ordering::Relaxed)
                                    % executor.worker_num()]
                                .waker
                                .wake()
                                .expect("wake worker to accpet spawned task must be successed");
                            }
                        });
                    },
                )
        };

        runnable.schedule();

        task
    }

    pub fn worker_num(&self) -> usize {
        self.workers.len()
    }

    fn init_workers<MakeF, F>(self: &Arc<Self>, maker: MakeF) -> Vec<F::Output>
    where
        F: Future,
        F::Output: Send,
        MakeF: FnOnce() -> F + Clone + Send,
    {
        future::block_on(join_all(self.workers.iter().enumerate().map(
            |(id, worker)| {
                let assign = worker.assign.clone();
                let maker = maker.clone();
                let executor = self.clone();
                let scoped = move || {
                    EXECUTOR.with(|ex| ex.set(executor).expect("set global executor must be ok"));
                    maker()
                };

                let schedule = schedule(id, assign, worker.waker.clone());

                let (runnable, task) = unsafe {
                    async_task::Builder::new()
                        .propagate_panic(true)
                        .spawn_unchecked(move |_| async move { scoped().await }, schedule)
                };
                runnable.schedule();

                worker
                    .waker
                    .wake()
                    .expect("wake worker to accpet spawned task must be successed");

                task
            },
        )))
    }
}

fn schedule(
    owned_thread: usize,
    assign: Arc<SegQueue<Runnable>>,
    waker: Arc<mio::Waker>,
) -> impl Fn(Runnable) {
    move |runnable| {
        CONTEXT.with(|context| {
            if let Some(context) = context.get() {
                if context.id == owned_thread {
                    context.local.borrow_mut().push_back(runnable);
                    return;
                }
            }

            assign.push(runnable);
            waker
                .wake()
                .expect("wake worker by task scheduling must be ok");
        })
    }
}
