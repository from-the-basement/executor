use std::{future::Future, sync::Arc, thread, thread::JoinHandle};

use async_task::{Runnable, Task};
use crossbeam_deque::Injector;
use crossbeam_queue::SegQueue;
use event_listener::Event;
use futures_lite::future;
use futures_util::future::join_all;

use crate::{
    blocking,
    deque::{Deque, Taker},
    io::Poller,
    worker,
    worker::{Worker, CONTEXT},
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
            max_blocking_thread_num: usize::MAX,
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
    pub(crate) global: Arc<Injector<Runnable>>,
}

unsafe impl Send for Executor {}
unsafe impl Sync for Executor {}

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
        })
    }

    fn create_worker_handler(
        worker_id: usize,
        assign: Arc<SegQueue<Runnable>>,
        closer: Arc<Event>,
        global: Taker<Runnable>,
    ) -> std::io::Result<WorkerHandler> {
        use worker::NR_TASKS;

        let a = assign.clone();
        let mut poller = Poller::with_capacity(NR_TASKS)?;
        let poller_waker = Arc::new(poller.waker()?);
        let pw = poller_waker.clone();

        let join = thread::Builder::new()
            .name(format!("async-worker-{}", worker_id))
            .spawn(move || {
                let worker = Worker::new(worker_id, a, global, poller, pw);
                future::block_on(worker.run(async move {
                    closer.listen().await;
                }));
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
        let executor = Arc::new(self);
        future::block_on(join_all(executor.workers.iter().enumerate().map(
            |(id, worker)| {
                let owned_thread = id;
                let assign = worker.assign.clone();
                let maker = maker.clone();
                let executor = executor.clone();
                let scoped = move || {
                    EXECUTOR.with(|ex| ex.set(executor).expect("set global executor must be ok"));
                    maker()
                };

                let schedule = schedule(owned_thread, assign, worker.waker.clone());

                let (runnable, task) =
                    unsafe { async_task::spawn_unchecked(async move { scoped().await }, schedule) };
                runnable.schedule();

                worker
                    .waker
                    .wake()
                    .expect("wake worker to accpet spawned task must be successed");

                task
            },
        )))
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

        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) };

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

        let (runnable, task) =
            unsafe { async_task::spawn_unchecked(async move { f().await }, schedule) };

        runnable.schedule();

        task
    }

    pub fn spawn<F>(&self, future: F) -> Task<F::Output>
    where
        F: 'static + Future + Send,
        F::Output: 'static + Send,
    {
        let (runnable, task) = unsafe {
            async_task::spawn_unchecked(future, move |runnable| {
                CONTEXT.with(|context| {
                    context
                        .get()
                        .expect("context should be initialized")
                        .global
                        .push(runnable);
                })
            })
        };

        self.global.push(runnable);

        task
    }

    pub fn worker_num(&self) -> usize {
        self.workers.len()
    }
}

fn schedule(
    owned_thread: usize,
    assign: Arc<SegQueue<Runnable>>,
    waker: Arc<mio::Waker>,
) -> impl Fn(Runnable) {
    move |runnable| {
        CONTEXT.with(|context| {
            if let Some(id) = context.get().map(|cx| cx.id) {
                if id == owned_thread {
                    CONTEXT.with(|context| {
                        let context = context.get().expect("context should be initialized");
                        context.local.borrow_mut().push_back(runnable);
                    });
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

struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
