mod blocking;
mod context;
mod deque;
mod executor;
pub mod fs;
pub mod io;
pub mod locals;
pub mod net;
pub mod shard;
pub mod futures {
    pub use futures_lite::*;
    pub mod util {
        pub use futures_util::*;
    }
}

use std::{cell::OnceCell, future::Future, sync::Arc};

pub use async_task::Task;
pub use blocking::unblock;
use context::CONTEXT;

pub use crate::executor::{Executor, ExecutorBuilder};

thread_local! {
    static EXECUTOR: OnceCell<Arc<Executor>> = const { OnceCell::new() };
}

pub fn spawn_local<F>(future: F) -> Task<F::Output>
where
    F: 'static + Future,
    F::Output: 'static,
{
    EXECUTOR.with(|executor| executor.get().unwrap().spawn_local(future))
}

pub fn spawn_to<F, Fut>(to: usize, f: F) -> Task<Fut::Output>
where
    F: 'static + Send + FnOnce() -> Fut,
    Fut: 'static + Future,
    Fut::Output: 'static + Send,
{
    EXECUTOR.with(|executor| {
        executor
            .get()
            .expect("method must be called under executor context")
            .spawn_to(to, f)
    })
}

pub fn spawn<F>(future: F) -> Task<F::Output>
where
    F: 'static + Future + Send,
    F::Output: 'static + Send,
{
    EXECUTOR.with(|executor| {
        executor
            .get()
            .expect("method must be called under executor context")
            .spawn(future)
    })
}

pub fn worker_num() -> usize {
    EXECUTOR.with(|executor| {
        executor
            .get()
            .expect("method must be called under executor context")
            .worker_num()
    })
}

pub fn get_current_worker_id() -> usize {
    try_get_current_worker_id().expect("method must be called under executor context")
}

pub fn try_get_current_worker_id() -> Option<usize> {
    CONTEXT.with(|context| context.get().map(|c| c.id))
}

#[cfg(test)]
mod test {
    use std::{
        cell::RefCell,
        sync::{Arc, Mutex},
    };

    use futures_lite::future::yield_now;

    use crate::{spawn, spawn_local, spawn_to, Executor};

    #[test]
    fn static_task() {
        let hello_world = "hello world";
        Executor::builder()
            .worker_num(1)
            .build()
            .unwrap()
            .run(|| async {
                spawn_local(async move {
                    let _ = hello_world;
                })
                .detach();
            });
    }

    #[test]
    fn spawn_to_another_worker() {
        let assertion = Arc::new(Mutex::new(0));
        Executor::builder().worker_num(2).build().unwrap().run(|| {
            thread_local! {
                static A: RefCell<i32> = RefCell::new(0);
            }
            async move {
                spawn_to(1, || async move {
                    A.with(|a| {
                        *a.borrow_mut() += 1;
                        let mut assertion = assertion.lock().unwrap();
                        *assertion += 1;
                        assert_eq!(*assertion, *a.borrow());
                    });
                    yield_now().await;
                })
                .await;
            }
        });
    }

    #[test]
    fn spawn_with_work_stealing() {
        Executor::builder()
            .worker_num(2)
            .build()
            .unwrap()
            .run(|| async {
                spawn(async {
                    let id0 = std::thread::current().id();
                    yield_now().await;
                    let id1 = std::thread::current().id();
                    yield_now().await;
                    let id2 = std::thread::current().id();
                    yield_now().await;
                    assert!(id0 == id1 && id1 == id2);
                })
                .detach()
            });
    }
}
