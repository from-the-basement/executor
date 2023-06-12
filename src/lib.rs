mod blocking;
mod deque;
mod executor;
pub mod io;
pub mod net;
pub mod utils;
mod worker;
pub mod futures {
    pub use futures_lite::*;
    pub mod util {
        pub use futures_util::*;
    }
}

use std::{future::Future, sync::Arc};

pub use async_task::Task;
pub use blocking::unblock;
use once_cell::sync::OnceCell;
use worker::CONTEXT;

pub use crate::executor::{Executor, ExecutorBuilder};

thread_local! {
    static EXECUTOR: OnceCell<Arc<Executor>> = OnceCell::new();
}

#[inline]
pub fn spawn_local<F>(future: F) -> Task<F::Output>
where
    F: 'static + Future,
    F::Output: 'static,
{
    EXECUTOR.with(|executor| executor.get().unwrap().spawn_local(future))
}

#[inline]
pub fn spawn_to<F, Fut>(to: usize, f: F) -> Task<Fut::Output>
where
    F: 'static + Send + FnOnce() -> Fut,
    Fut: 'static + Future,
    Fut::Output: 'static + Send,
{
    EXECUTOR.with(|executor| executor.get().unwrap().spawn_to(to, f))
}

#[inline]
pub fn spawn<F>(future: F) -> Task<F::Output>
where
    F: 'static + Future + Send,
    F::Output: 'static + Send,
{
    EXECUTOR.with(|executor| executor.get().unwrap().spawn(future))
}

#[inline]
pub fn worker_num() -> usize {
    EXECUTOR.with(|executor| executor.get().unwrap().worker_num())
}

#[inline]
pub fn current_id() -> usize {
    CONTEXT.with(|context| context.get().unwrap().id)
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
                    println!("{:?}", std::thread::current().id());
                    yield_now().await;
                    println!("{:?}", std::thread::current().id());
                    yield_now().await;
                    println!("{:?}", std::thread::current().id());
                    yield_now().await;
                })
                .detach()
            });
    }
}
