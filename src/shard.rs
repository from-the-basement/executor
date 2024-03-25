use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use std::future::Future;

#[derive(Debug)]
pub struct Shard<T> {
    inner: Arc<[Rc<RefCell<T>>]>,
}

impl<T> Clone for Shard<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

unsafe impl<T> Send for Shard<T> {}
unsafe impl<T> Sync for Shard<T> {}

impl<T> Shard<T> {
    pub fn new(f: impl Fn() -> T + Send) -> Self {
        Self {
            inner: Arc::from(
                (0..crate::worker_num())
                    .map(|_| Rc::new(RefCell::from((f)())))
                    .collect::<Vec<_>>(),
            ),
        }
    }

    pub async fn with<F, G>(
        &self,
        to: usize,
        f: impl FnOnce(Rc<RefCell<T>>) -> F + Send + 'static,
    ) -> G
    where
        F: Future<Output = G>,
        G: Send + 'static,
        T: 'static,
    {
        if to == crate::current_id() {
            (f)(self.inner[to].clone()).await
        } else {
            let this = self.clone();
            crate::spawn_to(to, move || {
                let this = this;
                async move { (f)(this.inner[to].clone()).await }
            })
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Executor;

    #[test]
    fn test_thread_local() {
        Executor::builder()
            .worker_num(2)
            .build()
            .unwrap()
            .run(|| async {
                let local = super::Shard::new(|| 42);
                let a = local
                    .with(0, |a| async move {
                        let mut a = a.borrow_mut();
                        *a += 1;
                        *a
                    })
                    .await;
                let b = local.with(1, |b| async move { *b.borrow() }).await;
                let a_ = local.with(0, |a| async move { *a.borrow() }).await;
                assert_eq!(a, 43);
                assert_eq!(b, 42);
                assert_eq!(a_, 43);
            });
    }
}
