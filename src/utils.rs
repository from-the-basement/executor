use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ThreadLocal<T> {
    inner: Arc<[T]>,
}

unsafe impl<T> Send for ThreadLocal<T> {}
unsafe impl<T> Sync for ThreadLocal<T> {}

impl<T> ThreadLocal<T> {
    #[inline]
    pub fn new(f: impl Fn() -> T) -> Self {
        let inner: Vec<_> = (0..crate::worker_num()).map(|_| (f)()).collect();
        Self {
            inner: Arc::from(inner),
        }
    }

    #[inline]
    pub fn get(&self) -> &T {
        &self.inner.as_ref()[crate::current_id()]
    }
}
