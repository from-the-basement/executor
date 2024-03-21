use std::{
    cell::RefCell,
    error::Error,
    fmt,
    future::Future,
    marker::PhantomPinned,
    mem,
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use pin_project_lite::pin_project;

#[macro_export]
macro_rules! task_local {
     // empty (base case for the recursion)
    () => {};

    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty; $($rest:tt)*) => {
        $crate::__task_local_inner!($(#[$attr])* $vis $name, $t);
        $crate::task_local!($($rest)*);
    };

    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty) => {
        $crate::__task_local_inner!($(#[$attr])* $vis $name, $t);
    }
}

#[macro_export]
macro_rules! __task_local_inner {
    ($(#[$attr:meta])* $vis:vis $name:ident, $t:ty) => {
        $(#[$attr])*
        $vis static $name: $crate::locals::LocalKey<$t> = {
            std::thread_local! {
                static __KEY: std::cell::RefCell<Option<$t>> = const { std::cell::RefCell::new(None) };
            }

            $crate::locals::LocalKey { inner: __KEY }
        };
    };
}

enum ScopeInnerErr {
    BorrowError,
    AccessError,
}

impl ScopeInnerErr {
    #[track_caller]
    fn panic(&self) -> ! {
        match self {
            Self::BorrowError => {
                panic!("cannot enter a task-local scope while the task-local storage is borrowed")
            }
            Self::AccessError => panic!(
                "cannot enter a task-local scope during or after destruction of the underlying \
                 thread-local"
            ),
        }
    }
}

impl From<std::cell::BorrowMutError> for ScopeInnerErr {
    fn from(_: std::cell::BorrowMutError) -> Self {
        Self::BorrowError
    }
}

impl From<std::thread::AccessError> for ScopeInnerErr {
    fn from(_: std::thread::AccessError) -> Self {
        Self::AccessError
    }
}

/// An error returned by [`LocalKey::try_with`](method@LocalKey::try_with).
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AccessError {
    _private: (),
}

impl fmt::Debug for AccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessError").finish()
    }
}

impl fmt::Display for AccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt("task-local value not set", f)
    }
}

impl Error for AccessError {}

pub struct LocalKey<T: 'static> {
    pub inner: thread::LocalKey<RefCell<Option<T>>>,
}

impl<T: 'static> LocalKey<T> {
    pub fn scope<F>(&'static self, value: T, f: F) -> TaskLocalFuture<T, F>
    where
        F: Future,
    {
        TaskLocalFuture {
            local: self,
            slot: Some(value),
            future: Some(f),
            _pinned: PhantomPinned,
        }
    }

    fn scope_inner<F, R>(&'static self, slot: &mut Option<T>, f: F) -> Result<R, ScopeInnerErr>
    where
        F: FnOnce() -> R,
    {
        struct Guard<'a, T: 'static> {
            local: &'static LocalKey<T>,
            slot: &'a mut Option<T>,
        }

        impl<'a, T: 'static> Drop for Guard<'a, T> {
            fn drop(&mut self) {
                // This should not panic.
                //
                // We know that the RefCell was not borrowed before the call to
                // `scope_inner`, so the only way for this to panic is if the
                // closure has created but not destroyed a RefCell guard.
                // However, we never give user-code access to the guards, so
                // there's no way for user-code to forget to destroy a guard.
                //
                // The call to `with` also should not panic, since the
                // thread-local wasn't destroyed when we first called
                // `scope_inner`, and it shouldn't have gotten destroyed since
                // then.
                self.local.inner.with(|inner| {
                    let mut ref_mut = inner.borrow_mut();
                    mem::swap(self.slot, &mut *ref_mut);
                });
            }
        }

        self.inner.try_with(|inner| {
            inner
                .try_borrow_mut()
                .map(|mut ref_mut| mem::swap(slot, &mut *ref_mut))
        })??;

        let guard = Guard { local: self, slot };

        let res = f();

        drop(guard);

        Ok(res)
    }

    #[track_caller]
    pub fn with<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        match self.try_with(f) {
            Ok(res) => res,
            Err(_) => panic!("cannot access a task-local storage value without setting it first"),
        }
    }

    /// Accesses the current task-local and runs the provided closure.
    ///
    /// If the task-local with the associated key is not present, this
    /// method will return an `AccessError`. For a panicking variant,
    /// see `with`.
    pub fn try_with<F, R>(&'static self, f: F) -> Result<R, AccessError>
    where
        F: FnOnce(&T) -> R,
    {
        // If called after the thread-local storing the task-local is destroyed,
        // then we are outside of a closure where the task-local is set.
        //
        // Therefore, it is correct to return an AccessError if `try_with`
        // returns an error.
        let try_with_res = self.inner.try_with(|v| {
            // This call to `borrow` cannot panic because no user-defined code
            // runs while a `borrow_mut` call is active.
            v.borrow().as_ref().map(f)
        });

        match try_with_res {
            Ok(Some(res)) => Ok(res),
            Ok(None) | Err(_) => Err(AccessError { _private: () }),
        }
    }
}

pin_project! {
    pub struct TaskLocalFuture<T, F>
    where
        T: 'static,
    {
        local: &'static LocalKey<T>,
        slot: Option<T>,
        #[pin]
        future: Option<F>,
        #[pin]
        _pinned: PhantomPinned,
    }
}

impl<T: 'static, F: Future> Future for TaskLocalFuture<T, F> {
    type Output = F::Output;

    #[track_caller]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut future_opt = this.future;

        let res = this
            .local
            .scope_inner(this.slot, || match future_opt.as_mut().as_pin_mut() {
                Some(fut) => {
                    let res = fut.poll(cx);
                    if res.is_ready() {
                        future_opt.set(None);
                    }
                    Some(res)
                }
                None => None,
            });

        match res {
            Ok(Some(res)) => res,
            Ok(None) => panic!("`TaskLocalFuture` polled after completion"),
            Err(err) => err.panic(),
        }
    }
}

impl<T: Copy + 'static> LocalKey<T> {
    /// Returns a copy of the task-local value
    /// if the task-local value implements `Copy`.
    ///
    /// # Panics
    ///
    /// This function will panic if the task local doesn't have a value set.
    #[track_caller]
    pub fn get(&'static self) -> T {
        self.with(|v| *v)
    }
}

#[cfg(test)]
mod tests {
    use crate::{futures::future::yield_now, spawn, Executor};

    task_local! {
        static FOO: u32;
    }

    #[test]
    fn task_local() {
        Executor::builder()
            .worker_num(1)
            .build()
            .unwrap()
            .run(|| async {
                let t0 = spawn(async move {
                    FOO.scope(1, async move {
                        yield_now().await;
                        assert_eq!(FOO.get(), 1);
                    })
                    .await;
                });
                let t1 = spawn(async move {
                    FOO.scope(2, async move {
                        yield_now().await;
                        assert_eq!(FOO.get(), 2);
                    })
                    .await;
                });
                t1.await;
                t0.await;
            });
    }
}
