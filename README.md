# Executor

A Rust async runtime that bases on async-task and mio. It supports three different task scheduling strategies:

```rust
pub fn spawn_local<F>(future: F) -> Task<F::Output>
where
    F: 'static + Future,
    F::Output: 'static;

pub fn spawn_to<F, Fut>(to: usize, f: F) -> Task<Fut::Output>
where
    F: 'static + Send + FnOnce() -> Fut,
    Fut: 'static + Future,
    Fut::Output: 'static + Send;

pub fn spawn<F>(future: F) -> Task<F::Output>
where
    F: 'static + Future + Send,
    F::Output: 'static + Send;
```

It is helpful to build thread-per-core & work-stealing fusion app
