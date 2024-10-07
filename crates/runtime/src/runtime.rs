// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::disallowed_types)]

use std::any::type_name;
use std::future::Future;
use std::panic::resume_unwind;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use fastrace::future::FutureExt as _;
use fastrace::Span;
use futures::ready;

static RUNTIME_ID: AtomicUsize = AtomicUsize::new(0);

/// A runtime to run future tasks
#[derive(Debug, Clone)]
pub struct Runtime {
    name: String,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl Runtime {
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Spawn a future and execute it in this thread pool.
    ///
    /// Similar to [`tokio::runtime::Runtime::spawn`].
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = type_name::<F>();
        let handle = self
            .runtime
            .spawn(future.in_span(Span::enter_with_local_parent(name)));
        JoinHandle::new(handle)
    }

    /// Run the provided function on an executor dedicated to blocking
    /// operations.
    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        JoinHandle::new(self.runtime.spawn_blocking(func))
    }

    /// Run a future to complete, this is the runtime entry point
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, thiserror::Error)]
#[error("task was canceled")]
pub struct CanceledError;

#[pin_project::pin_project]
#[derive(Debug)]
pub struct JoinHandle<R> {
    #[pin]
    inner: tokio::task::JoinHandle<R>,
}

impl<R> JoinHandle<R> {
    fn new(inner: tokio::task::JoinHandle<R>) -> Self {
        Self { inner }
    }

    pub fn cancel(&self) {
        self.inner.abort()
    }
}

impl<R> Future for JoinHandle<R> {
    type Output = Result<R, CanceledError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let val = ready!(this.inner.poll(cx));
        match val {
            Ok(val) => std::task::Poll::Ready(Ok(val)),
            Err(err) => {
                if err.is_panic() {
                    resume_unwind(err.into_panic())
                } else {
                    std::task::Poll::Ready(Err(CanceledError))
                }
            }
        }
    }
}

pub struct Builder {
    runtime_name: String,
    thread_name: String,
    builder: tokio::runtime::Builder,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            runtime_name: format!("runtime-{}", RUNTIME_ID.fetch_add(1, Ordering::Relaxed)),
            thread_name: "default-worker".to_string(),
            builder: tokio::runtime::Builder::new_multi_thread(),
        }
    }
}

impl Builder {
    /// Sets the number of worker threads the Runtime will use.
    ///
    /// This can be any number above 0. The default value is the number of cores available to the
    /// system.
    pub fn worker_threads(&mut self, val: usize) -> &mut Self {
        self.builder.worker_threads(val);
        self
    }

    /// Specifies the limit for additional threads spawned by the Runtime.
    ///
    /// These threads are used for blocking operations like tasks spawned through spawn_blocking,
    /// they are not always active and will exit if left idle for too long, You can change this
    /// timeout duration with thread_keep_alive. The default value is 512.
    pub fn max_blocking_threads(&mut self, val: usize) -> &mut Self {
        self.builder.max_blocking_threads(val);
        self
    }

    /// Sets a custom timeout for a thread in the blocking pool.
    ///
    /// By default, the timeout for a thread is set to 10 seconds.
    pub fn thread_keep_alive(&mut self, duration: Duration) -> &mut Self {
        self.builder.thread_keep_alive(duration);
        self
    }

    pub fn runtime_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.runtime_name = val.into();
        self
    }

    /// Sets name of threads spawned by the Runtime thread pool
    pub fn thread_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.thread_name = val.into();
        self
    }

    pub fn build(&mut self) -> std::io::Result<Runtime> {
        let name = self.runtime_name.clone();
        let runtime = self
            .builder
            .enable_all()
            .thread_name(self.thread_name.clone())
            .build()
            .map(Arc::new)?;
        Ok(Runtime { name, runtime })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    fn runtime() -> Arc<Runtime> {
        let runtime = Builder::default()
            .worker_threads(2)
            .thread_name("test_spawn_join")
            .build();
        Arc::new(runtime.unwrap())
    }

    #[test]
    fn test_block_on() {
        let runtime = runtime();

        let out = runtime.block_on(async {
            let (tx, rx) = oneshot::channel();

            let _ = thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                tx.send("ZONE").unwrap();
            });

            rx.await.unwrap()
        });

        assert_eq!(out, "ZONE");
    }

    #[test]
    fn test_spawn_blocking() {
        let runtime = runtime();
        let runtime1 = runtime.clone();
        let out = runtime.block_on(async move {
            let runtime2 = runtime1.clone();
            let inner = runtime1
                .spawn_blocking(move || runtime2.spawn(async move { "hello" }))
                .await
                .unwrap();

            inner.await.unwrap()
        });

        assert_eq!(out, "hello")
    }

    #[test]
    fn test_spawn_join() {
        let runtime = runtime();
        let handle = runtime.spawn(async { 1 + 1 });

        assert_eq!(2, runtime.block_on(handle).unwrap());
    }
}
