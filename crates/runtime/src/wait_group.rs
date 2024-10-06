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

use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;

use futures::task::AtomicWaker;

#[derive(Clone)]
pub struct WaitGroup {
    inner: Arc<Inner>,
}

pub struct WaitGroupFuture {
    inner: Weak<Inner>,
}

impl WaitGroupFuture {
    /// Gets the number of active workers.
    pub fn workers(&self) -> usize {
        Weak::strong_count(&self.inner)
    }
}

struct Inner {
    waker: AtomicWaker,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.waker.wake();
    }
}

impl WaitGroup {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                waker: AtomicWaker::new(),
            }),
        }
    }

    /// Gets the number of active workers.
    pub fn workers(&self) -> usize {
        Arc::strong_count(&self.inner) - 1
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoFuture for WaitGroup {
    type Output = ();

    type IntoFuture = WaitGroupFuture;

    fn into_future(self) -> Self::IntoFuture {
        WaitGroupFuture {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl Future for WaitGroupFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.upgrade() {
            Some(inner) => {
                inner.waker.register(cx.waker());
                Poll::Pending
            }
            None => Poll::Ready(()),
        }
    }
}

#[cfg(test)]
mod test {
    use pollster::FutureExt;

    use super::*;
    use crate::test_runtime;

    #[test]
    fn test_wait_group_match() {
        let wg = WaitGroup::new();

        for _ in 0..100 {
            let w = wg.clone();
            let _drop = test_runtime().spawn(async move {
                drop(w);
            });
        }

        wg.into_future().block_on();
    }

    #[test]
    fn test_wait_group_timeout() {
        let wg = WaitGroup::new();
        let _wg_clone = wg.clone();
        test_runtime().block_on(async move {
            tokio::select! {
                _ = wg => panic!("wait group should timeout"),
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
            }
        });
    }
}
