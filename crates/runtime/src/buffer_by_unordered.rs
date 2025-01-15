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

use std::fmt;
use std::pin::Pin;

use futures::future::Future;
use futures::stream::Fuse;
use futures::stream::FuturesUnordered;
use futures::stream::Stream;
use futures::stream::StreamExt;
use futures::task::Context;
use futures::task::Poll;

/// Stream for the [`buffer_by_unordered`] method.
///
/// [`buffer_by_unordered`]: crate::StreamExt::buffer_by_unordered
#[must_use = "streams do nothing unless polled"]
#[pin_project::pin_project]
pub struct BufferByUnordered<St, F>
where
    St: Stream<Item = (F, usize)>,
    F: Future,
{
    #[pin]
    stream: Fuse<St>,
    in_progress_queue: FuturesUnordered<SizedFuture<F>>,
    pending_queue: Vec<SizedFuture<F>>,
    max_size: usize,
    total_size: usize,
}

impl<St, F> fmt::Debug for BufferByUnordered<St, F>
where
    St: Stream<Item = (F, usize)> + fmt::Debug,
    F: Future,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferByUnordered")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("max_size", &self.max_size)
            .field("total_size", &self.total_size)
            .finish()
    }
}

impl<St, F> BufferByUnordered<St, F>
where
    St: Stream<Item = (F, usize)>,
    F: Future,
{
    pub(crate) fn new(stream: St, max_size: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesUnordered::new(),
            pending_queue: Vec::new(),
            max_size,
            total_size: 0,
        }
    }
}

impl<St, F> Stream for BufferByUnordered<St, F>
where
    St: Stream<Item = (F, usize)>,
    F: Future,
{
    type Item = F::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Collect inputs from the stream into the pending_queue.
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some((fut, size))) => {
                    this.pending_queue.push(SizedFuture { future: fut, size });
                }
                Poll::Ready(None) => break,
                Poll::Pending => break,
            }
        }

        // Try to spawn off as many futures as possible while respecting size limits.
        // Always allow at least one future, even if it exceeds max_size to prevent deadlock when a
        // single task is larger than max_size.
        while let Some(fut) = this.pending_queue.pop() {
            if *this.total_size == 0 || *this.total_size + fut.size <= *this.max_size {
                *this.total_size += fut.size;
                this.in_progress_queue.push(fut);
            } else {
                this.pending_queue.push(fut);
                break;
            }
        }

        // Attempt to poll the futures in the in_progress_queue.
        match this.in_progress_queue.poll_next_unpin(cx) {
            Poll::Ready(Some((output, size))) => {
                *this.total_size -= size;
                Poll::Ready(Some(output))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

#[pin_project::pin_project]
struct SizedFuture<F> {
    #[pin]
    future: F,
    size: usize,
}

impl<F: Future> Future for SizedFuture<F> {
    type Output = (F::Output, usize);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.poll(cx) {
            Poll::Ready(output) => Poll::Ready((output, *this.size)),
            Poll::Pending => Poll::Pending,
        }
    }
}
