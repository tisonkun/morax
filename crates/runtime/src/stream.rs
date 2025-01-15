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

use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::AsyncFnOnce;
use std::pin::Pin;
use std::ptr;
use std::task::Context;
use std::task::Poll;

use futures::stream::FusedStream;
use futures::stream::Stream;

use crate::buffer_by_unordered::BufferByUnordered;

pub trait StreamExt: Stream {
    fn buffer_by_unordered<F>(self, max_size: usize) -> BufferByUnordered<Self, F>
    where
        Self: Sized,
        Self: Stream<Item = (F, usize)>,
        F: Future,
    {
        BufferByUnordered::new(self, max_size)
    }
}

impl<T: ?Sized> StreamExt for T where T: Stream {}

pub fn make_stream<T>(
    f: impl AsyncFnOnce(&mut Sender<T>) -> () + 'static,
) -> impl Stream<Item = T> {
    let (mut tx, rx) = pair::<T>();
    AsyncStream::new(rx, async move {
        f(&mut tx).await;
    })
}

pub fn make_try_stream<T, E>(
    f: impl AsyncFnOnce(&mut TrySender<T, E>) -> Result<(), E> + 'static,
) -> impl Stream<Item = Result<T, E>> {
    let (tx, rx) = pair::<Result<T, E>>();
    let mut tx = TrySender { sender: tx };
    AsyncStream::new(rx, async move {
        let result = f(&mut tx).await;
        if let Err(err) = result {
            tx.sender.send(Err(err)).await;
        }
    })
}

#[pin_project::pin_project]
#[derive(Debug)]
pub struct AsyncStream<T, U> {
    rx: Receiver<T>,
    done: bool,
    #[pin]
    generator: U,
}

impl<T, U> AsyncStream<T, U> {
    fn new(rx: Receiver<T>, generator: U) -> AsyncStream<T, U> {
        AsyncStream {
            rx,
            done: false,
            generator,
        }
    }
}

impl<T, U> FusedStream for AsyncStream<T, U>
where
    U: Future<Output = ()>,
{
    fn is_terminated(&self) -> bool {
        self.done
    }
}

impl<T, U> Stream for AsyncStream<T, U>
where
    U: Future<Output = ()>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();

        if *me.done {
            return Poll::Ready(None);
        }

        let mut dst = None;
        let res = {
            let _enter = me.rx.enter(&mut dst);
            me.generator.poll(cx)
        };

        *me.done = res.is_ready();

        if dst.is_some() {
            return Poll::Ready(dst.take());
        }

        if *me.done {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.done {
            (0, Some(0))
        } else {
            (0, None)
        }
    }
}

thread_local!(static STORE: Cell<*mut ()> = const { Cell::new(ptr::null_mut()) });

fn pair<T>() -> (Sender<T>, Receiver<T>) {
    let tx = Sender { p: PhantomData };
    let rx = Receiver { p: PhantomData };
    (tx, rx)
}

#[derive(Debug)]
pub struct TrySender<T, E> {
    sender: Sender<Result<T, E>>,
}

impl<T, E> TrySender<T, E> {
    pub fn send(&mut self, value: T) -> impl Future<Output = ()> {
        Send {
            value: Some(Ok::<T, E>(value)),
        }
    }
}

#[derive(Debug)]
pub struct Sender<T> {
    p: PhantomData<fn(T) -> T>,
}

impl<T> Sender<T> {
    pub fn send(&mut self, value: T) -> impl Future<Output = ()> {
        Send { value: Some(value) }
    }
}

struct Send<T> {
    value: Option<T>,
}

impl<T> Unpin for Send<T> {}

impl<T> Future for Send<T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.value.is_none() {
            return Poll::Ready(());
        }

        STORE.with(|cell| {
            let ptr = cell.get() as *mut Option<T>;
            #[allow(unsafe_code)]
            let option_ref = unsafe { ptr.as_mut() }.expect("invalid usage");

            if option_ref.is_none() {
                *option_ref = self.value.take();
            }

            Poll::Pending
        })
    }
}

#[derive(Debug)]
struct Receiver<T> {
    p: PhantomData<T>,
}

struct Enter<'a, T> {
    prev: *mut (),
    #[expect(unused)]
    rx: &'a mut Receiver<T>,
}

impl<T> Receiver<T> {
    pub(crate) fn enter<'a>(&'a mut self, dst: &'a mut Option<T>) -> Enter<'a, T> {
        let prev = STORE.with(|cell| {
            let prev = cell.get();
            cell.set(dst as *mut _ as *mut ());
            prev
        });

        Enter { rx: self, prev }
    }
}

impl<T> Drop for Enter<'_, T> {
    fn drop(&mut self) {
        STORE.set(self.prev);
    }
}
