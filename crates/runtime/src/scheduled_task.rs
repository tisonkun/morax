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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::future::Future;
use std::future::IntoFuture;
use std::time::Duration;

use crate::CanceledError;
use crate::JoinHandle;
use crate::Runtime;

#[derive(Debug)]
pub struct ScheduledTask<T> {
    name: String,
    task: JoinHandle<T>,
}

impl<T> ScheduledTask<T> {
    pub fn cancel(&self) {
        log::info!("cancelling scheduled task: {}", self.name);
        self.task.cancel();
    }
}

impl<T> IntoFuture for ScheduledTask<T> {
    type Output = Result<T, CanceledError>;
    type IntoFuture = JoinHandle<T>;

    fn into_future(self) -> Self::IntoFuture {
        self.task
    }
}

impl<T> Display for ScheduledTask<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScheduledTask({})", self.name)
    }
}

pub trait TaskFn<T = ()> {
    type Error: Debug + Send + 'static;

    fn call(&mut self) -> impl Future<Output = Result<T, Self::Error>> + Send;
}

pub fn schedule_with_fixed_delay<F>(
    name: impl Into<String>,
    runtime: &Runtime,
    initial_delay: Option<Duration>,
    delay: Duration,
    mut task_fn: F,
) -> ScheduledTask<()>
where
    F: TaskFn<()> + Send + 'static,
{
    let name = name.into();
    let name_clone = name.clone();

    let task = runtime.spawn(async move {
        if let Some(initial_delay) = initial_delay {
            if initial_delay > Duration::ZERO {
                tokio::time::sleep(initial_delay).await;
            }
        }

        loop {
            if let Err(err) = task_fn.call().await {
                log::error!(err:?; "failed to run scheduled task: {name_clone}");
            }
            tokio::time::sleep(delay).await;
        }
    });

    ScheduledTask { name, task }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::AtomicI32;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use super::*;
    use crate::test_runtime;

    struct TickTask {
        n: Arc<AtomicI32>,
    }

    impl TaskFn for TickTask {
        type Error = Infallible;

        async fn call(&mut self) -> Result<(), Infallible> {
            let _ = self.n.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn test_schedule_with_fixed_delay() {
        let n = Arc::new(AtomicI32::new(0));
        let task_fn = TickTask { n: n.clone() };

        let task = schedule_with_fixed_delay(
            "TickTask",
            test_runtime(),
            None,
            Duration::from_millis(100),
            task_fn,
        );

        test_runtime().block_on(async move {
            tokio::time::sleep(Duration::from_millis(550)).await;
            task.cancel();
            task.cancel();
            task.await.unwrap_err(); // cancelled
            assert!(n.load(Ordering::Relaxed) >= 4);
        });
    }
}
