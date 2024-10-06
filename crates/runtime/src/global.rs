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

use std::sync::Arc;
use std::sync::OnceLock;

use latches::task::Latch;
use serde::Deserialize;
use serde::Serialize;

use crate::Builder;
use crate::Runtime;

pub fn make_runtime(runtime_name: &str, thread_name: &str, worker_threads: usize) -> Runtime {
    log::info!("creating runtime with runtime_name: {runtime_name}, thread_name: {thread_name}, work_threads: {worker_threads}.");
    Builder::default()
        .runtime_name(runtime_name)
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .build()
        .expect("failed to create runtime")
}

#[cfg(any(test, feature = "test"))]
pub fn test_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| make_runtime("test_runtime", "test_runtime", 4))
}

#[derive(Debug)]
struct GlobalRuntimes {
    api_runtime: Runtime,
    exec_runtime: Runtime,
    meta_runtime: Runtime,
    data_runtime: Runtime,
    bg_runtime: Runtime,
    shutdown: Arc<Latch>,
}

static GLOBAL_RUNTIMES: OnceLock<GlobalRuntimes> = OnceLock::new();

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeOptions {
    #[serde(default = "default_threads")]
    pub api_runtime_threads: usize,
    #[serde(default = "default_threads")]
    pub exec_runtime_threads: usize,
    #[serde(default = "default_threads")]
    pub meta_runtime_threads: usize,
    #[serde(default = "default_threads")]
    pub data_runtime_threads: usize,
    #[serde(default = "default_threads")]
    pub bg_runtime_threads: usize,
}

const fn default_threads() -> usize {
    1
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            api_runtime_threads: 1,
            exec_runtime_threads: 1,
            meta_runtime_threads: 1,
            data_runtime_threads: 1,
            bg_runtime_threads: 1,
        }
    }
}

pub fn init(opts: &RuntimeOptions) {
    GLOBAL_RUNTIMES.get_or_init(|| do_initialize_runtimes(opts));
}

fn do_initialize_runtimes(opts: &RuntimeOptions) -> GlobalRuntimes {
    log::info!("initializing global runtimes: {opts:?}");

    let api_runtime = make_runtime("api_runtime", "api_thread", opts.api_runtime_threads);
    let exec_runtime = make_runtime("exec_runtime", "exec_thread", opts.exec_runtime_threads);
    let meta_runtime = make_runtime("meta_runtime", "meta_thread", opts.meta_runtime_threads);
    let data_runtime = make_runtime("data_runtime", "data_thread", opts.data_runtime_threads);
    let bg_runtime = make_runtime("bg_runtime", "bg_thread", opts.bg_runtime_threads);
    let shutdown = Arc::new(Latch::new(1));

    GlobalRuntimes {
        api_runtime,
        exec_runtime,
        meta_runtime,
        data_runtime,
        bg_runtime,
        shutdown,
    }
}

fn fetch_runtimes_or_default() -> &'static GlobalRuntimes {
    GLOBAL_RUNTIMES.get_or_init(|| do_initialize_runtimes(&RuntimeOptions::default()))
}

pub fn api_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().api_runtime
}

pub fn exec_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().exec_runtime
}

pub fn meta_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().meta_runtime
}

pub fn data_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().data_runtime
}

pub fn bg_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().bg_runtime
}

pub fn shutdown() {
    let runtimes = fetch_runtimes_or_default();
    runtimes.shutdown.count_down();
}

pub async fn wait_shutdown() {
    fetch_runtimes_or_default().shutdown.wait().await;
}

#[cfg(test)]
mod tests {
    use core::panic;

    use pollster::FutureExt;

    use super::*;

    #[test]
    fn test_spawn_block_on() {
        let handle = api_runtime().spawn(async { 1 + 1 });
        assert_eq!(2, api_runtime().block_on(handle).unwrap());

        let handle = exec_runtime().spawn(async { 2 + 2 });
        assert_eq!(4, exec_runtime().block_on(handle).unwrap());

        let handle = meta_runtime().spawn(async { 3 + 3 });
        assert_eq!(6, meta_runtime().block_on(handle).unwrap());

        let handle = data_runtime().spawn(async { 4 + 4 });
        assert_eq!(8, data_runtime().block_on(handle).unwrap());
    }

    #[test]
    fn test_spawn_from_blocking() {
        let runtimes = [
            api_runtime(),
            exec_runtime(),
            meta_runtime(),
            data_runtime(),
        ];

        for runtime in runtimes {
            let out = runtime.block_on(async move {
                let inner = runtime
                    .spawn_blocking(|| runtime.spawn(async move { "hello" }))
                    .await
                    .unwrap();
                inner.await.unwrap()
            });
            assert_eq!(out, "hello")
        }
    }

    #[test]
    fn test_task_panic() {
        let _fut = exec_runtime().spawn(async { panic!("test panic") });
        wait_shutdown().block_on();
    }
}
