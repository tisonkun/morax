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

use std::num::NonZeroUsize;
use std::sync::OnceLock;

use morax_protos::config::RuntimeOptions;

use crate::num_cpus;
use crate::Builder;
use crate::Runtime;

pub fn make_runtime(runtime_name: &str, thread_name: &str, worker_threads: usize) -> Runtime {
    log::info!(
        "creating runtime with runtime_name: {runtime_name}, thread_name: {thread_name}, work_threads: {worker_threads}."
    );
    Builder::default()
        .runtime_name(runtime_name)
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .build()
        .expect("failed to create runtime")
}

pub fn telemetry_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| make_runtime("telemetry_runtime", "telemetry_thread", 1))
}

#[cfg(any(test, feature = "test"))]
pub fn test_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| make_runtime("test_runtime", "test_thread", 4))
}

#[derive(Debug)]
struct GlobalRuntimes {
    server_runtime: Runtime,
    exec_runtime: Runtime,
    io_runtime: Runtime,
}

static GLOBAL_RUNTIMES: OnceLock<GlobalRuntimes> = OnceLock::new();

pub fn init(opts: &RuntimeOptions) {
    GLOBAL_RUNTIMES.get_or_init(|| do_initialize_runtimes(opts));
}

fn do_initialize_runtimes(opts: &RuntimeOptions) -> GlobalRuntimes {
    log::info!("initializing global runtimes: {opts:?}");

    set_panic_hook();

    let RuntimeOptions {
        server_runtime_threads,
        exec_runtime_threads,
        io_runtime_threads,
    } = opts;

    let server_runtime = make_runtime(
        "server_runtime",
        "server_thread",
        server_runtime_threads
            .unwrap_or_else(default_server_threads)
            .get(),
    );
    let exec_runtime = make_runtime(
        "exec_runtime",
        "exec_thread",
        exec_runtime_threads
            .unwrap_or_else(default_exec_threads)
            .get(),
    );
    let io_runtime = make_runtime(
        "io_runtime",
        "io_thread",
        io_runtime_threads.unwrap_or_else(default_io_threads).get(),
    );

    GlobalRuntimes {
        server_runtime,
        exec_runtime,
        io_runtime,
    }
}

fn default_server_threads() -> NonZeroUsize {
    NonZeroUsize::new(2).unwrap()
}

fn default_exec_threads() -> NonZeroUsize {
    num_cpus()
}

fn default_io_threads() -> NonZeroUsize {
    num_cpus()
}

fn set_panic_hook() {
    std::panic::set_hook(Box::new(move |info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        log::error!("panic occurred: {info}\nbacktrace:\n{backtrace}");
        better_panic::Settings::auto().create_panic_handler()(info);
        log::info!("shutting down runtimes");
        std::process::exit(1);
    }));
}

fn fetch_runtimes_or_default() -> &'static GlobalRuntimes {
    GLOBAL_RUNTIMES.get_or_init(|| do_initialize_runtimes(&RuntimeOptions::default()))
}

pub fn server_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().server_runtime
}

pub fn exec_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().exec_runtime
}

pub fn io_runtime() -> &'static Runtime {
    &fetch_runtimes_or_default().io_runtime
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_block_on() {
        let handle = server_runtime().spawn(async { 1 + 1 });
        assert_eq!(2, server_runtime().block_on(handle));

        let handle = exec_runtime().spawn(async { 2 + 2 });
        assert_eq!(4, exec_runtime().block_on(handle));

        let handle = io_runtime().spawn(async { 4 + 4 });
        assert_eq!(8, io_runtime().block_on(handle));
    }

    #[test]
    fn test_spawn_from_blocking() {
        let runtimes = [server_runtime(), exec_runtime(), io_runtime()];

        for runtime in runtimes {
            let out = runtime.block_on(async move {
                let inner = runtime
                    .spawn_blocking(|| runtime.spawn(async move { "hello" }))
                    .await;
                inner.await
            });
            assert_eq!(out, "hello")
        }
    }
}
