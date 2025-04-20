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

mod global;
pub use global::*;

mod runtime;
pub use runtime::*;

/// Returns the number of logical CPUs on the current machine.
// This method fills the gap that `std::thread::available_parallelism()`
// may return `Err` on some platforms, in which case we default to `1`.
#[track_caller]
pub fn num_cpus() -> std::num::NonZeroUsize {
    match std::thread::available_parallelism() {
        Ok(parallelism) => parallelism,
        Err(err) => {
            log::warn!(err: err; "failed to fetch the available parallelism; fallback to 1");
            std::num::NonZeroUsize::new(1).unwrap()
        }
    }
}
