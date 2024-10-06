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

use std::any::Any;

pub use state::make_test_env_state;
pub use state::start_test_server;
pub use state::TestEnvProps;
pub use state::TestEnvState;

mod container;
mod state;

type DropGuard = Box<dyn Any>;

pub fn make_test_name<TestFn>() -> String {
    let replacer = regex::Regex::new(r"[^a-zA-Z0-9]").unwrap();
    let test_name = std::any::type_name::<TestFn>()
        .rsplit("::")
        .find(|part| *part != "{{closure}}")
        .unwrap();
    replacer.replace_all(test_name, "_").to_string()
}
