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

use logforth::append;
use logforth::filter::env::EnvFilterBuilder;
use logforth::filter::EnvFilter;
use logforth::layout;
use logforth::Dispatch;
use logforth::Logger;
use morax_protos::config::TelemetryConfig;

pub fn init(config: &TelemetryConfig) {
    let mut logger = Logger::new();

    // stderr logger
    if let Some(ref stderr) = config.log.stderr {
        logger = logger.dispatch(
            Dispatch::new()
                .filter(make_rust_log_filter_with_default_env(&stderr.filter))
                .layout(layout::TextLayout::default())
                .append(append::Stderr),
        );
    }

    let _ = logger.apply();
}

fn make_rust_log_filter(filter: &str) -> EnvFilter {
    let builder = EnvFilterBuilder::new()
        .try_parse(filter)
        .unwrap_or_else(|_| panic!("failed to parse filter: {filter}"));
    EnvFilter::new(builder)
}

fn make_rust_log_filter_with_default_env(filter: &str) -> EnvFilter {
    if let Ok(filter) = std::env::var("RUST_LOG") {
        make_rust_log_filter(&filter)
    } else {
        make_rust_log_filter(filter)
    }
}
