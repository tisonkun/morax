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
use std::process::ExitCode;

use morax_protos::config::LogConfig;
use morax_protos::config::StderrAppenderConfig;
use morax_protos::config::TelemetryConfig;
use morax_protos::property::TopicProps;
use tests_toolkit::make_test_name;

pub struct Testkit {
    pub client: morax_wal_client::HTTPClient,
    pub topic_props: TopicProps,
}

pub fn harness<T, Fut>(test: impl Send + FnOnce(Testkit) -> Fut) -> ExitCode
where
    T: std::process::Termination,
    Fut: Send + Future<Output = T>,
{
    morax_telemetry::init(&TelemetryConfig {
        log: LogConfig {
            stderr: Some(StderrAppenderConfig {
                filter: "INFO".to_string(),
            }),
        },
    });

    let test_name = make_test_name::<Fut>();
    let Some(state) = tests_toolkit::start_test_server(&test_name) else {
        return ExitCode::SUCCESS;
    };

    morax_runtime::test_runtime().block_on(async move {
        let server_addr = format!("http://{}", state.server_state.wal_broker_advertise_addr());
        let builder = reqwest::ClientBuilder::new();
        let client = morax_wal_client::HTTPClient::new(server_addr, builder).unwrap();

        let exit_code = test(Testkit {
            client,
            topic_props: TopicProps {
                storage: state.env_props.storage,
            },
        })
        .await
        .report();

        state.server_state.shutdown();
        state.server_state.await_shutdown().await;
        exit_code
    })
}
