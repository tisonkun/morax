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

use morax_protos::config::LogConfig;
use morax_protos::config::MetaServiceConfig;
use morax_protos::config::ServerConfig;
use morax_protos::config::StderrAppenderConfig;
use morax_protos::config::TelemetryConfig;
use morax_protos::config::WALBrokerConfig;
use morax_runtime::RuntimeOptions;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub telemetry: TelemetryConfig,
    pub runtime: RuntimeOptions,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig {
                wal_broker: WALBrokerConfig {
                    listen_addr: "0.0.0.0:8848".to_string(),
                    advertise_addr: None,
                },
                meta: MetaServiceConfig {
                    service_url: "postgres://morax:my_secret_password@127.0.0.1:5432/morax_meta"
                        .to_string(),
                },
            },
            telemetry: TelemetryConfig {
                log: LogConfig {
                    stderr: Some(StderrAppenderConfig {
                        filter: "DEBUG".to_string(),
                    }),
                },
            },
            runtime: RuntimeOptions::default(),
        }
    }
}
