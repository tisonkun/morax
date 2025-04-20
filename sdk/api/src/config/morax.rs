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

use serde::Deserialize;
use serde::Serialize;

use crate::config::BrokerConfig;
use crate::config::LogsConfig;
use crate::config::MetaServiceConfig;
use crate::config::RuntimeOptions;
use crate::config::ServerConfig;
use crate::config::StderrAppenderConfig;
use crate::config::TelemetryConfig;
use crate::property::S3StorageProperty;
use crate::property::StorageProperty;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub telemetry: TelemetryConfig,
    pub runtime: RuntimeOptions,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig {
                broker: BrokerConfig {
                    listen_addr: "0.0.0.0:8848".to_string(),
                    advertise_addr: None,
                },
                meta: MetaServiceConfig {
                    service_url: "postgres://morax:my_secret_password@127.0.0.1:5432/morax_meta"
                        .to_string(),
                },
                default_storage: StorageProperty::S3(S3StorageProperty {
                    bucket: "test-bucket".to_string(),
                    region: "us-east-1".to_string(),
                    prefix: "/".to_string(),
                    endpoint: "http://127.0.0.1:9000".to_string(),
                    access_key_id: "minioadmin".to_string(),
                    secret_access_key: "minioadmin".to_string(),
                    virtual_host_style: false,
                }),
            },
            telemetry: TelemetryConfig {
                logs: LogsConfig {
                    stderr: Some(StderrAppenderConfig {
                        filter: "DEBUG".to_string(),
                    }),
                },
            },
            runtime: RuntimeOptions::default(),
        }
    }
}
