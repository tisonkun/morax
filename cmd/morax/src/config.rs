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

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;

use morax_protos::config::KafkaBrokerConfig;
use morax_protos::config::LogConfig;
use morax_protos::config::MetaServiceConfig;
use morax_protos::config::ServerConfig;
use morax_protos::config::StderrAppenderConfig;
use morax_protos::config::TelemetryConfig;
use morax_protos::config::WALBrokerConfig;
use morax_protos::property::StorageProps;
use morax_runtime::RuntimeOptions;
use opendal::services::S3Config;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub telemetry: TelemetryConfig,
    pub runtime: RuntimeOptions,
}

const fn bind_endpoint(port: u16) -> SocketAddr {
    let ipaddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    SocketAddr::new(ipaddr, port)
}

const fn localhost_endpoint(port: u16) -> SocketAddr {
    let ipaddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
    SocketAddr::new(ipaddr, port)
}

fn postgres_url(user: &str, password: &str, endpoint: &SocketAddr, db: &str) -> String {
    format!("postgres://{}:{}@{}/{}", user, password, endpoint, db)
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig {
                kafka_broker: KafkaBrokerConfig {
                    addr: bind_endpoint(9092),
                    fallback_storage: StorageProps::S3({
                        let mut config = S3Config::default();
                        config.bucket = "test-bucket".to_string();
                        config.region = Some("us-east-1".to_string());
                        config.endpoint = Some("http://127.0.0.1:9000".to_string());
                        config.access_key_id = Some("minioadmin".to_string());
                        config.secret_access_key = Some("minioadmin".to_string());
                        config
                    }),
                },
                wal_broker: WALBrokerConfig {
                    addr: bind_endpoint(8848),
                },
                meta: MetaServiceConfig {
                    service_url: postgres_url(
                        "morax",
                        "my_secret_password",
                        &localhost_endpoint(5432),
                        "morax_meta",
                    ),
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
