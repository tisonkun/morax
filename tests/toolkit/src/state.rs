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

use std::net::SocketAddr;

use morax_api::config::BrokerConfig;
use morax_api::config::MetaServiceConfig;
use morax_api::config::ServerConfig;
use morax_api::property::StorageProperty;
use morax_server::ServerState;
use morax_storage::make_op;
use serde::Deserialize;
use serde::Serialize;
use sqlx::migrate::MigrateDatabase;
use url::Url;

use crate::container::make_testcontainers_env_props;
use crate::DropGuard;

#[derive(Debug)]
pub struct TestServerState {
    pub server_state: ServerState,
    pub env_props: TestEnvProps,
    _drop_guards: Vec<DropGuard>,
}

#[derive(Debug)]
pub struct TestEnvState {
    pub env_props: TestEnvProps,
    _drop_guards: Vec<DropGuard>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEnvProps {
    pub meta: MetaServiceConfig,
    pub storage: StorageProperty,
}

pub(crate) fn read_test_env_props() -> Option<TestEnvProps> {
    let file = std::env::var("TEST_ENV_PROPS_FILE").ok()?;
    let path = std::path::Path::new(&file);
    let path = if path.is_absolute() {
        path.canonicalize().unwrap()
    } else {
        env!("CARGO_WORKSPACE_DIR")
            .parse::<std::path::PathBuf>()
            .unwrap()
            .join(path)
            .canonicalize()
            .unwrap()
    };
    let content = std::fs::read_to_string(&path).unwrap();
    Some(toml::from_str(&content).unwrap())
}

pub fn start_test_server(test_name: &str) -> Option<TestServerState> {
    let TestEnvState {
        env_props,
        _drop_guards,
    } = make_test_env_state(test_name)?;
    let host = local_ip_address::local_ip().unwrap();
    let broker = BrokerConfig {
        listen_addr: SocketAddr::new(host, 0).to_string(),
        advertise_addr: None,
    };
    let server_state = morax_runtime::test_runtime()
        .block_on(morax_server::start(ServerConfig {
            broker,
            meta: env_props.meta.clone(),
            default_storage: env_props.storage.clone(),
        }))
        .unwrap();
    Some(TestServerState {
        server_state,
        env_props,
        _drop_guards,
    })
}

pub fn make_test_env_state(test_name: &str) -> Option<TestEnvState> {
    let mut _drop_guards = Vec::<DropGuard>::new();

    let mut props = if option_enabled("SKIP_INTEGRATION") {
        return None;
    } else if let Some(props) = read_test_env_props() {
        props
    } else {
        let (props, drop_guards) = make_testcontainers_env_props();
        _drop_guards.extend(drop_guards);
        props
    };

    props.meta.service_url = {
        let mut url = Url::parse(&props.meta.service_url).unwrap();
        url.set_path(test_name);
        url.to_string()
    };
    _drop_guards.push(Box::new(scopeguard::guard_on_success(
        props.meta.service_url.clone(),
        |url| {
            morax_runtime::test_runtime().block_on(async move {
                sqlx::Postgres::drop_database(&url).await.unwrap();
            });
        },
    )));

    match props.storage {
        StorageProperty::S3(ref mut config) => {
            config.prefix = format!("/{test_name}/");
        }
    }

    let client = make_op(props.storage.clone()).unwrap();
    morax_runtime::test_runtime().block_on(async {
        client.remove_all("/").await.unwrap();
    });
    _drop_guards.push(Box::new(scopeguard::guard_on_success((), move |()| {
        morax_runtime::test_runtime().block_on(async move {
            client.remove_all("/").await.unwrap();
        });
    })));

    // ensure containers get dropped last
    _drop_guards.reverse();
    Some(TestEnvState {
        env_props: props,
        _drop_guards,
    })
}

fn option_enabled(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .filter(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .is_some()
}
