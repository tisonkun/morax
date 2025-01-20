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

use std::borrow::Cow;

use morax_protos::config::MetaServiceConfig;
use morax_protos::property::StorageProps;
use opendal::services::S3Config;
use testcontainers::core::ContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::SyncRunner;
use testcontainers::Container;
use testcontainers::Image;
use testcontainers::ImageExt;
use testcontainers::ReuseDirective;
use testcontainers::TestcontainersError;

use crate::DropGuard;
use crate::TestEnvProps;

const USERNAME: &str = "morax";
const PASSWORD: &str = "my_secret_password";

#[derive(Default, Debug, Clone)]
struct Postgres;

impl Image for Postgres {
    fn name(&self) -> &str {
        "postgres"
    }

    fn tag(&self) -> &str {
        "16.3-bullseye"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        )]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        vec![("POSTGRES_USER", USERNAME), ("POSTGRES_PASSWORD", PASSWORD)]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[ContainerPort::Tcp(5432)]
    }
}

fn make_meta_service_config(container: &Container<Postgres>) -> MetaServiceConfig {
    let host = local_ip_address::local_ip().unwrap();
    let port = container.get_host_port_ipv4(5432).unwrap();
    MetaServiceConfig {
        service_url: format!("postgres://{USERNAME}:{PASSWORD}@{host}:{port}"),
    }
}

const ACCESS_KEY_ID: &str = "morax_data_access_key";
const SECRET_ACCESS_KEY: &str = "morax_data_secret_access_key";
const BUCKET: &str = "test-bucket";
const REGION: &str = "us-east-1";

#[derive(Default, Debug, Clone)]
struct MinIO;

impl Image for MinIO {
    fn name(&self) -> &str {
        "minio/minio"
    }

    fn tag(&self) -> &str {
        "RELEASE.2024-07-16T23-46-41Z"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("API:")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        vec![
            ("MINIO_ROOT_USER", ACCESS_KEY_ID),
            ("MINIO_ROOT_PASSWORD", SECRET_ACCESS_KEY),
            ("NO_COLOR", "true"),
        ]
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "-c".to_owned(),
            format!("mkdir -p /data/{BUCKET} && minio server /data"),
        ]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[ContainerPort::Tcp(9000)]
    }
}

fn make_s3_props(container: &Container<MinIO>) -> StorageProps {
    let host = local_ip_address::local_ip().unwrap();
    let port = container.get_host_port_ipv4(9000).unwrap();

    let mut config = S3Config::default();
    config.bucket = BUCKET.to_string();
    config.region = Some(REGION.to_string());
    config.endpoint = Some(format!("http://{host}:{port}"));
    config.access_key_id = Some(ACCESS_KEY_ID.to_string());
    config.secret_access_key = Some(SECRET_ACCESS_KEY.to_string());

    StorageProps::S3(config)
}

fn maybe_docker_error(err: TestcontainersError) -> TestcontainersError {
    if matches!(err, TestcontainersError::Client(_)) {
        eprintln!(
            "Error: Docker is not installed or running. Please install and start Docker, or set \
            `SKIP_INTEGRATION=1` to skip these tests.\n\
            Example: SKIP_INTEGRATION=1 cargo x test"
        );
    }
    err
}

pub(crate) fn make_testcontainers_env_props() -> (TestEnvProps, Vec<DropGuard>) {
    let postgres = Postgres
        .with_reuse(ReuseDirective::Always)
        .start()
        .map_err(maybe_docker_error)
        .unwrap();
    let minio = MinIO
        .with_reuse(ReuseDirective::Always)
        .start()
        .map_err(maybe_docker_error)
        .unwrap();
    let props = TestEnvProps {
        meta: make_meta_service_config(&postgres),
        storage: make_s3_props(&minio),
    };
    let drop_guards: Vec<DropGuard> = vec![Box::new(postgres), Box::new(minio)];

    (props, drop_guards)
}
