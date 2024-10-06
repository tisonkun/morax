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

use error_stack::bail;
use error_stack::ResultExt;
use morax_protos::config::MetaServiceConfig;
use sqlx::migrate::MigrateDatabase;
use sqlx::postgres::PgPoolOptions;
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::PgPool;
use sqlx::Postgres;

use crate::bootstrap::bootstrap;
use crate::MetaError;

mod kafka_group;
mod pubsub;
mod topic;

type MetaResult<T> = error_stack::Result<T, MetaError>;

async fn connect(url: &str) -> error_stack::Result<PgPool, sqlx::Error> {
    Ok(PgPoolOptions::new().connect(url).await?)
}

async fn resolve_meta_version(url: &str) -> error_stack::Result<i32, sqlx::Error> {
    let mut conn = PgConnection::connect(url).await?;

    let exists = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'meta_version'
        )"#,
    )
    .fetch_one(&mut conn)
    .await?;

    if exists {
        let version = sqlx::query_scalar("SELECT MAX(version) FROM meta_version")
            .fetch_one(&mut conn)
            .await?;
        Ok(version)
    } else {
        Ok(0)
    }
}

#[derive(Debug)]
pub struct PostgresMetaService {
    pool: PgPool,
}

impl PostgresMetaService {
    pub async fn new(config: &MetaServiceConfig) -> MetaResult<Self> {
        let make_error = || MetaError("failed to connect and bootstrap the database".to_string());

        let url = config.service_url.as_str();
        log::info!("connecting to meta service at {url}");

        if !Postgres::database_exists(url)
            .await
            .change_context_lazy(make_error)?
        {
            log::info!("creating meta database at {url}");
            Postgres::create_database(url)
                .await
                .change_context_lazy(make_error)?;
        }

        let meta_version = resolve_meta_version(url)
            .await
            .change_context_lazy(make_error)?;
        log::info!("resolved meta version: {meta_version}");

        match meta_version {
            0 => {
                log::info!("bootstrapping meta database at {url}");
                let pool = connect(url).await.change_context_lazy(make_error)?;
                bootstrap(pool.clone())
                    .await
                    .change_context_lazy(make_error)?;
                Ok(Self { pool })
            }
            1 => {
                log::info!("using existing meta database at {url}");
                let pool = connect(url).await.change_context_lazy(make_error)?;
                Ok(Self { pool })
            }
            version => bail!(MetaError(format!("unsupported meta version: {version}"))),
        }
    }
}
