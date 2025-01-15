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

use sqlx::Executor;
use sqlx::PgPool;

pub async fn bootstrap(pool: PgPool) -> error_stack::Result<(), sqlx::Error> {
    let mut txn = pool.begin().await?;

    // create the meta version table
    txn.execute("CREATE TABLE IF NOT EXISTS meta_version(version INT NOT NULL PRIMARY KEY);")
        .await?;
    txn.execute("INSERT INTO meta_version (version) VALUES (1) ON CONFLICT DO NOTHING;")
        .await?;

    // create a sequence for producer ids
    txn.execute("CREATE SEQUENCE IF NOT EXISTS producer_ids CYCLE")
        .await?;

    // topics
    txn.execute(
        r#"
CREATE TABLE IF NOT EXISTS topics (
    id UUID NOT NULL,
    name TEXT NOT NULL,
    properties JSONB NOT NULL,
    UNIQUE (id), UNIQUE (name)
);
"#,
    )
    .await?;

    txn.execute(
        r#"
CREATE TABLE IF NOT EXISTS topic_offsets (
    topic_id UUID NOT NULL,
    last_offset BIGINT NOT NULL,
    UNIQUE (topic_id)
);
"#,
    )
    .await?;

    // topic splits:
    // * start_offset is inclusive
    // * end_offset is exclusive
    txn.execute(
        r#"
CREATE TABLE IF NOT EXISTS topic_splits (
    topic_id UUID NOT NULL,
    topic_name TEXT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    split_id TEXT NOT NULL
);
"#,
    )
    .await?;

    txn.commit().await?;
    Ok(())
}
