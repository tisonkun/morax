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

use error_stack::Result;
use error_stack::ResultExt;
use sqlx::types::Json;

use crate::CreateTopicRequest;
use crate::MetaError;
use crate::PostgresMetaService;
use crate::Topic;

impl PostgresMetaService {
    pub async fn create_topic(&self, request: CreateTopicRequest) -> Result<Topic, MetaError> {
        let make_error = || MetaError("failed to create topic".to_string());
        let pool = self.pool.clone();

        let mut txn = pool.begin().await.change_context_lazy(make_error)?;

        let topic_name = request.name;
        let properties = request.properties;

        let topic: Topic = sqlx::query_as(
            r#"
                INSERT INTO topics (topic_id, topic_name, properties)
                VALUES (nextval('object_ids'), $1, $2)
                RETURNING topic_id, topic_name, properties
                "#,
        )
        .bind(topic_name)
        .bind(Json(properties))
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        sqlx::query("INSERT INTO topic_offsets (topic_id, last_offset) VALUES ($1, 0)")
            .bind(topic.topic_id)
            .execute(&mut *txn)
            .await
            .change_context_lazy(make_error)?;

        txn.commit().await.change_context_lazy(make_error)?;
        Ok(topic)
    }

    pub async fn get_topic_by_name(&self, topic_name: String) -> Result<Topic, MetaError> {
        let make_error = || MetaError("failed to get topic by name".to_string());
        let pool = self.pool.clone();

        sqlx::query_as("SELECT topic_id, topic_name, properties FROM topics WHERE topic_name = $1")
            .bind(&topic_name)
            .fetch_one(&pool)
            .await
            .change_context_lazy(make_error)
    }
}
