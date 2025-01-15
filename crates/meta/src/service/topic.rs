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

use error_stack::ResultExt;
use sqlx::types::Json;

use crate::service::MetaResult;
use crate::CreateTopicRequest;
use crate::MetaError;
use crate::PostgresMetaService;
use crate::Topic;

impl PostgresMetaService {
    pub async fn create_topic(&self, request: CreateTopicRequest) -> MetaResult<Topic> {
        let make_error = || MetaError("failed to create topic".to_string());
        let pool = self.pool.clone();

        let mut txn = pool.begin().await.change_context_lazy(make_error)?;

        let topic_id = uuid::Uuid::new_v4();
        let topic_name = request.name;
        let properties = request.properties;

        let topic = sqlx::query_as("INSERT INTO topics (id, name, properties) VALUES ($1, $2, $3) RETURNING id, name, properties")
            .bind(topic_id)
            .bind(topic_name)
            .bind(Json(properties))
            .fetch_one(&mut *txn)
            .await
            .change_context_lazy(make_error)?;

        sqlx::query("INSERT INTO topic_offsets (topic_id, last_offset) VALUES ($1, 0)")
            .bind(topic_id)
            .execute(&mut *txn)
            .await
            .change_context_lazy(make_error)?;

        txn.commit().await.change_context_lazy(make_error)?;
        Ok(topic)
    }

    pub async fn get_topics_by_id(&self, topic_id: uuid::Uuid) -> MetaResult<Topic> {
        let make_error = || MetaError("failed to get all topics".to_string());
        let pool = self.pool.clone();

        sqlx::query_as("SELECT id, name, properties FROM topics WHERE id = $1")
            .bind(topic_id)
            .fetch_one(&pool)
            .await
            .change_context_lazy(make_error)
    }

    pub async fn get_topics_by_name(&self, topic_name: String) -> MetaResult<Topic> {
        let make_error = || MetaError("failed to get all topics".to_string());
        let pool = self.pool.clone();

        sqlx::query_as("SELECT id, name, properties FROM topics WHERE name = $1")
            .bind(&topic_name)
            .fetch_one(&pool)
            .await
            .change_context_lazy(make_error)
    }

    pub async fn get_all_topics(&self) -> MetaResult<Vec<Topic>> {
        let make_error = || MetaError("failed to get all topics".to_string());
        let pool = self.pool.clone();

        sqlx::query_as("SELECT id, name, properties FROM topics")
            .fetch_all(&pool)
            .await
            .change_context_lazy(make_error)
    }
}
