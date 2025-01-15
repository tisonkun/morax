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

use crate::service::MetaResult;
use crate::CommitRecordBatchesRequest;
use crate::FetchRecordBatchesRequest;
use crate::MetaError;
use crate::PostgresMetaService;
use crate::TopicSplit;

impl PostgresMetaService {
    pub async fn new_producer_id(&self) -> MetaResult<i64> {
        let make_error = || MetaError("failed to generate new producer id".to_string());
        let pool = self.pool.clone();

        sqlx::query_scalar("SELECT nextval('producer_ids')")
            .fetch_one(&pool)
            .await
            .change_context_lazy(make_error)
    }

    pub async fn fetch_record_batches(
        &self,
        request: FetchRecordBatchesRequest,
    ) -> MetaResult<Vec<TopicSplit>> {
        let make_error = || MetaError("failed to fetch record batches".to_string());
        let pool = self.pool.clone();

        let topic_id = if request.topic_id != uuid::Uuid::default() {
            request.topic_id
        } else {
            sqlx::query_scalar("SELECT id FROM topics WHERE name = $1")
                .bind(request.topic_name)
                .fetch_one(&pool)
                .await
                .change_context_lazy(make_error)?
        };

        sqlx::query_as("SELECT topic_id, topic_name, start_offset, end_offset, split_id FROM topic_splits WHERE topic_id = $1 AND end_offset > $2 ORDER BY end_offset ASC")
            .bind(topic_id)
            .bind(request.offset)
            .fetch_all(&pool)
            .await
            .change_context_lazy(make_error)
    }

    pub async fn commit_record_batches(
        &self,
        request: CommitRecordBatchesRequest,
    ) -> MetaResult<(i64, i64)> {
        let make_error = || MetaError("failed to commit record batches".to_string());
        let pool = self.pool.clone();

        let mut txn = pool.begin().await.change_context_lazy(make_error)?;

        let (topic_id, topic_name): (uuid::Uuid, String) =
            sqlx::query_as("SELECT id, name FROM topics WHERE name = $1")
                .bind(request.topic_name)
                .fetch_one(&mut *txn)
                .await
                .change_context_lazy(make_error)?;

        let start_offset: i64 = sqlx::query_scalar(
            "SELECT last_offset FROM topic_offsets WHERE topic_id = $1 FOR UPDATE",
        )
        .bind(topic_id)
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;
        let last_offset = start_offset + request.record_len as i64;
        let end_offset = sqlx::query_scalar(
            "UPDATE topic_offsets SET last_offset = $1 WHERE topic_id = $2 RETURNING last_offset",
        )
        .bind(last_offset)
        .bind(topic_id)
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;
        debug_assert_eq!(last_offset, end_offset, "last offset mismatch");

        sqlx::query("INSERT INTO topic_splits (topic_id, topic_name, start_offset, end_offset, split_id) VALUES ($1, $2, $3, $4, $5)")
            .bind(topic_id)
            .bind(topic_name)
            .bind(start_offset)
            .bind(end_offset)
            .bind(request.split_id)
            .execute(&mut *txn)
            .await
            .change_context_lazy(make_error)?;

        txn.commit().await.change_context_lazy(make_error)?;
        Ok((start_offset, end_offset))
    }
}
