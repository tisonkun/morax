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

use crate::CommitTopicSplitRequest;
use crate::MetaError;
use crate::PostgresMetaService;

impl PostgresMetaService {
    pub async fn commit_topic_splits(
        &self,
        request: CommitTopicSplitRequest,
    ) -> Result<(i64, i64), MetaError> {
        let make_error = || MetaError("failed to commit topic splits".to_string());
        let pool = self.pool.clone();

        let mut txn = pool.begin().await.change_context_lazy(make_error)?;

        let topic_id = request.topic_id;
        let start_offset: i64 = sqlx::query_scalar(
            "SELECT last_offset FROM topic_offsets WHERE topic_id = $1 FOR UPDATE",
        )
        .bind(topic_id)
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        let last_offset = start_offset + request.count;
        let end_offset = sqlx::query_scalar(
            "UPDATE topic_offsets SET last_offset = $1 WHERE topic_id = $2 RETURNING last_offset",
        )
        .bind(last_offset)
        .bind(topic_id)
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;
        assert_eq!(last_offset, end_offset, "last offset mismatch");

        sqlx::query("INSERT INTO topic_splits (topic_id, start_offset, end_offset, split_id) VALUES ($1, $2, $3, $4)")
            .bind(topic_id)
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
