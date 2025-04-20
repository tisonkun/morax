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

use crate::FetchTopicSplitRequest;
use crate::MetaError;
use crate::PostgresMetaService;
use crate::TopicSplit;

impl PostgresMetaService {
    pub async fn fetch_topic_splits(
        &self,
        request: FetchTopicSplitRequest,
    ) -> Result<Vec<TopicSplit>, MetaError> {
        let make_error = || MetaError("failed to fetch topic splits".to_string());
        let pool = self.pool.clone();

        let mut txn = pool.begin().await.change_context_lazy(make_error)?;

        let topic_id = request.topic_id;
        let start_offset = request.start_offset;
        let end_offset = request.end_offset;

        let topic_splits: Vec<TopicSplit> = sqlx::query_as(
            r#"
            SELECT split_id, topic_id, start_offset, end_offset
            FROM topic_splits
            WHERE topic_id = $1 AND start_offset < $3 AND end_offset > $2
            "#,
        )
        .bind(topic_id)
        .bind(start_offset)
        .bind(end_offset)
        .fetch_all(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        txn.commit().await.change_context_lazy(make_error)?;

        Ok(topic_splits)
    }
}
