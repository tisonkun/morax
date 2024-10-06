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

use std::collections::BTreeMap;

use error_stack::ResultExt;
use sqlx::types::Json;

use crate::service::MetaResult;
use crate::GroupMeta;
use crate::MetaError;
use crate::PostgresMetaService;

impl PostgresMetaService {
    pub async fn upsert_group_meta<Resp>(
        &self,
        group_id: String,
        insert_on_missing: bool,
        f: impl FnOnce(Option<GroupMeta>) -> Result<GroupMeta, Resp> + Send + 'static,
    ) -> MetaResult<Result<GroupMeta, Resp>>
    where
        Resp: Send + 'static,
    {
        let make_error = || MetaError("failed to upsert group meta".to_string());
        let pool = self.pool.clone();

        morax_runtime::meta_runtime()
            .spawn(async move {
                if insert_on_missing {
                    let mut txn = pool.begin().await.change_context_lazy(make_error)?;
                    let group_meta = GroupMeta {
                        group_id: group_id.clone(),
                        generation_id: 0,
                        leader_id: None,
                        protocol: None,
                        protocol_type: None,
                        members: BTreeMap::new(),
                    };
                    sqlx::query(
                        "INSERT INTO kafka_consumer_groups VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    )
                    .bind(&group_id)
                    .bind(Json(group_meta))
                    .execute(&mut *txn)
                    .await
                    .change_context_lazy(make_error)?;
                    txn.commit().await.change_context_lazy(make_error)?;
                }

                let mut txn = pool.begin().await.change_context_lazy(make_error)?;
                let group_meta: Option<Json<GroupMeta>> = sqlx::query_scalar(
                    "SELECT group_meta FROM kafka_consumer_groups WHERE group_id = $1 FOR UPDATE",
                )
                .bind(&group_id)
                .fetch_optional(&mut *txn)
                .await
                .change_context_lazy(make_error)?;

                let result = f(group_meta.map(|group_meta| group_meta.0));
                if let Ok(ref group_meta) = result {
                    sqlx::query(
                        "UPDATE kafka_consumer_groups SET group_meta = $1 WHERE group_id = $2",
                    )
                    .bind(Json(group_meta.clone()))
                    .bind(&group_id)
                    .execute(&mut *txn)
                    .await
                    .change_context_lazy(make_error)?;
                    txn.commit().await.change_context_lazy(make_error)?;
                }
                Ok(result)
            })
            .await
            .change_context_lazy(make_error)?
    }
}
