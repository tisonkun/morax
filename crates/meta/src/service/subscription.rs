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

use std::ops::Bound;

use error_stack::Result;
use error_stack::ResultExt;
use sqlx::postgres::types::PgRange;

use crate::AcknowledgeRequest;
use crate::CreateSubscriptionRequest;
use crate::MetaError;
use crate::PostgresMetaService;
use crate::Subscription;
use crate::Topic;

impl PostgresMetaService {
    pub async fn create_subscription(
        &self,
        request: CreateSubscriptionRequest,
    ) -> Result<Subscription, MetaError> {
        let make_error = || MetaError("failed to create subscription".to_string());
        let pool = self.pool.clone();

        let mut txn = pool.begin().await.change_context_lazy(make_error)?;

        let subscription_name = request.name;
        let topic_name = request.topic;

        let topic: Topic = sqlx::query_as(
            r#"
            SELECT topic_id, topic_name, properties
            FROM topics
            WHERE topic_name = $1
            "#,
        )
        .bind(&topic_name)
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        let subscription_id: i64 = sqlx::query_scalar(
            r#"
                INSERT INTO subscriptions (subscription_id, subscription_name, topic_id)
                VALUES (nextval('object_ids'), $1, $2)
                RETURNING subscription_id
                "#,
        )
        .bind(&subscription_name)
        .bind(topic.topic_id)
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        sqlx::query(
            r#"
            INSERT INTO acknowledgements (subscription_id, topic_id, acks) VALUES ($1, $2, '{}')
            "#,
        )
        .bind(subscription_id)
        .bind(topic.topic_id)
        .execute(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        txn.commit().await.change_context_lazy(make_error)?;
        Ok(Subscription {
            subscription_id,
            subscription_name,
            topic_id: topic.topic_id,
            topic_name,
        })
    }

    pub async fn get_subscription_by_name(
        &self,
        subscription_name: String,
    ) -> Result<Subscription, MetaError> {
        let make_error = || MetaError("failed to get subscription by name".to_string());
        let pool = self.pool.clone();

        sqlx::query_as(
            r#"
            SELECT subscription_id, subscription_name, topics.topic_id, topics.topic_name
            FROM subscriptions JOIN topics ON subscriptions.topic_id = topics.topic_id
            WHERE subscription_name = $1
            "#,
        )
        .bind(&subscription_name)
        .fetch_one(&pool)
        .await
        .change_context_lazy(make_error)
    }

    pub async fn list_acks(&self, subscription_id: i64) -> Result<Vec<(i64, i64)>, MetaError> {
        let make_error = || MetaError("failed to list acks".to_string());
        let pool = self.pool.clone();

        let ranges = sqlx::query_scalar(
            r#"
            SELECT acks FROM acknowledgements
            WHERE subscription_id = $1
            "#,
        )
        .bind(subscription_id)
        .fetch_one(&pool)
        .await
        .change_context_lazy(make_error)?;

        Ok(pg_ranges_to_vec_ranges(ranges))
    }

    pub async fn acknowledge(&self, request: AcknowledgeRequest) -> Result<(), MetaError> {
        let make_error = || MetaError("failed to acknowledge ids".to_string());
        let pool = self.pool.clone();

        let mut txn = pool.begin().await.change_context_lazy(make_error)?;

        let acks: Vec<PgRange<i64>> = sqlx::query_scalar(
            r#"
            SELECT acks FROM acknowledgements
            WHERE subscription_id = $1 FOR UPDATE
            "#,
        )
        .fetch_one(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        let mut acks = pg_ranges_to_vec_ranges(acks);
        for ack_id in request.ack_ids {
            acks.push((ack_id, ack_id + 1))
        }
        acks = merge_ranges(acks);

        sqlx::query(
            r#"
            UPDATE acknowledgements
            SET acks = $1
            WHERE subscription_id = $2
            "#,
        )
        .bind(vec_ranges_to_pg_ranges(acks))
        .bind(request.subscription_id)
        .execute(&mut *txn)
        .await
        .change_context_lazy(make_error)?;

        txn.commit().await.change_context_lazy(make_error)?;
        Ok(())
    }
}

fn pg_ranges_to_vec_ranges(ranges: Vec<PgRange<i64>>) -> Vec<(i64, i64)> {
    ranges
        .into_iter()
        .map(|r| match (r.start, r.end) {
            (Bound::Included(start), Bound::Excluded(end)) => (start, end),
            _ => unreachable!("acks are always left-closed right-open"),
        })
        .collect()
}

fn vec_ranges_to_pg_ranges(ranges: Vec<(i64, i64)>) -> Vec<PgRange<i64>> {
    ranges
        .into_iter()
        .map(|(start, end)| PgRange::from([Bound::Included(start), Bound::Excluded(end)]))
        .collect()
}

// Merge a list of left-closed right-open ranges into a list of left-closed right-open ranges.
fn merge_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    if ranges.len() <= 1 {
        return ranges;
    }

    ranges.sort();
    let mut merged_ranges = vec![];
    let mut current_range = ranges[0];
    for range in ranges.iter().skip(1) {
        if range.0 <= current_range.1 {
            current_range.1 = current_range.1.max(range.1);
        } else {
            merged_ranges.push(current_range);
            current_range = *range;
        }
    }
    merged_ranges.push(current_range);
    merged_ranges
}
