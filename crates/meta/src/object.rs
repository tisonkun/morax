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

use morax_api::property::TopicProperty;
use sqlx::types::Json;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CreateTopicRequest {
    pub name: String,
    pub properties: TopicProperty,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Topic {
    pub topic_id: i64,
    pub topic_name: String,
    pub properties: Json<TopicProperty>,
}

#[derive(Debug, Clone)]
pub struct CreateSubscriptionRequest {
    pub name: String,
    pub topic: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Subscription {
    pub subscription_id: i64,
    pub subscription_name: String,
    pub topic_id: i64,
    pub topic_name: String,
}

#[derive(Debug, Clone)]
pub struct CommitTopicSplitRequest {
    pub topic_id: i64,
    pub split_id: Uuid,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct AcknowledgeRequest {
    pub subscription_id: i64,
    pub ack_ids: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicSplitRequest {
    pub topic_id: i64,
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TopicSplit {
    pub topic_id: i64,
    pub start_offset: i64,
    pub end_offset: i64,
    pub split_id: Uuid,
}
