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

use morax_protos::property::TopicProps;
use sqlx::types::Json;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CreateTopicRequest {
    pub name: String,
    pub properties: TopicProps,
}

#[derive(Debug, Clone)]
pub struct CommitRecordBatchesRequest {
    pub topic_name: String,
    pub record_len: i32,
    pub split_id: String,
}

#[derive(Debug, Clone)]
pub struct FetchRecordBatchesRequest {
    pub topic_id: Uuid,
    pub topic_name: String,
    pub offset: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TopicSplit {
    pub topic_id: Uuid,
    pub topic_name: String,
    pub start_offset: i64,
    pub end_offset: i64,
    pub split_id: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Topic {
    pub id: Uuid,
    pub name: String,
    pub properties: Json<TopicProps>,
}
