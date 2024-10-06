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

use morax_protos::property::TopicProps;
use serde::Deserialize;
use serde::Serialize;
use sqlx::types::Json;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CreateTopicRequest {
    pub name: String,
    pub partitions: i32,
    pub properties: TopicProps,
}

#[derive(Debug, Clone)]
pub struct CommitRecordBatchesRequest {
    pub topic_name: String,
    pub partition_id: i32,
    pub record_len: i32,
    pub split_id: String,
}

#[derive(Debug, Clone)]
pub struct FetchRecordBatchesRequest {
    pub topic_id: Uuid,
    pub topic_name: String,
    pub partition_id: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TopicPartitionSplit {
    pub topic_id: Uuid,
    pub topic_name: String,
    pub partition_id: i32,
    pub start_offset: i64,
    pub end_offset: i64,
    pub split_id: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Topic {
    pub id: Uuid,
    pub name: String,
    pub partitions: i32,
    pub properties: Json<TopicProps>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMeta {
    pub group_id: String,
    pub generation_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_type: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub members: BTreeMap<String, MemberMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberMeta {
    pub group_id: String,
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub protocol_type: String,
    pub protocols: BTreeMap<String, Vec<u8>>,
    pub assignment: Vec<u8>,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
}
