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

use std::sync::Arc;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use error_stack::Result;
use error_stack::ResultExt;
use morax_meta::CommitRecordBatchesRequest;
use morax_meta::CreateTopicRequest;
use morax_meta::FetchRecordBatchesRequest;
use morax_meta::PostgresMetaService;
use morax_protos::request::AppendLogRequest;
use morax_protos::request::AppendLogResponse;
use morax_protos::request::CreateLogRequest;
use morax_protos::request::CreateLogResponse;
use morax_protos::request::Entry;
use morax_protos::request::ReadLogRequest;
use morax_protos::request::ReadLogResponse;
use morax_storage::TopicStorage;
use serde::Deserialize;
use serde::Serialize;

use crate::BrokerError;

// TODO(tisonkun): figure out whether flexbuffers is the proper format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EntryData {
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Broker {
    meta: Arc<PostgresMetaService>,
}

impl Broker {
    pub fn new(meta: Arc<PostgresMetaService>) -> Self {
        Broker { meta }
    }

    pub async fn create(
        &self,
        request: CreateLogRequest,
    ) -> Result<CreateLogResponse, BrokerError> {
        let name = request.name;
        let make_error = || BrokerError(format!("failed to create log with name {name}"));

        let topic = self
            .meta
            .create_topic(CreateTopicRequest {
                name: name.clone(),
                partitions: 1,
                properties: request.properties,
            })
            .await
            .change_context_lazy(make_error)?;

        Ok(CreateLogResponse { name: topic.name })
    }

    pub async fn read_at(&self, request: ReadLogRequest) -> Result<ReadLogResponse, BrokerError> {
        let name = request.name;
        let make_error = || BrokerError(format!("failed to read log from {name}"));

        let topic = self
            .meta
            .get_topics_by_name(name.clone())
            .await
            .change_context_lazy(make_error)?;

        let topic_storage = TopicStorage::new(topic.properties.0.storage);

        let splits = self
            .meta
            .fetch_record_batches(FetchRecordBatchesRequest {
                topic_id: Default::default(),
                topic_name: name.clone(),
                partition_id: 0,
                offset: request.offset,
            })
            .await
            .change_context_lazy(make_error)?;

        let mut entries = vec![];
        for split in splits {
            debug_assert_eq!(&split.topic_name, &topic.name);
            debug_assert_eq!(split.partition_id, 0);
            let data = topic_storage
                .read_at(&split.topic_name, split.partition_id, &split.split_id)
                .await
                .change_context_lazy(make_error)?;
            let deserializer =
                flexbuffers::Reader::get_root(data.as_slice()).change_context_lazy(|| {
                    BrokerError("failed to deserialize entry data".to_string())
                })?;
            let entry_data =
                Vec::<EntryData>::deserialize(deserializer).change_context_lazy(|| {
                    BrokerError("failed to deserialize entry data".to_string())
                })?;
            for (i, entry_data) in entry_data.into_iter().enumerate() {
                if split.start_offset + i as i64 >= request.offset {
                    entries.push(Entry {
                        index: Some(split.start_offset + i as i64),
                        data: BASE64_STANDARD.encode(&entry_data.data),
                    });
                }
            }
        }
        Ok(ReadLogResponse { entries })
    }

    pub async fn append(
        &self,
        request: AppendLogRequest,
    ) -> Result<AppendLogResponse, BrokerError> {
        let name = request.name;
        let make_error = || BrokerError(format!("failed to append log to {name}"));

        let topic = self
            .meta
            .get_topics_by_name(name.clone())
            .await
            .change_context_lazy(make_error)?;

        let topic_storage = TopicStorage::new(topic.properties.0.storage);

        let entry_cnt = request.entries.len();
        let entry_data = {
            let mut entry_data = vec![];
            for entry in request.entries.into_iter() {
                entry_data.push(EntryData {
                    data: BASE64_STANDARD
                        .decode(entry.data.as_bytes())
                        .change_context_lazy(|| {
                            BrokerError(format!("failed to decode base64: {:?}", entry.data))
                        })?,
                });
            }
            let mut serializer = flexbuffers::FlexbufferSerializer::new();
            entry_data
                .serialize(&mut serializer)
                .change_context_lazy(|| {
                    BrokerError("failed to serialize entry data".to_string())
                })?;
            serializer.take_buffer()
        };
        let split_id = topic_storage
            .write_to(&topic.name, 0, entry_data)
            .await
            .change_context_lazy(make_error)?;

        let (start_offset, end_offset) = self
            .meta
            .commit_record_batches(CommitRecordBatchesRequest {
                topic_name: name.clone(),
                partition_id: 0,
                record_len: entry_cnt as i32,
                split_id,
            })
            .await
            .change_context_lazy(make_error)?;

        Ok(AppendLogResponse {
            offsets: start_offset..end_offset,
        })
    }
}
