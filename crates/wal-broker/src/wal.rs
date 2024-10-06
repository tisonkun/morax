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

use error_stack::Result;
use error_stack::ResultExt;
use morax_meta::CommitRecordBatchesRequest;
use morax_meta::CreateTopicRequest;
use morax_meta::FetchRecordBatchesRequest;
use morax_meta::PostgresMetaService;
use morax_protos::rpc::AppendLogRequest;
use morax_protos::rpc::AppendLogResponse;
use morax_protos::rpc::CreateLogRequest;
use morax_protos::rpc::CreateLogResponse;
use morax_protos::rpc::ReadLogRequest;
use morax_protos::rpc::ReadLogResponse;
use morax_storage::TopicStorage;

use crate::BrokerError;

#[derive(Debug, Clone)]
pub struct WALBroker {
    meta: Arc<PostgresMetaService>,
}

impl WALBroker {
    pub fn new(meta: Arc<PostgresMetaService>) -> Self {
        WALBroker { meta }
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

        let mut data = vec![];
        for split in splits {
            debug_assert_eq!(&split.topic_name, &topic.name);
            debug_assert_eq!(split.partition_id, 0);
            let mut part = topic_storage
                .read_at(&split.topic_name, split.partition_id, &split.split_id)
                .await
                .change_context_lazy(make_error)?;
            data.append(&mut part);
        }
        Ok(ReadLogResponse { data })
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

        let split_id = topic_storage
            .write_to(&topic.name, 0, request.data)
            .await
            .change_context_lazy(make_error)?;

        let (start_offset, end_offset) = self
            .meta
            .commit_record_batches(CommitRecordBatchesRequest {
                topic_name: name.clone(),
                partition_id: 0,
                record_len: request.entry_cnt,
                split_id,
            })
            .await
            .change_context_lazy(make_error)?;

        Ok(AppendLogResponse {
            start_offset,
            end_offset,
        })
    }
}
