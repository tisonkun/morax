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

use kafka_api::records::RecordBatches;
use kafka_api::schemata::error::ErrorCode;
use kafka_api::schemata::init_producer_id_request::InitProducerIdRequest;
use kafka_api::schemata::init_producer_id_response::InitProducerIdResponse;
use kafka_api::schemata::produce_request::ProduceRequest;
use kafka_api::schemata::produce_response::PartitionProduceResponse;
use kafka_api::schemata::produce_response::ProduceResponse;
use kafka_api::schemata::produce_response::TopicProduceResponse;
use morax_meta::Topic;
use morax_protos::property::TopicFormat;
use morax_storage::TopicStorage;

use crate::broker::Broker;

impl Broker {
    pub(super) async fn receive_init_producer_id(
        &self,
        _request: InitProducerIdRequest,
    ) -> InitProducerIdResponse {
        match self.meta.new_producer_id().await {
            Ok(id) => InitProducerIdResponse {
                producer_id: id,
                producer_epoch: 0,
                ..Default::default()
            },
            Err(err) => {
                log::error!("failed to create producer id: {err:?}");
                let error = ErrorCode::UNKNOWN_SERVER_ERROR;
                InitProducerIdResponse {
                    error_code: error.code(),
                    ..Default::default()
                }
            }
        }
    }

    pub(super) async fn receive_produce(&self, request: ProduceRequest) -> ProduceResponse {
        let mut responses = vec![];
        for topic in request.topic_data {
            let topic_name = topic.name.clone();
            let topic_storage = match self.meta.get_topics_by_name(topic_name.clone()).await {
                Ok(Topic { properties, .. }) => match &properties.format {
                    TopicFormat::Kafka => TopicStorage::new(properties.0.storage),
                    format => {
                        log::error!("unsupported topic format: {format:?}");
                        let mut partition_responses = vec![];
                        for partition in topic.partition_data {
                            if partition.records.is_some() {
                                partition_responses.push(PartitionProduceResponse {
                                    error_code: ErrorCode::UNSUPPORTED_FOR_MESSAGE_FORMAT.code(),
                                    error_message: Some(format!(
                                        "unsupported topic format: {format:?}"
                                    )),
                                    ..Default::default()
                                });
                            }
                        }
                        responses.push(TopicProduceResponse {
                            name: topic_name,
                            partition_responses,
                            ..Default::default()
                        });
                        continue;
                    }
                },
                Err(err) => {
                    log::error!("malformed record batches: {err:?}");
                    let mut partition_responses = vec![];
                    for partition in topic.partition_data {
                        if partition.records.is_some() {
                            partition_responses.push(PartitionProduceResponse {
                                error_code: ErrorCode::INVALID_RECORD.code(),
                                error_message: Some(format!("malformed record batches: {err}")),
                                ..Default::default()
                            });
                        }
                    }
                    responses.push(TopicProduceResponse {
                        name: topic_name,
                        partition_responses,
                        ..Default::default()
                    });
                    continue;
                }
            };

            let mut partition_responses = vec![];
            for partition in topic.partition_data {
                let Some(records) = partition.records else {
                    continue;
                };

                let idx = partition.index;
                let topic_name = topic.name.clone();
                let record_batches = RecordBatches::new(records);
                let batches = match record_batches.batches() {
                    Ok(batches) => batches,
                    Err(err) => {
                        log::error!("malformed record batches: {err:?}");
                        partition_responses.push(PartitionProduceResponse {
                            error_code: ErrorCode::INVALID_RECORD.code(),
                            error_message: Some(format!("malformed record batches: {err}")),
                            ..Default::default()
                        });
                        continue;
                    }
                };

                let record_len: i32 = batches
                    .into_iter() // HACK: consume batches to drop the ref to record_batches
                    .map(|batch| batch.view().records_count())
                    .sum();

                let split_id = match topic_storage
                    .write_to(&topic_name, idx, record_batches.into_bytes())
                    .await
                {
                    Ok(split_id) => split_id,
                    Err(err) => {
                        log::error!("failed to write split: {err:?}");
                        partition_responses.push(PartitionProduceResponse {
                            error_code: ErrorCode::KAFKA_STORAGE_ERROR.code(),
                            error_message: Some(format!("failed to write split: {err}")),
                            ..Default::default()
                        });
                        continue;
                    }
                };

                let commit_record_batches_request = morax_meta::CommitRecordBatchesRequest {
                    topic_name,
                    partition_id: idx,
                    record_len,
                    split_id,
                };

                match self
                    .meta
                    .commit_record_batches(commit_record_batches_request)
                    .await
                {
                    Ok((_, end_offset)) => {
                        partition_responses.push(PartitionProduceResponse {
                            index: idx,
                            base_offset: end_offset - record_len as i64,
                            ..Default::default()
                        });
                    }
                    // TODO(tisonkun): cleanup now dangling splits
                    Err(err) => {
                        log::error!("failed to commit record batches: {err:?}");
                        partition_responses.push(PartitionProduceResponse {
                            error_code: ErrorCode::KAFKA_STORAGE_ERROR.code(),
                            error_message: Some(format!("failed to commit record batches: {err}")),
                            ..Default::default()
                        });
                    }
                }
            }
            responses.push(TopicProduceResponse {
                name: topic_name,
                partition_responses,
                ..Default::default()
            });
        }
        ProduceResponse {
            responses,
            ..Default::default()
        }
    }
}
