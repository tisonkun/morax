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
use kafka_api::schemata::fetch_request::FetchRequest;
use kafka_api::schemata::fetch_response::FetchResponse;
use kafka_api::schemata::fetch_response::FetchableTopicResponse;
use kafka_api::schemata::fetch_response::PartitionData;
use kafka_api::schemata::offset_fetch_request::OffsetFetchRequest;
use kafka_api::schemata::offset_fetch_response::OffsetFetchResponse;
use kafka_api::schemata::offset_fetch_response::OffsetFetchResponseGroup;
use kafka_api::schemata::offset_fetch_response::OffsetFetchResponsePartition;
use kafka_api::schemata::offset_fetch_response::OffsetFetchResponsePartitions;
use kafka_api::schemata::offset_fetch_response::OffsetFetchResponseTopic;
use kafka_api::schemata::offset_fetch_response::OffsetFetchResponseTopics;
use kafka_api::schemata::request_header::RequestHeader;
use morax_meta::TopicPartitionSplit;
use morax_storage::TopicStorage;

use crate::broker::Broker;

impl Broker {
    pub(super) async fn receive_offset_fetch(
        &self,
        header: RequestHeader,
        request: OffsetFetchRequest,
    ) -> OffsetFetchResponse {
        macro_rules! make_offset_fetch_response_topics {
            ($request_topics:expr, $topic_model:ident, $partition_model:ident) => {{
                let mut topics = vec![];
                for topic in $request_topics.iter() {
                    let mut partitions = vec![];
                    for idx in topic.partition_indexes.iter() {
                        partitions.push($partition_model {
                            partition_index: *idx,
                            committed_offset: 0,
                            committed_leader_epoch: 0,
                            ..Default::default()
                        });
                    }
                    topics.push($topic_model {
                        name: topic.name.clone(),
                        partitions,
                        ..Default::default()
                    });
                }
                topics
            }};
        }

        if header.request_api_version <= 7 {
            // fetch single group
            let topics = make_offset_fetch_response_topics!(
                &request.topics,
                OffsetFetchResponseTopic,
                OffsetFetchResponsePartition
            );

            return OffsetFetchResponse {
                topics,
                ..Default::default()
            };
        }

        let mut groups = vec![];
        for group in request.groups.iter() {
            let topics = make_offset_fetch_response_topics!(
                &group.topics,
                OffsetFetchResponseTopics,
                OffsetFetchResponsePartitions
            );
            groups.push(OffsetFetchResponseGroup {
                group_id: group.group_id.clone(),
                topics,
                ..Default::default()
            });
        }

        OffsetFetchResponse {
            groups,
            ..Default::default()
        }
    }

    pub(super) async fn receive_fetch(&self, request: FetchRequest) -> FetchResponse {
        async fn read_splits(
            storage: &TopicStorage,
            splits: &[TopicPartitionSplit],
            partition_id: i32,
        ) -> PartitionData {
            let partition_id = splits
                .iter()
                .map(|s| s.partition_id)
                .fold(partition_id, |a, b| {
                    assert_eq!(a, b, "partition id in one splits batch must be the same");
                    a
                });

            let committed_index = splits.iter().map(|s| s.end_offset).max().unwrap_or(0) - 1;

            let mut records = vec![];
            for split in splits.iter() {
                let split_records = match storage
                    .read_at(&split.topic_name, split.partition_id, &split.split_id)
                    .await
                {
                    Ok(split_records) => split_records,
                    Err(err) => {
                        log::error!("failed to read split: {err:?}");
                        return PartitionData {
                            error_code: ErrorCode::KAFKA_STORAGE_ERROR.code(),
                            ..Default::default()
                        };
                    }
                };
                let mut record_batches = RecordBatches::new(split_records.to_vec());
                let batches = match record_batches.mut_batches() {
                    Ok(batches) => batches,
                    Err(err) => {
                        log::error!("malformed record batches: {err:?}");
                        return PartitionData {
                            error_code: ErrorCode::INVALID_RECORD.code(),
                            ..Default::default()
                        };
                    }
                };
                for mut batch in batches {
                    batch.set_last_offset(split.end_offset - 1)
                }
                records.extend(record_batches.into_bytes());
            }

            PartitionData {
                partition_index: partition_id,
                high_watermark: committed_index,
                last_stable_offset: committed_index,
                log_start_offset: 0,
                records,
                ..Default::default()
            }
        }

        let mut responses = vec![];
        for topic in request.topics.iter() {
            let topic_storage = match if topic.topic_id != uuid::Uuid::default() {
                self.meta.get_topics_by_id(topic.topic_id).await
            } else {
                self.meta.get_topics_by_name(topic.topic.clone()).await
            } {
                Ok(topic) => TopicStorage::new(topic.properties.0.storage),
                Err(err) => {
                    log::error!("failed to fetch topic metadata: {err:?}");
                    let mut partitions = vec![];
                    for _ in topic.partitions.iter() {
                        partitions.push(PartitionData {
                            error_code: ErrorCode::KAFKA_STORAGE_ERROR.code(),
                            ..Default::default()
                        });
                    }
                    responses.push(FetchableTopicResponse {
                        topic: topic.topic.clone(),
                        topic_id: topic.topic_id,
                        partitions,
                        ..Default::default()
                    });
                    continue;
                }
            };

            let mut partitions = vec![];
            for part in topic.partitions.iter() {
                let fetch_record_batches_request = morax_meta::FetchRecordBatchesRequest {
                    topic_id: topic.topic_id,
                    topic_name: topic.topic.clone(),
                    partition_id: part.partition,
                    offset: part.fetch_offset,
                };

                let splits = match self
                    .meta
                    .fetch_record_batches(fetch_record_batches_request)
                    .await
                {
                    Ok(splits) => splits,
                    Err(err) => {
                        log::error!("failed to fetch record batches: {err:?}");
                        partitions.push(PartitionData {
                            error_code: ErrorCode::KAFKA_STORAGE_ERROR.code(),
                            ..Default::default()
                        });
                        continue;
                    }
                };

                let partition = read_splits(&topic_storage, &splits, part.partition).await;
                partitions.push(partition);
            }

            responses.push(FetchableTopicResponse {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            });
        }
        FetchResponse {
            session_id: request.session_id,
            responses,
            ..Default::default()
        }
    }
}
