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

use kafka_api::schemata::api_versions_request::ApiVersionsRequest;
use kafka_api::schemata::api_versions_response::ApiVersion;
use kafka_api::schemata::api_versions_response::ApiVersionsResponse;
use kafka_api::schemata::apikey::ApiMessageType;
use kafka_api::schemata::create_topic_request::CreateTopicsRequest;
use kafka_api::schemata::create_topic_response::CreatableTopicResult;
use kafka_api::schemata::create_topic_response::CreateTopicsResponse;
use kafka_api::schemata::error::ErrorCode;
use kafka_api::schemata::find_coordinator_request::FindCoordinatorRequest;
use kafka_api::schemata::find_coordinator_response::Coordinator;
use kafka_api::schemata::find_coordinator_response::FindCoordinatorResponse;
use kafka_api::schemata::heartbeat_request::HeartbeatRequest;
use kafka_api::schemata::heartbeat_response::HeartbeatResponse;
use kafka_api::schemata::metadata_request::MetadataRequest;
use kafka_api::schemata::metadata_response::MetadataResponse;
use kafka_api::schemata::metadata_response::MetadataResponseBroker;
use kafka_api::schemata::metadata_response::MetadataResponsePartition;
use kafka_api::schemata::metadata_response::MetadataResponseTopic;
use morax_protos::property::TopicProps;

use crate::broker::Broker;

impl Broker {
    pub(super) fn receive_api_versions(&self, _request: ApiVersionsRequest) -> ApiVersionsResponse {
        let api_keys = supported_apis()
            .iter()
            .map(|api| ApiVersion {
                api_key: api.api_key(),
                min_version: api.lowest_supported_version(),
                max_version: api.highest_supported_version(),
                ..Default::default()
            })
            .collect();

        ApiVersionsResponse {
            error_code: 0,
            api_keys,
            ..Default::default()
        }
    }

    pub(super) fn receive_find_coordinator(
        &self,
        request: FindCoordinatorRequest,
    ) -> FindCoordinatorResponse {
        let mut coordinators = vec![];

        if !request.key.is_empty() {
            coordinators.push(Coordinator {
                key: request.key,
                node_id: self.cluster_meta.broker.node_id,
                host: self.cluster_meta.broker.host.clone(),
                port: self.cluster_meta.broker.port,
                ..Default::default()
            });
        }

        for key in request.coordinator_keys {
            coordinators.push(Coordinator {
                key,
                node_id: self.cluster_meta.broker.node_id,
                host: self.cluster_meta.broker.host.clone(),
                port: self.cluster_meta.broker.port,
                ..Default::default()
            });
        }

        FindCoordinatorResponse {
            coordinators,
            node_id: self.cluster_meta.broker.node_id,
            host: self.cluster_meta.broker.host.clone(),
            port: self.cluster_meta.broker.port,
            ..Default::default()
        }
    }

    pub(super) async fn receive_metadata(&self, _request: MetadataRequest) -> MetadataResponse {
        let broker_meta = self.cluster_meta.broker.clone();
        let brokers = vec![MetadataResponseBroker {
            node_id: broker_meta.node_id,
            host: broker_meta.host,
            port: broker_meta.port,
            ..Default::default()
        }];

        match self.meta.get_all_topics().await {
            Ok(topics) => MetadataResponse {
                brokers,
                cluster_id: Some(self.cluster_meta.cluster_id.clone()),
                controller_id: self.cluster_meta.controller_id,
                topics: topics
                    .into_iter()
                    .map(|topic| MetadataResponseTopic {
                        name: Some(topic.name),
                        topic_id: topic.id,
                        partitions: (0..topic.partitions)
                            .map(|idx| MetadataResponsePartition {
                                partition_index: idx,
                                leader_id: self.cluster_meta.broker.node_id,
                                replica_nodes: vec![self.cluster_meta.broker.node_id],
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            Err(err) => {
                log::error!("failed to get topic: {err:?}");
                MetadataResponse {
                    brokers,
                    cluster_id: Some(self.cluster_meta.cluster_id.clone()),
                    controller_id: self.cluster_meta.controller_id,
                    ..Default::default()
                }
            }
        }
    }

    pub(super) async fn receive_create_topic(
        &self,
        request: CreateTopicsRequest,
    ) -> CreateTopicsResponse {
        let mut topics = vec![];
        for topic in request.topics {
            let create_topic_request = morax_meta::CreateTopicRequest {
                name: topic.name.clone(),
                partitions: topic.num_partitions.max(1),
                properties: TopicProps {
                    storage: self.fallback_storage.clone(),
                },
            };

            match self.meta.create_topic(create_topic_request).await {
                Ok(topic) => {
                    topics.push(CreatableTopicResult {
                        name: topic.name,
                        topic_id: topic.id,
                        num_partitions: topic.partitions,
                        ..Default::default()
                    });
                }
                Err(err) => {
                    log::error!("failed to create topic: {err:?}");
                    let error = ErrorCode::TOPIC_ALREADY_EXISTS;
                    topics.push(CreatableTopicResult {
                        name: topic.name,
                        error_code: error.code(),
                        error_message: Some(error.message().to_string()),
                        ..Default::default()
                    });
                }
            }
        }
        CreateTopicsResponse {
            topics,
            ..Default::default()
        }
    }

    pub(super) fn receive_heartbeat(&self, _request: HeartbeatRequest) -> HeartbeatResponse {
        // TODO(tisonkun): implement group heartbeat and expiration; currently, it always succeeds
        //  and group expiration is not implemented.
        HeartbeatResponse::default()
    }
}

const fn supported_apis() -> &'static [ApiMessageType] {
    &[
        ApiMessageType::API_VERSIONS,
        ApiMessageType::CREATE_TOPICS,
        ApiMessageType::FETCH,
        ApiMessageType::FIND_COORDINATOR,
        ApiMessageType::INIT_PRODUCER_ID,
        ApiMessageType::JOIN_GROUP,
        ApiMessageType::METADATA,
        ApiMessageType::OFFSET_FETCH,
        ApiMessageType::PRODUCE,
        ApiMessageType::SYNC_GROUP,
        ApiMessageType::HEARTBEAT,
    ]
}
