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
use morax_api::property::StorageProperty;
use morax_api::property::TopicProperty;
use morax_api::request::AcknowledgeRequest;
use morax_api::request::AcknowledgeResponse;
use morax_api::request::CreateSubscriptionRequest;
use morax_api::request::CreateSubscriptionResponse;
use morax_api::request::CreateTopicRequest;
use morax_api::request::CreateTopicResponse;
use morax_api::request::PublishMessageRequest;
use morax_api::request::PublishMessageResponse;
use morax_api::request::PubsubMessage;
use morax_api::request::PullMessageRequest;
use morax_api::request::PullMessageResponse;
use morax_api::request::ReceivedMessage;
use morax_meta::PostgresMetaService;
use morax_storage::TopicStorage;

use crate::BrokerError;

#[derive(Debug, Clone)]
pub struct Broker {
    meta: Arc<PostgresMetaService>,
    default_storage: StorageProperty,
}

impl Broker {
    pub fn new(meta: Arc<PostgresMetaService>, default_storage: StorageProperty) -> Self {
        Broker {
            meta,
            default_storage,
        }
    }

    pub async fn create_topic(
        &self,
        topic_name: String,
        request: CreateTopicRequest,
    ) -> Result<CreateTopicResponse, BrokerError> {
        let make_error = || BrokerError(format!("failed to create topic with name {topic_name}"));

        self.meta
            .create_topic(morax_meta::CreateTopicRequest {
                name: topic_name.clone(),
                properties: TopicProperty {
                    storage: request
                        .storage
                        .unwrap_or_else(|| self.default_storage.clone()),
                },
            })
            .await
            .change_context_lazy(make_error)?;

        Ok(CreateTopicResponse { name: topic_name })
    }

    pub async fn create_subscription(
        &self,
        subscription_name: String,
        request: CreateSubscriptionRequest,
    ) -> Result<CreateSubscriptionResponse, BrokerError> {
        let name = subscription_name;
        let topic = request.topic_name;

        let make_error = || {
            BrokerError(format!(
                "failed to create subscription {name} for topic {topic}"
            ))
        };

        self.meta
            .create_subscription(morax_meta::CreateSubscriptionRequest {
                name: name.clone(),
                topic: topic.clone(),
            })
            .await
            .change_context_lazy(make_error)?;

        Ok(CreateSubscriptionResponse { topic, name })
    }

    pub async fn publish(
        &self,
        topic_name: String,
        request: PublishMessageRequest,
    ) -> Result<PublishMessageResponse, BrokerError> {
        let make_error = || BrokerError(format!("failed to publish message to topic {topic_name}"));

        let topic = self
            .meta
            .get_topic_by_name(topic_name.clone())
            .await
            .change_context_lazy(make_error)?;
        let topic_storage = TopicStorage::new(topic.properties.0.storage);

        let now = jiff::Timestamp::now();
        let messages = request
            .messages
            .into_iter()
            .map(|msg| PubsubMessage {
                publish_time: Some(now),
                ..msg
            })
            .collect::<Vec<_>>();
        let messages_count = messages.len();
        let split = serialize_messages(messages)?;
        let split_id = topic_storage
            .write_split(topic.topic_id, split)
            .await
            .change_context_lazy(make_error)?;

        let (start, end) = self
            .meta
            .commit_topic_splits(morax_meta::CommitTopicSplitRequest {
                topic_id: topic.topic_id,
                split_id,
                count: messages_count as i64,
            })
            .await
            .change_context_lazy(make_error)?;

        Ok(PublishMessageResponse {
            message_ids: (start..end).map(|id| id.to_string()).collect(),
        })
    }

    pub async fn pull(
        &self,
        subscription_name: String,
        request: PullMessageRequest,
    ) -> Result<PullMessageResponse, BrokerError> {
        let make_error = || {
            BrokerError(format!(
                "failed to pull messages for subscription {subscription_name}"
            ))
        };

        let subscription = self
            .meta
            .get_subscription_by_name(subscription_name.clone())
            .await
            .change_context_lazy(make_error)?;

        let acks = self
            .meta
            .list_acks(subscription.subscription_id)
            .await
            .change_context_lazy(make_error)?;

        let mut ranges = vec![];
        let mut current = (0, 0);
        for (start, end) in acks {
            if start > current.1 {
                ranges.push((current.1, start));
            }
            current = (start, end);
        }
        ranges.push((current.1, current.1 + request.max_messages));

        let topic = self
            .meta
            .get_topic_by_name(subscription.topic_name)
            .await
            .change_context_lazy(make_error)?;
        let topic_storage = TopicStorage::new(topic.properties.0.storage);

        let mut splits = vec![];
        for (start, end) in ranges {
            splits.extend(
                self.meta
                    .fetch_topic_splits(morax_meta::FetchTopicSplitRequest {
                        topic_id: topic.topic_id,
                        start_offset: start,
                        end_offset: end,
                    })
                    .await
                    .change_context_lazy(make_error)?,
            );
        }

        let mut messages = vec![];
        for split in splits {
            let data = topic_storage
                .read_split(split.topic_id, split.split_id)
                .await
                .change_context_lazy(make_error)?;
            let msgs = deserialize_messages(&data)?;
            for (i, msg) in msgs.into_iter().enumerate() {
                let id = split.start_offset + i as i64;
                messages.push(ReceivedMessage {
                    ack_id: id.to_string(),
                    message: PubsubMessage {
                        message_id: Some(id.to_string()),
                        ..msg
                    },
                })
            }
        }

        Ok(PullMessageResponse { messages })
    }

    pub async fn acknowledge(
        &self,
        subscription_name: String,
        request: AcknowledgeRequest,
    ) -> Result<AcknowledgeResponse, BrokerError> {
        let make_error = || {
            BrokerError(format!(
                "failed to acknowledge messages for subscription {subscription_name}"
            ))
        };

        let subscription = self
            .meta
            .get_subscription_by_name(subscription_name.clone())
            .await
            .change_context_lazy(make_error)?;

        let mut ack_ids = vec![];
        for id in request.ack_ids {
            let id = id.parse::<i64>().change_context_lazy(make_error)?;
            ack_ids.push(id);
        }

        self.meta
            .acknowledge(morax_meta::AcknowledgeRequest {
                subscription_id: subscription.subscription_id,
                ack_ids,
            })
            .await
            .change_context_lazy(make_error)?;

        Ok(AcknowledgeResponse {})
    }
}

fn serialize_messages(messages: Vec<PubsubMessage>) -> Result<Vec<u8>, BrokerError> {
    serde_json::to_vec(&messages)
        .change_context_lazy(|| BrokerError("failed to serialize messages".to_string()))
}

fn deserialize_messages(data: &[u8]) -> Result<Vec<PubsubMessage>, BrokerError> {
    serde_json::from_slice(data)
        .change_context_lazy(|| BrokerError("failed to deserialize messages".to_string()))
}
