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

use serde::Deserialize;
use serde::Serialize;

use crate::property::StorageProperty;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubsubMessage {
    /// The server-assigned ID of each published message. Guaranteed to be unique within the topic.
    /// It must not be populated by the publisher in a publish call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// The time at which the message was published, populated by the server. It must not be
    /// populated by the publisher in a publish call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_time: Option<jiff::Timestamp>,
    /// Optional. Attributes for this message. If this field is empty, the message must contain
    /// non-empty data. This can be used to filter messages on the subscription.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    /// A padded, base64-encoded string of bytes, encoded with a URL and filename safe alphabet
    /// (sometimes referred to as "web-safe" or "base64url"). Defined by [RFC4648].
    ///
    /// [RFC4648]: https://datatracker.ietf.org/doc/html/rfc4648
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivedMessage {
    /// This ID can be used to acknowledge the received message.
    pub ack_id: String,
    /// The message.
    pub message: PubsubMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicRequest {
    /// Optional. The [`StorageProperty`] for the topic. If not specified, the default storage
    /// property will be used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageProperty>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicResponse {
    /// Name of the topic.
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishMessageRequest {
    /// Required. The messages to publish.
    pub messages: Vec<PubsubMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishMessageResponse {
    /// The server-assigned ID of each published message, in the same order as the messages in the
    /// request. IDs are guaranteed to be unique within the topic.
    pub message_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSubscriptionRequest {
    /// Required. The name of the topic from which this subscription is receiving messages.
    pub topic_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSubscriptionResponse {
    /// The name of the topic from which this subscription is receiving messages.
    pub topic: String,
    /// Name of the subscription.
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullMessageRequest {
    /// Required. The maximum number of messages to return for this request. Must be a positive
    /// integer.
    pub max_messages: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullMessageResponse {
    /// Received Pub/Sub messages.
    pub messages: Vec<ReceivedMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcknowledgeRequest {
    /// Required. The acknowledgment ID for the messages being acknowledged. Must not be empty.
    pub ack_ids: Vec<String>,
}
