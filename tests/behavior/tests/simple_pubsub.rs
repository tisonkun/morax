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

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use behavior_tests::harness;
use behavior_tests::Testkit;
use insta::assert_compact_debug_snapshot;
use morax_api::request::AcknowledgeRequest;
use morax_api::request::CreateSubscriptionRequest;
use morax_api::request::CreateTopicRequest;
use morax_api::request::PublishMessageRequest;
use morax_api::request::PubsubMessage;
use morax_api::request::PullMessageRequest;
use test_harness::test;

fn make_entry(payload: &str) -> PubsubMessage {
    PubsubMessage {
        message_id: None,
        publish_time: None,
        attributes: Default::default(),
        data: BASE64_STANDARD.encode(payload),
    }
}

#[test(harness)]
async fn test_simple_pubsub(testkit: Testkit) {
    let topic_name = "wal".to_string();
    let subscription_name = "wal_sub".to_string();

    let r = testkit
        .client
        .create_topic(topic_name.clone(), CreateTopicRequest { storage: None })
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @r#"Success(CreateTopicResponse { name: "wal" })"#);

    let r = testkit
        .client
        .publish(
            topic_name.clone(),
            PublishMessageRequest {
                messages: vec![make_entry("0"), make_entry("1")],
            },
        )
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @r#"Success(PublishMessageResponse { message_ids: ["0", "1"] })"#);

    let r = testkit
        .client
        .create_subscription(
            subscription_name.clone(),
            CreateSubscriptionRequest { topic_name },
        )
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @r#"Success(CreateSubscriptionResponse { topic: "wal", name: "wal_sub" })"#);

    let r = testkit
        .client
        .pull(
            subscription_name.clone(),
            PullMessageRequest { max_messages: 64 },
        )
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @r#"Success(PullMessageResponse { messages: [ReceivedMessage { ack_id: "0", message: PubsubMessage { message_id: Some("0"), publish_time: Some(2025-04-20T11:05:17.52495Z), attributes: {}, data: "MA==" } }, ReceivedMessage { ack_id: "1", message: PubsubMessage { message_id: Some("1"), publish_time: Some(2025-04-20T11:05:17.52495Z), attributes: {}, data: "MQ==" } }] })"#);

    let resp = r.into_success().unwrap();
    let ack_ids = resp
        .messages
        .into_iter()
        .map(|m| m.ack_id)
        .collect::<Vec<_>>();
    let r = testkit
        .client
        .acknowledge(subscription_name.clone(), AcknowledgeRequest { ack_ids })
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @"Error(ErrorStatus { code: 422, payload: [102, 97, 105, 108, 101, 100, 32, 116, 111, 32, 97, 99, 107, 110, 111, 119, 108, 101, 100, 103, 101, 32, 109, 101, 115, 115, 97, 103, 101, 115, 32, 102, 111, 114, 32, 115, 117, 98, 115, 99, 114, 105, 112, 116, 105, 111, 110, 32, 119, 97, 108, 95, 115, 117, 98] })");
}
