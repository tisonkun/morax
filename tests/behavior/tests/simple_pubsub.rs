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
use insta::assert_compact_json_snapshot;
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
    let resp = r.into_success().unwrap();
    assert_compact_json_snapshot!(resp, @r#"{"name": "wal"}"#);

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
    let resp = r.into_success().unwrap();
    assert_compact_json_snapshot!(resp, @r#"{"message_ids": ["0", "1"]}"#);

    let r = testkit
        .client
        .create_subscription(
            subscription_name.clone(),
            CreateSubscriptionRequest { topic_name },
        )
        .await
        .unwrap();
    let resp = r.into_success().unwrap();
    assert_compact_json_snapshot!(resp, @r#"{"topic": "wal", "name": "wal_sub"}"#);

    let r = testkit
        .client
        .pull(
            subscription_name.clone(),
            PullMessageRequest { max_messages: 64 },
        )
        .await
        .unwrap();
    let resp = r.into_success().unwrap();
    assert_compact_json_snapshot!(resp, {
        ".messages[].message.publish_time" => "2025-10-01T00:00:00Z",
    }, @r#"
    {
      "messages": [
        {
          "ack_id": "0",
          "message": {
            "message_id": "0",
            "publish_time": "2025-10-01T00:00:00Z",
            "data": "MA=="
          }
        },
        {
          "ack_id": "1",
          "message": {
            "message_id": "1",
            "publish_time": "2025-10-01T00:00:00Z",
            "data": "MQ=="
          }
        }
      ]
    }
    "#);

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
    let resp = r.into_success().unwrap();
    assert_compact_json_snapshot!(resp, @"{}");
}
