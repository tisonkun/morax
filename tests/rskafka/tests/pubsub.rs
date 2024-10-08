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

mod testkit;

use std::collections::BTreeMap;

use rskafka::chrono::TimeZone;
use rskafka::chrono::Utc;
use rskafka::client::partition::Compression;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::record::Record;
use test_harness::test;
use testkit::harness;
use testkit::Testkit;

#[test(harness)]
async fn test_simple_pubsub(testkit: Testkit) {
    let client = testkit.client;
    let topic = "test_basic_pubsub";

    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(topic, 2, 1, 5000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    for i in 0..10 {
        partition_client
            .produce(
                vec![Record {
                    key: None,
                    value: Some(format!("hello kafka {i}").into_bytes()),
                    headers: BTreeMap::from([("foo".to_string(), b"bar".to_vec())]),
                    timestamp: Utc.timestamp_millis_opt(42).unwrap(),
                }],
                Compression::default(),
            )
            .await
            .unwrap();
    }

    let (records, high_watermark) = partition_client
        .fetch_records(0, 1..1_000_000, 1_000)
        .await
        .unwrap();
    assert_eq!(high_watermark, 9);
    assert_eq!(records.len(), 10);

    for (i, record_and_offset) in records.into_iter().enumerate() {
        assert_eq!(record_and_offset.offset, i as i64);

        let record = record_and_offset.record;
        assert_eq!(record.value, Some(format!("hello kafka {i}").into_bytes()));
        assert_eq!(
            record.headers,
            BTreeMap::from([("foo".to_string(), b"bar".to_vec())])
        );
        assert_eq!(record.timestamp, Utc.timestamp_millis_opt(42).unwrap());
    }
}
