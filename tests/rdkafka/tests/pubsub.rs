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

use std::time::Duration;

use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use rdkafka::Message;
use test_harness::test;
use testkit::harness;
use testkit::Testkit;

#[test(harness)]
async fn test_simple_pubsub(testkit: Testkit) {
    let Testkit {
        admin,
        producer,
        consumer,
    } = testkit;
    let topic = "test_basic_pubsub";

    admin
        .create_topics(
            [&NewTopic {
                name: topic,
                num_partitions: 2,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &Default::default(),
        )
        .await
        .unwrap();

    for i in 0..9 {
        let value = i.to_string();
        let (partition, offset) = producer
            .send(
                FutureRecord::to(topic)
                    .payload(value.as_bytes())
                    .key(format!("alice-{i}").as_bytes()),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .unwrap();

        log::info!(
            "Successfully produced record to topic {} partition [{}] @ offset {}",
            topic,
            partition,
            offset
        );
    }

    consumer.subscribe(&[topic]).unwrap();
    for _ in 0..9 {
        let msg = consumer.recv().await.unwrap();
        let payload = match msg.payload_view::<str>() {
            None => "",
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                log::error!("Error while deserializing message payload: {:?}", e);
                ""
            }
        };
        log::info!(
            "key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
            msg.key(),
            payload,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.timestamp()
        );
    }
}
