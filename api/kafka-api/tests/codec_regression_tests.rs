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

use kafka_api::codec::Encodable;
use kafka_api::schemata::fetch_response::FetchResponse;
use kafka_api::schemata::fetch_response::FetchableTopicResponse;
use kafka_api::schemata::fetch_response::PartitionData;
use uuid::Uuid;

#[test]
fn test_encode_nullable_array_regression() {
    // Background: NullableArray once always calculate None size as 4;
    //  for flexible version, it should be 1 (=VarInt.calculate_size(0)).

    let response = FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses: vec![FetchableTopicResponse {
            topic: "".to_string(),
            topic_id: Uuid::parse_str("e74ffca6-43a3-4134-8981-8104d781bd2d").unwrap(),
            partitions: vec![PartitionData {
                partition_index: 0,
                error_code: 0,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                diverging_epoch: None,
                current_leader: None,
                snapshot_id: None,
                aborted_transactions: None,
                preferred_read_replica: -1,
                records: vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 57, 255, 255, 255, 255, 2, 86, 24, 14, 84, 0,
                    0, 0, 0, 0, 0, 0, 0, 1, 144, 213, 210, 147, 109, 0, 0, 1, 144, 213, 210, 147,
                    109, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 14, 0, 0, 0, 1, 2,
                    49, 0,
                ],
                unknown_tagged_fields: vec![],
            }],
            unknown_tagged_fields: vec![],
        }],
        unknown_tagged_fields: vec![],
    };

    let mut bs = vec![];
    response.write(&mut bs, 15).unwrap();
    assert_eq!(bs.len(), response.calculate_size(15));
}
