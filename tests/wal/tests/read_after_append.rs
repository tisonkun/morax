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

use insta::assert_compact_debug_snapshot;
use morax_protos::rpc::AppendLogRequest;
use morax_protos::rpc::CreateLogRequest;
use morax_protos::rpc::ReadLogRequest;
use test_harness::test;
use wal_tests::harness;
use wal_tests::Testkit;

#[test(harness)]
async fn test_simple_pubsub(testkit: Testkit) {
    let name = "db_log".to_string();
    let properties = testkit.topic_props;

    let r = testkit
        .client
        .create_log(CreateLogRequest {
            name: name.clone(),
            properties,
        })
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @r###"Success(CreateLogResponse { name: "db_log" })"###);

    let r = testkit
        .client
        .append_log(AppendLogRequest {
            name: name.clone(),
            data: "0;1;2;3;4;5;6;7;8;9;".as_bytes().to_vec(),
            entry_cnt: 10,
        })
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @"Success(AppendLogResponse { start_offset: 0, end_offset: 10 })");

    let r = testkit
        .client
        .read_log(ReadLogRequest { name, offset: 0 })
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @r###"Success(ReadLogResponse { data: "0;1;2;3;4;5;6;7;8;9;" })"###);
}
