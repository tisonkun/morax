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

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use insta::assert_compact_debug_snapshot;
use morax_protos::rpc::AppendLogRequest;
use morax_protos::rpc::CreateLogRequest;
use morax_protos::rpc::Entry;
use morax_protos::rpc::ReadLogRequest;
use test_harness::test;
use testkit::harness;
use testkit::Testkit;

fn make_entry(payload: &str) -> Entry {
    Entry {
        index: None,
        data: BASE64_STANDARD.encode(payload),
    }
}

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
            entries: vec![make_entry("0"), make_entry("1")],
        })
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @"Success(AppendLogResponse { offsets: 0..2 })");

    let r = testkit
        .client
        .read_log(ReadLogRequest { name, offset: 0 })
        .await
        .unwrap();
    assert_compact_debug_snapshot!(r, @r###"Success(ReadLogResponse { entries: [Entry { index: Some(0), data: "MA==" }, Entry { index: Some(1), data: "MQ==" }] })"###);
}
