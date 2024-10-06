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

use byteorder::ReadBytesExt;

use crate::codec::*;
use crate::IoResult;

// In version 0, the request read offsets from ZK.
//
// Starting in version 1, the broker supports fetching offsets from the internal __consumer_offsets
// topic.
//
// Starting in version 2, the request can contain a null topics array to indicate that offsets
// for all topics should be fetched. It also returns a top level error code
// for group or coordinator level errors.
//
// Version 3, 4, and 5 are the same as version 2.
//
// Version 6 is the first flexible version.
//
// Version 7 is adding the require stable flag.
//
// Version 8 is adding support for fetching offsets for multiple groups at a time

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchRequest {
    /// The group to fetch offsets for.
    pub group_id: String,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    pub topics: Vec<OffsetFetchRequestTopic>,
    /// Each group we would like to fetch offsets for.
    pub groups: Vec<OffsetFetchRequestGroup>,
    /// Whether broker should hold on returning unstable offsets but set a retryable error code for
    /// the partitions.
    pub require_stable: bool,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for OffsetFetchRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = OffsetFetchRequest::default();
        if version <= 7 {
            this.group_id = NullableString(version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("groups"))?;
            this.topics = NullableArray(Struct(version), version >= 6)
                .decode(buf)?
                .or_else(|| if version >= 2 { Some(vec![]) } else { None })
                .ok_or_else(|| err_decode_message_null("topics"))?;
        }
        if version >= 8 {
            this.groups = NullableArray(Struct(version), true)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("groups"))?;
        }
        if version >= 7 {
            this.require_stable = Bool.decode(buf)?;
        }
        if version >= 6 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchRequestTopic {
    /// The topic name.
    pub name: String,
    /// The partition indexes we would like to fetch offsets for.
    pub partition_indexes: Vec<i32>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for OffsetFetchRequestTopic {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = OffsetFetchRequestTopic {
            name: NullableString(version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("name"))?,
            partition_indexes: NullableArray(Int32, version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("partition_indexes"))?,
            ..Default::default()
        };
        if version >= 6 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchRequestGroup {
    /// The group ID.
    pub group_id: String,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    pub topics: Vec<OffsetFetchRequestTopic>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for OffsetFetchRequestGroup {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 8 {
            Err(err_decode_message_unsupported(
                version,
                "OffsetFetchRequestGroup",
            ))?
        }
        let mut this = OffsetFetchRequestGroup {
            group_id: NullableString(true)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("group_id"))?,
            topics: NullableArray(Struct(version), true)
                .decode(buf)?
                .unwrap_or_default(),
            ..Default::default()
        };
        if version >= 6 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
