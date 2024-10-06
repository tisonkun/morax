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

// In version 0, an empty array indicates "request metadata for all topics."  In version 1 and
// higher, an empty array indicates "request metadata for no topics," and a null array is used to
// indicate "request metadata for all topics."
//
// Version 2 and 3 are the same as version 1.
//
// Version 4 adds AllowAutoTopicCreation.
//
// Starting in version 8, authorized operations can be requested for cluster and topic resource.
//
// Version 9 is the first flexible version.
//
// Version 10 adds topicId and allows name field to be null. However, this functionality was not
// implemented on the server. Versions 10 and 11 should not use the topicId field or set topic name
// to null.
//
// Version 11 deprecates IncludeClusterAuthorizedOperations field. This is now exposed
// by the DescribeCluster API (KIP-700).
//
// Version 12 supports topic ID.

#[derive(Debug, Default, Clone)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for.
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// If this is true, the broker may auto-create topics that we requested which do not already
    /// exist, if it is configured to do so.
    pub allow_auto_topic_creation: bool,
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
    /// Whether to include topic authorized operations.
    pub include_topic_authorized_operations: bool,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for MetadataRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = MetadataRequest::default();
        if version >= 1 {
            this.topics = NullableArray(Struct(version), version >= 9).decode(buf)?;
        } else {
            let topics = NullableArray(Struct(version), version >= 9)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("topics"))?;
            this.topics = if topics.is_empty() {
                None
            } else {
                Some(topics)
            };
        }
        if version >= 4 {
            this.allow_auto_topic_creation = Bool.decode(buf)?;
        } else {
            this.allow_auto_topic_creation = true;
        };
        if (8..=10).contains(&version) {
            this.include_cluster_authorized_operations = Bool.decode(buf)?;
        }
        if version >= 9 {
            this.include_topic_authorized_operations = Bool.decode(buf)?;
        }
        if version >= 9 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MetadataRequestTopic {
    /// The topic id.
    pub topic_id: uuid::Uuid,
    /// The topic name.
    pub name: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for MetadataRequestTopic {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 12 {
            Err(err_decode_message_unsupported(
                version,
                "MetadataRequestTopic",
            ))?
        }
        let mut this = MetadataRequestTopic::default();
        if version >= 10 {
            this.topic_id = Uuid.decode(buf)?;
        }
        this.name = if version >= 10 {
            NullableString(true).decode(buf)?
        } else {
            Some(
                NullableString(version >= 9)
                    .decode(buf)?
                    .ok_or_else(|| err_decode_message_null("name"))?,
            )
        };
        if version >= 9 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
