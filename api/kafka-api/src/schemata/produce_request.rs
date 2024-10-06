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

// Version 1 and 2 are the same as version 0.
//
// Version 3 adds the transactional ID, which is used for authorization when attempting to write
// transactional data.  Version 3 also adds support for Kafka Message Format v2.
//
// Version 4 is the same as version 3, but the requester must be prepared to handle a
// KAFKA_STORAGE_ERROR.
//
// Version 5 and 6 are the same as version 3.
//
// Starting in version 7, records can be produced using ZStandard compression.  See KIP-110.
//
// Starting in Version 8, response has RecordErrors and ErrorMessage. See KIP-467.
//
// Version 9 enables flexible versions.

#[derive(Debug, Default, Clone)]
pub struct ProduceRequest {
    /// The transactional ID, or null if the producer is not transactional.
    pub transactional_id: Option<String>,
    /// The number of acknowledgments the producer requires the leader to have received before
    /// considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the
    /// leader and -1 for the full ISR.
    pub acks: i16,
    /// The timeout to await a response in milliseconds.
    pub timeout_ms: i32,
    /// Each topic to produce to.
    pub topic_data: Vec<TopicProduceData>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for ProduceRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = ProduceRequest::default();
        if version >= 3 {
            this.transactional_id = NullableString(version >= 9).decode(buf)?;
        }
        this.acks = Int16.decode(buf)?;
        this.timeout_ms = Int32.decode(buf)?;
        this.topic_data = NullableArray(Struct(version), version >= 9)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("topic_data"))?;
        if version >= 9 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct TopicProduceData {
    /// The topic name.
    pub name: String,
    /// Each partition to produce to.
    pub partition_data: Vec<PartitionProduceData>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for TopicProduceData {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 9 {
            Err(err_decode_message_unsupported(version, "TopicProduceData"))?
        }
        let mut this = TopicProduceData {
            name: NullableString(version >= 9)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("name"))?,
            partition_data: NullableArray(Struct(version), version >= 9)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("partition_data"))?,
            ..Default::default()
        };
        if version >= 9 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct PartitionProduceData {
    /// The partition index.
    pub index: i32,
    /// The record data to be produced.
    pub records: Option<Vec<u8>>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for PartitionProduceData {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 9 {
            Err(err_decode_message_unsupported(
                version,
                "PartitionProduceData",
            ))?
        }
        let mut this = PartitionProduceData {
            index: Int32.decode(buf)?,
            records: NullableBytes(version >= 9).decode(buf)?,
            ..Default::default()
        };
        if version >= 9 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
