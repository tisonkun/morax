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

use byteorder::WriteBytesExt;

use crate::codec::*;
use crate::IoResult;

// Version 1 adds a per-topic error message string.
//
// Version 2 adds the throttle time.
//
// Starting in version 3, on quota violation, brokers send out responses before throttling.
//
// Version 4 makes partitions/replicationFactor optional even when assignments are not present
// (KIP-464).
//
// Version 5 is the first flexible version.
// Version 5 also returns topic configs in the response (KIP-525).
//
// Version 6 is identical to version 5 but may return a THROTTLING_QUOTA_EXCEEDED error
// in the response if the topics creation is throttled (KIP-599).
//
// Version 7 returns the topic ID of the newly created topic if creation is successful.

#[derive(Debug, Default, Clone)]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to create.
    pub topics: Vec<CreatableTopicResult>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for CreateTopicsResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version >= 2 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        NullableArray(Struct(version), version >= 5).encode(buf, self.topics.as_slice())?;
        if version >= 5 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        if version >= 2 {
            res += Int32::SIZE; // self.throttle_time_ms
        }
        res += NullableArray(Struct(version), version >= 5).calculate_size(self.topics.as_slice());
        if version >= 5 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct CreatableTopicResult {
    /// The topic name.
    pub name: String,
    /// The unique topic ID
    pub topic_id: uuid::Uuid,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// Optional topic config error returned if configs are not returned in the response.
    pub topic_config_error_code: i16,
    /// Number of partitions of the topic.
    pub num_partitions: i32,
    /// Replication factor of the topic.
    pub replication_factor: i16,
    /// Configuration of the topic.
    pub configs: Vec<CreatableTopicConfigs>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for CreatableTopicResult {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        NullableString(version >= 5).encode(buf, self.name.as_str())?;
        if version >= 7 {
            Uuid.encode(buf, self.topic_id)?;
        }
        Int16.encode(buf, self.error_code)?;
        if version >= 1 {
            NullableString(version >= 5).encode(buf, self.error_message.as_deref())?;
        }
        if version >= 5 {
            Int32.encode(buf, self.num_partitions)?;
            Int16.encode(buf, self.replication_factor)?;
            NullableArray(Struct(version), true).encode(buf, self.configs.as_slice())?;
        }
        if version >= 5 {
            RawTaggedFieldList.encode_with(buf, 1, &self.unknown_tagged_fields, |buf| {
                RawTaggedFieldWriter.write_field(buf, 0, Int16, self.topic_config_error_code)?;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += NullableString(version >= 5).calculate_size(self.name.as_str());
        if version >= 7 {
            res += Uuid::SIZE; // self.topic_id
        }
        res += Int16::SIZE; // self.error_code
        if version >= 1 {
            res += NullableString(version >= 5).calculate_size(self.error_message.as_deref());
        }
        if version >= 5 {
            res += Int32::SIZE; // self.num_partitions
            res += Int16::SIZE; // self.replication_factor
            res += NullableArray(Struct(version), true).calculate_size(self.configs.as_slice());
        }
        if version >= 5 {
            res += RawTaggedFieldList.calculate_size_with(
                1,
                RawTaggedFieldWriter.calculate_field_size(0, Int16, &self.topic_config_error_code),
                &self.unknown_tagged_fields,
            );
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct CreatableTopicConfigs {
    /// The configuration name.
    pub name: String,
    /// The configuration value.
    pub value: Option<String>,
    /// True if the configuration is read-only.
    pub read_only: bool,
    /// The configuration source.
    pub config_source: i8,
    /// True if this configuration is sensitive.
    pub is_sensitive: bool,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for CreatableTopicConfigs {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version < 5 {
            Err(err_encode_message_unsupported(
                version,
                "CreatableTopicConfigs",
            ))?
        }
        NullableString(true).encode(buf, self.name.as_str())?;
        NullableString(true).encode(buf, self.value.as_deref())?;
        Bool.encode(buf, self.read_only)?;
        Int8.encode(buf, self.config_source)?;
        Bool.encode(buf, self.is_sensitive)?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }

    fn calculate_size(&self, _version: i16) -> usize {
        let mut res = 0;
        res += NullableString(true).calculate_size(self.name.as_str());
        res += NullableString(true).calculate_size(self.value.as_deref());
        res += Bool::SIZE; // self.read_only
        res += Int8::SIZE; // self.config_source
        res += Bool::SIZE; // self.is_sensitive
        res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        res
    }
}
