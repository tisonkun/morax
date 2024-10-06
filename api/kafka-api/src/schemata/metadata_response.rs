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

// Version 1 adds fields for the rack of each broker, the controller id, and
// whether the topic is internal.
//
// Version 2 adds the cluster ID field.
//
// Version 3 adds the throttle time.
//
// Version 4 is the same as version 3.
//
// Version 5 adds a per-partition offline_replicas field. This field specifies
// the list of replicas that are offline.
//
// Starting in version 6, on quota violation, brokers send out responses before throttling.
//
// Version 7 adds the leader epoch to the partition metadata.
//
// Starting in version 8, brokers can send authorized operations for topic and cluster.
//
// Version 9 is the first flexible version.
//
// Version 10 adds topicId.
//
// Version 11 deprecates ClusterAuthorizedOperations. This is now exposed
// by the DescribeCluster API (KIP-700).
//
// Version 12 supports topicId.

#[derive(Debug, Default, Clone)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each broker in the response.
    pub brokers: Vec<MetadataResponseBroker>,
    /// The cluster ID that responding broker belongs to.
    pub cluster_id: Option<String>,
    /// The ID of the controller broker.
    pub controller_id: i32,
    /// Each topic in the response.
    pub topics: Vec<MetadataResponseTopic>,
    /// 32-bit bitfield to represent authorized operations for this cluster.
    pub cluster_authorized_operations: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for MetadataResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version >= 3 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        NullableArray(Struct(version), version >= 9).encode(buf, self.brokers.as_slice())?;
        if version >= 2 {
            NullableString(version >= 9).encode(buf, self.cluster_id.as_deref())?;
        }
        if version >= 1 {
            Int32.encode(buf, self.controller_id)?;
        }
        NullableArray(Struct(version), version >= 9).encode(buf, self.topics.as_slice())?;
        if (8..=10).contains(&version) {
            Int32.encode(buf, self.cluster_authorized_operations)?;
        }
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        if version >= 3 {
            res += Int32::SIZE; // self.throttle_time_ms
        }
        res += NullableArray(Struct(version), version >= 9).calculate_size(self.brokers.as_slice());
        if version >= 2 {
            res += NullableString(version >= 9).calculate_size(self.cluster_id.as_deref());
        }
        if version >= 1 {
            res += Int32::SIZE; // self.controller_id
        }
        res += NullableArray(Struct(version), version >= 9).calculate_size(self.topics.as_slice());
        if (8..=10).contains(&version) {
            res += Int32::SIZE; // self.cluster_authorized_operations
        }
        if version >= 9 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct MetadataResponseBroker {
    /// The broker ID.
    pub node_id: i32,
    /// The broker hostname.
    pub host: String,
    /// The broker port.
    pub port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    pub rack: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for MetadataResponseBroker {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        Int32.encode(buf, self.node_id)?;
        NullableString(version >= 9).encode(buf, self.host.as_str())?;
        Int32.encode(buf, self.port)?;
        if version >= 1 {
            NullableString(version >= 9).encode(buf, self.rack.as_deref())?;
        }
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int32::SIZE; // self.node_id
        res += NullableString(version >= 9).calculate_size(self.host.as_str());
        res += Int32::SIZE; // self.port
        if version >= 1 {
            res += NullableString(version >= 9).calculate_size(self.rack.as_deref());
        }
        if version >= 9 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct MetadataResponseTopic {
    /// The topic error, or 0 if there was no error.
    pub error_code: i16,
    /// The topic name.
    pub name: Option<String>,
    /// The topic id.
    pub topic_id: uuid::Uuid,
    /// True if the topic is internal.
    pub is_internal: bool,
    /// Each partition in the topic.
    pub partitions: Vec<MetadataResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    pub topic_authorized_operations: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for MetadataResponseTopic {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        Int16.encode(buf, self.error_code)?;
        match self.name {
            None => {
                if version >= 12 {
                    NullableString(true).encode(buf, None)?;
                } else {
                    Err(err_encode_message_null("name"))?;
                }
            }
            Some(ref name) => {
                NullableString(version >= 9).encode(buf, name.as_str())?;
            }
        }
        if version >= 10 {
            Uuid.encode(buf, self.topic_id)?;
        }
        if version >= 1 {
            Bool.encode(buf, self.is_internal)?;
        }
        NullableArray(Struct(version), version >= 9).encode(buf, self.partitions.as_slice())?;
        if version >= 8 {
            Int32.encode(buf, self.topic_authorized_operations)?;
        }
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int16::SIZE; // self.error_code
        res += NullableString(version >= 9).calculate_size(self.name.as_deref());
        if version >= 10 {
            res += Uuid::SIZE; // self.topic_id
        }
        if version >= 1 {
            res += Bool::SIZE; // self.is_internal
        }
        res +=
            NullableArray(Struct(version), version >= 9).calculate_size(self.partitions.as_slice());
        if version >= 8 {
            res += Int32::SIZE; // self.topic_authorized_operations
        }
        if version >= 9 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct MetadataResponsePartition {
    /// The partition error, or 0 if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The leader epoch of this partition.
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    pub isr_nodes: Vec<i32>,
    /// The set of offline replicas of this partition.
    pub offline_replicas: Vec<i32>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for MetadataResponsePartition {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        Int16.encode(buf, self.error_code)?;
        Int32.encode(buf, self.partition_index)?;
        Int32.encode(buf, self.leader_id)?;
        if version >= 7 {
            Int32.encode(buf, self.leader_epoch)?;
        }
        NullableArray(Int32, version >= 9).encode(buf, self.replica_nodes.as_slice())?;
        NullableArray(Int32, version >= 9).encode(buf, self.isr_nodes.as_slice())?;
        if version >= 5 {
            NullableArray(Int32, version >= 9).encode(buf, self.offline_replicas.as_slice())?;
        }
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int16::SIZE; // self.error_code
        res += Int32::SIZE; // self.partition_index
        res += Int32::SIZE; // self.leader_id
        if version >= 7 {
            res += Int32::SIZE; // self.leader_epoch
        }
        res += NullableArray(Int32, version >= 9).calculate_size(self.replica_nodes.as_slice());
        res += NullableArray(Int32, version >= 9).calculate_size(self.isr_nodes.as_slice());
        if version >= 5 {
            res +=
                NullableArray(Int32, version >= 9).calculate_size(self.offline_replicas.as_slice());
        }
        if version >= 9 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}
