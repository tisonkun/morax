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

// Version 1 is the same as version 0.
//
// Starting in Version 2, the requester must be able to handle Kafka Log
// Message format version 1.
//
// Version 3 adds MaxBytes.  Starting in version 3, the partition ordering in
// the request is now relevant.  Partitions will be processed in the order
// they appear in the request.
//
// Version 4 adds IsolationLevel.  Starting in version 4, the requester must be
// able to handle Kafka log message format version 2.
//
// Version 5 adds LogStartOffset to indicate the earliest available offset of
// partition data that can be consumed.
//
// Version 6 is the same as version 5.
//
// Version 7 adds incremental fetch request support.
//
// Version 8 is the same as version 7.
//
// Version 9 adds CurrentLeaderEpoch, as described in KIP-320.
//
// Version 10 indicates that we can use the ZStd compression algorithm, as
// described in KIP-110.
// Version 12 adds flexible versions support as well as epoch validation through
// the `LastFetchedEpoch` field
//
// Version 13 replaces topic names with topic IDs (KIP-516). May return UNKNOWN_TOPIC_ID error code.
//
// Version 14 is the same as version 13 but it also receives a new error called
// OffsetMovedToTieredStorageException(KIP-405)
//
// Version 15 adds the ReplicaState which includes new field ReplicaEpoch and the ReplicaId. Also,
// deprecate the old ReplicaId field and set its default value to -1. (KIP-903)

#[derive(Debug, Default, Clone)]
pub struct FetchRequest {
    /// The clusterId if known. This is used to validate metadata fetches prior to broker
    /// registration.
    pub cluster_id: Option<String>,
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    pub replica_id: i32,
    pub replica_state: ReplicaState,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED
    /// (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1),
    /// non-transactional and COMMITTED transactional records are visible. To be more concrete,
    /// READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable
    /// offset), and enables the inclusion of the list of aborted transactions in the result, which
    /// allows consumers to discard ABORTED transactional records
    pub isolation_level: i8,
    /// The fetch session ID.
    pub session_id: i32,
    /// The fetch session epoch, which is used for ordering requests in a session.
    pub session_epoch: i32,
    /// The topics to fetch.
    pub topics: Vec<FetchTopic>,
    /// In an incremental fetch request, the partitions to remove.
    pub forgotten_topics_data: Vec<ForgottenTopic>,
    /// Rack ID of the consumer making this request.
    pub rack_id: String,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for FetchRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = FetchRequest {
            replica_id: -1,
            max_bytes: i32::MAX,
            session_epoch: -1,
            ..Default::default()
        };
        if version <= 14 {
            this.replica_id = Int32.decode(buf)?
        }
        this.max_wait_ms = Int32.decode(buf)?;
        this.min_bytes = Int32.decode(buf)?;
        if version >= 3 {
            this.max_bytes = Int32.decode(buf)?;
        }
        if version >= 4 {
            this.isolation_level = Int8.decode(buf)?;
        }
        if version >= 7 {
            this.session_id = Int32.decode(buf)?;
        }
        if version >= 7 {
            this.session_epoch = Int32.decode(buf)?;
        }
        this.topics = NullableArray(Struct(version), version >= 12)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("topics"))?;
        if version >= 7 {
            this.forgotten_topics_data = NullableArray(Struct(version), version >= 12)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("forgotten_topics_data"))?;
        }
        if version >= 11 {
            this.rack_id = NullableString(version >= 12)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("rack_id"))?;
        }
        if version >= 12 {
            this.unknown_tagged_fields =
                RawTaggedFieldList.decode_with(buf, |buf, tag, _| match tag {
                    0 => {
                        this.cluster_id = NullableString(true).decode(buf)?;
                        Ok(true)
                    }
                    1 => {
                        if version >= 15 {
                            this.replica_state = ReplicaState::read(buf, version)?;
                        }
                        Ok(true)
                    }
                    _ => Ok(false),
                })?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ReplicaState {
    /// The replica ID of the follower, or -1 if this request is from a consumer.
    pub replica_id: i32,
    /// The epoch of this follower, or -1 if not available.
    pub replica_epoch: i64,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for ReplicaState {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 15 {
            Err(err_decode_message_unsupported(version, "ReplicaState"))?
        }

        Ok(ReplicaState {
            replica_id: Int32.decode(buf)?,
            replica_epoch: Int64.decode(buf)?,
            unknown_tagged_fields: RawTaggedFieldList.decode(buf)?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct FetchTopic {
    /// The name of the topic to fetch.
    pub topic: String,
    /// The unique topic ID
    pub topic_id: uuid::Uuid,
    /// The partitions to fetch.
    pub partitions: Vec<FetchPartition>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for FetchTopic {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 15 {
            Err(err_decode_message_unsupported(version, "FetchTopic"))?
        }
        let mut this = FetchTopic::default();
        if version <= 12 {
            this.topic = NullableString(version >= 12)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("topic"))?;
        }
        if version >= 13 {
            this.topic_id = Uuid.decode(buf)?;
        }
        this.partitions = NullableArray(Struct(version), version >= 12)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("partitions"))?;
        if version >= 12 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct FetchPartition {
    /// The partition index.
    pub partition: i32,
    /// The current leader epoch of the partition.
    pub current_leader_epoch: i32,
    /// The message offset.
    pub fetch_offset: i64,
    /// The epoch of the last fetched record or -1 if there is none
    pub last_fetched_epoch: i32,
    /// The earliest available offset of the follower replica.
    ///
    /// The field is only used when the request is sent by the follower.
    pub log_start_offset: i64,
    /// The maximum bytes to fetch from this partition.
    ///
    /// See KIP-74 for cases where this limit may not be honored.
    pub partition_max_bytes: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for FetchPartition {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 15 {
            Err(err_decode_message_unsupported(version, "FetchPartition"))?
        }
        let mut this = FetchPartition {
            partition: Int32.decode(buf)?,
            ..Default::default()
        };
        this.current_leader_epoch = if version >= 9 { Int32.decode(buf)? } else { -1 };
        this.fetch_offset = Int64.decode(buf)?;
        this.last_fetched_epoch = if version >= 12 {
            Int32.decode(buf)?
        } else {
            -1
        };
        this.log_start_offset = if version >= 5 { Int64.decode(buf)? } else { -1 };
        this.partition_max_bytes = Int32.decode(buf)?;
        if version >= 12 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ForgottenTopic {
    /// The topic name.
    pub topic: String,
    /// The unique topic ID
    pub topic_id: uuid::Uuid,
    /// The partitions indexes to forget.
    pub partitions: Vec<i32>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for ForgottenTopic {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 15 {
            Err(err_decode_message_unsupported(version, "ForgottenTopic"))?
        }
        let mut this = ForgottenTopic::default();
        if version <= 12 {
            this.topic = NullableString(version >= 12)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("topic"))?;
        }
        if version >= 13 {
            this.topic_id = Uuid.decode(buf)?;
        }
        this.partitions = NullableArray(Int32, version >= 12)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("partitions"))?;
        if version >= 12 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
