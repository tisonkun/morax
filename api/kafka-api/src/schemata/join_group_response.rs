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

// Version 1 is the same as version 0.
//
// Version 2 adds throttle time.
//
// Starting in version 3, on quota violation, brokers send out responses before throttling.
//
// Starting in version 4, the client needs to issue a second request to join group
// with assigned id.
//
// Version 5 is bumped to apply group.instance.id to identify member across restarts.
//
// Version 6 is the first flexible version.
//
// Starting from version 7, the broker sends back the Protocol Type to the client (KIP-559).
//
// Version 8 is the same as version 7.
//
// Version 9 adds the SkipAssignment field.

#[derive(Debug, Default, Clone)]
pub struct JoinGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The generation ID of the group.
    pub generation_id: i32,
    /// The group protocol name.
    pub protocol_type: Option<String>,
    /// The group protocol selected by the coordinator.
    pub protocol_name: Option<String>,
    /// The leader of the group.
    pub leader: String,
    /// True if the leader must skip running the assignment.
    pub skip_assignment: bool,
    /// The member id assigned by the group coordinator.
    pub member_id: String,
    pub members: Vec<JoinGroupResponseMember>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for JoinGroupResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version >= 2 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        Int16.encode(buf, self.error_code)?;
        Int32.encode(buf, self.generation_id)?;
        if version >= 7 {
            NullableString(true).encode(buf, self.protocol_type.as_deref())?;
        }
        if version < 7 && self.protocol_name.is_none() {
            Err(err_encode_message_null("protocol_name"))?
        }
        NullableString(version >= 6).encode(buf, self.protocol_name.as_deref())?;
        NullableString(version >= 6).encode(buf, self.leader.as_str())?;
        if version >= 9 {
            Bool.encode(buf, self.skip_assignment)?;
        }
        NullableString(version >= 6).encode(buf, self.member_id.as_str())?;
        NullableArray(Struct(version), version >= 6).encode(buf, self.members.as_slice())?;
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        if version >= 2 {
            res += Int32::SIZE; // self.throttle_time_ms
        }
        res += Int16::SIZE; // self.error_code
        res += Int32::SIZE; // self.generation_id
        if version >= 7 {
            res += NullableString(true).calculate_size(self.protocol_type.as_deref());
        }
        res += NullableString(version >= 6).calculate_size(self.protocol_name.as_deref());
        res += NullableString(version >= 6).calculate_size(self.leader.as_str());
        if version >= 9 {
            res += Bool::SIZE; // self.skip_assignment
        }
        res += NullableString(version >= 6).calculate_size(self.member_id.as_str());
        res += NullableArray(Struct(version), version >= 6).calculate_size(self.members.as_slice());
        if version >= 6 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct JoinGroupResponseMember {
    /// The group member ID
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<String>,
    /// The group member metadata.
    pub metadata: Vec<u8>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for JoinGroupResponseMember {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        NullableString(version >= 6).encode(buf, self.member_id.as_str())?;
        if version >= 5 {
            NullableString(version >= 6).encode(buf, self.group_instance_id.as_deref())?;
        }
        NullableBytes(version >= 6).encode(buf, &self.metadata)?;
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += NullableString(version >= 6).calculate_size(self.member_id.as_str());
        if version >= 5 {
            res += NullableString(version >= 6).calculate_size(self.group_instance_id.as_deref());
        }
        res += NullableBytes(version >= 6).calculate_size(&self.metadata);
        if version >= 6 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}
