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

// Versions 1 and 2 are the same as version 0.
//
// Starting from version 3, we add a new field called groupInstanceId to indicate member identity
// across restarts.
//
// Version 4 is the first flexible version.
//
// Starting from version 5, the client sends the Protocol Type and the Protocol Name
// to the broker (KIP-559). The broker will reject the request if they are inconsistent
// with the Type and Name known by the broker.

#[derive(Debug, Default, Clone)]
pub struct SyncGroupRequest {
    /// The unique group identifier.
    pub group_id: String,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID assigned by the group.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<String>,
    /// The group protocol type.
    pub protocol_type: Option<String>,
    /// The group protocol name
    pub protocol_name: Option<String>,
    /// Each assignment.
    pub assignments: Vec<SyncGroupRequestAssignment>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for SyncGroupRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = SyncGroupRequest {
            group_id: NullableString(version >= 4)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("group_id"))?,
            generation_id: Int32.decode(buf)?,
            member_id: NullableString(version >= 4)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("member_id"))?,
            ..Default::default()
        };
        if version >= 3 {
            this.group_instance_id = NullableString(version >= 4).decode(buf)?;
        }
        if version >= 5 {
            this.protocol_type = NullableString(true).decode(buf)?;
            this.protocol_name = NullableString(true).decode(buf)?;
        }
        this.assignments = NullableArray(Struct(version), version >= 4)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("assignments"))?;
        if version >= 4 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct SyncGroupRequestAssignment {
    /// The ID of the member to assign.
    pub member_id: String,
    /// The member assignment.
    pub assignment: Vec<u8>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for SyncGroupRequestAssignment {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 5 {
            Err(err_decode_message_unsupported(
                version,
                "SyncGroupRequestAssignment",
            ))?
        }
        let mut this = SyncGroupRequestAssignment {
            member_id: NullableString(version >= 4)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("member_id"))?,
            assignment: NullableBytes(version >= 4)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("assignment"))?,
            ..Default::default()
        };
        if version >= 4 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
