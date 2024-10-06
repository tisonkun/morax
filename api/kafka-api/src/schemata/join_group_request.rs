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

// Version 1 adds RebalanceTimeoutMs.
//
// Version 2 and 3 are the same as version 1.
//
// Starting from version 4, the client needs to issue a second request to join group
//
// Starting from version 5, we add a new field called groupInstanceId to indicate member identity
// across restarts. with assigned id.
//
// Version 6 is the first flexible version.
//
// Version 7 is the same as version 6.
//
// Version 8 adds the Reason field (KIP-800).
//
// Version 9 is the same as version 8.

#[derive(Debug, Default, Clone)]
pub struct JoinGroupRequest {
    /// The group identifier.
    pub group_id: String,
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout
    /// in milliseconds.
    pub session_timeout_ms: i32,
    /// The maximum time in milliseconds that the coordinator will wait for each member to rejoin
    /// when rebalancing the group.
    pub rebalance_timeout_ms: i32,
    /// The member id assigned by the group coordinator.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<String>,
    /// The unique name the for class of protocols implemented by the group we want to join.
    pub protocol_type: String,
    /// The list of protocols that the member supports.
    pub protocols: Vec<JoinGroupRequestProtocol>,
    /// The reason why the member (re-)joins the group.
    pub reason: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for JoinGroupRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = JoinGroupRequest {
            group_id: NullableString(version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("group_id"))?,
            session_timeout_ms: Int32.decode(buf)?,
            ..Default::default()
        };
        this.rebalance_timeout_ms = if version >= 1 { Int32.decode(buf)? } else { -1 };
        this.member_id = NullableString(version >= 6)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("member_id"))?;
        if version >= 5 {
            this.group_instance_id = NullableString(version >= 6).decode(buf)?;
        }
        this.protocol_type = NullableString(version >= 6)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("protocol_type"))?;
        this.protocols = NullableArray(Struct(version), version >= 6)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("protocols"))?;
        if version >= 8 {
            this.reason = NullableString(true).decode(buf)?;
        }
        if version >= 6 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct JoinGroupRequestProtocol {
    /// The protocol name.
    pub name: String,
    /// The protocol metadata.
    pub metadata: Vec<u8>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for JoinGroupRequestProtocol {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        if version > 9 {
            Err(err_decode_message_unsupported(
                version,
                "JoinGroupRequestProtocol",
            ))?
        }
        let mut this = JoinGroupRequestProtocol {
            name: NullableString(version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("name"))?,
            metadata: NullableBytes(version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("metadata"))?,
            ..Default::default()
        };
        if version >= 6 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
