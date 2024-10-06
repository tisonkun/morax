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

use crate::codec::err_decode_message_null;
use crate::codec::Decodable;
use crate::codec::Decoder;
use crate::codec::Int32;
use crate::codec::NullableString;
use crate::codec::RawTaggedField;
use crate::codec::RawTaggedFieldList;
use crate::IoResult;

// Version 1 and version 2 are the same as version 0.
//
// Starting from version 3, we add a new field called groupInstanceId to indicate member identity
// across restarts.
//
// Version 4 is the first flexible version.
#[derive(Debug, Default, Clone)]
pub struct HeartbeatRequest {
    /// The group id.
    pub group_id: String,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for HeartbeatRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = HeartbeatRequest {
            group_id: NullableString(version >= 4)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("topics"))?,
            generation_id: Int32.decode(buf)?,
            member_id: NullableString(version >= 4)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("member_id"))?,
            ..Default::default()
        };
        if version >= 3 {
            this.group_instance_id = NullableString(version >= 4).decode(buf)?;
        }
        if version >= 4 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
