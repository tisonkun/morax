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

// Version 1 adds throttle time.
//
// Starting in version 2, on quota violation, brokers send out responses before throttling.
//
// Starting from version 3, syncGroupRequest supports a new field called groupInstanceId to indicate
// member identity across restarts.
//
// Version 4 is the first flexible version.
//
// Starting from version 5, the broker sends back the Protocol Type and the Protocol Name
// to the client (KIP-559).

#[derive(Debug, Default, Clone)]
pub struct SyncGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The group protocol type.
    pub protocol_type: Option<String>,
    /// The group protocol name
    pub protocol_name: Option<String>,
    /// The member assignment.
    pub assignment: Vec<u8>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for SyncGroupResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version >= 1 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        Int16.encode(buf, self.error_code)?;
        if version >= 5 {
            NullableString(true).encode(buf, self.protocol_type.as_deref())?;
            NullableString(true).encode(buf, self.protocol_name.as_deref())?;
        }
        NullableBytes(version >= 4).encode(buf, &self.assignment)?;
        if version >= 4 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        if version >= 1 {
            res += Int32::SIZE; // self.throttle_time_ms
        }
        res += Int16::SIZE; // self.error_code
        if version >= 5 {
            res += NullableString(true).calculate_size(self.protocol_type.as_deref());
            res += NullableString(true).calculate_size(self.protocol_name.as_deref());
        }
        res += NullableBytes(version >= 4).calculate_size(&self.assignment);
        if version >= 4 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}
