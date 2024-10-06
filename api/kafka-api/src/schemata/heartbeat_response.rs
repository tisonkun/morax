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

use crate::codec::Encodable;
use crate::codec::Encoder;
use crate::codec::FixedSizeEncoder;
use crate::codec::Int16;
use crate::codec::Int32;
use crate::codec::RawTaggedField;
use crate::codec::RawTaggedFieldList;
use crate::IoResult;

// Version 1 adds throttle time.
//
// Starting in version 2, on quota violation, brokers send out responses before throttling.
//
// Starting from version 3, heartbeatRequest supports a new field called groupInstanceId to indicate
// member identity across restarts.
//
// Version 4 is the first flexible version.
#[derive(Debug, Default, Clone)]
pub struct HeartbeatResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for HeartbeatResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version >= 1 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        Int16.encode(buf, self.error_code)?;
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
        if version >= 4 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}
