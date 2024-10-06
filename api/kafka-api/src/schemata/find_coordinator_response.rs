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

// Version 1 adds throttle time and error messages.
//
// Starting in version 2, on quota violation, brokers send out responses before throttling.
//
// Version 3 is the first flexible version.
//
// Version 4 adds support for batching via Coordinators (KIP-699)

#[derive(Debug, Default, Clone)]
pub struct FindCoordinatorResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The node id.
    pub node_id: i32,
    /// The host name.
    pub host: String,
    /// The port.
    pub port: i32,
    /// Each coordinator result in the response
    pub coordinators: Vec<Coordinator>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for FindCoordinatorResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version >= 1 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        if version <= 3 {
            Int16.encode(buf, self.error_code)?;
        }
        if (1..=3).contains(&version) {
            NullableString(version >= 3).encode(buf, self.error_message.as_deref())?;
        }
        if version <= 3 {
            Int32.encode(buf, self.node_id)?;
        }
        if version <= 3 {
            NullableString(version >= 3).encode(buf, self.host.as_str())?;
        }
        if version <= 3 {
            Int32.encode(buf, self.port)?;
        }
        if version >= 4 {
            NullableArray(Struct(version), true).encode(buf, self.coordinators.as_slice())?;
        }
        if version >= 3 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        if version >= 1 {
            res += Int32::SIZE; // self.throttle_time_ms
        }
        if version <= 3 {
            res += Int16::SIZE; // self.error_code
        }
        if (1..=3).contains(&version) {
            res += NullableString(version >= 3).calculate_size(self.error_message.as_deref());
        }
        if version <= 3 {
            res += Int32::SIZE; // self.node_id
        }
        if version <= 3 {
            res += NullableString(version >= 3).calculate_size(self.host.as_str());
        }
        if version <= 3 {
            res += Int32::SIZE; // self.port
        }
        if version >= 4 {
            res +=
                NullableArray(Struct(version), true).calculate_size(self.coordinators.as_slice());
        }
        if version >= 3 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct Coordinator {
    /// The coordinator key.
    pub key: String,
    /// The node id.
    pub node_id: i32,
    /// The host name.
    pub host: String,
    /// The port.
    pub port: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for Coordinator {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version > 4 {
            Err(err_encode_message_unsupported(version, "Coordinator"))?
        }
        NullableString(true).encode(buf, self.key.as_str())?;
        Int32.encode(buf, self.node_id)?;
        NullableString(true).encode(buf, self.host.as_str())?;
        Int32.encode(buf, self.port)?;
        Int16.encode(buf, self.error_code)?;
        NullableString(true).encode(buf, self.error_message.as_deref())?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }

    fn calculate_size(&self, _version: i16) -> usize {
        let mut res = 0;
        res += NullableString(true).calculate_size(self.key.as_str());
        res += Int32::SIZE; // self.node_id
        res += NullableString(true).calculate_size(self.host.as_str());
        res += Int32::SIZE; // self.port
        res += Int16::SIZE; // self.error_code
        res += NullableString(true).calculate_size(self.error_message.as_deref());
        res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        res
    }
}
