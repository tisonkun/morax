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

// Starting in version 1, on quota violation, brokers send out responses before throttling.
//
// Version 2 is the first flexible version.
//
// Version 3 is the same as version 2.
//
// Version 4 adds the support for new error code PRODUCER_FENCED.

#[derive(Debug, Default, Clone)]
pub struct InitProducerIdResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The current producer id.
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    pub producer_epoch: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for InitProducerIdResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        Int32.encode(buf, self.throttle_time_ms)?;
        Int16.encode(buf, self.error_code)?;
        Int64.encode(buf, self.producer_id)?;
        Int16.encode(buf, self.producer_epoch)?;
        if version >= 2 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int32::SIZE; // self.throttle_time_ms
        res += Int16::SIZE; // self.error_code
        res += Int64::SIZE; // self.producer_id
        res += Int16::SIZE; // self.producer_epoch
        if version >= 2 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}
