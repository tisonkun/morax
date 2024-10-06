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

// Version 1 is the first flexible version.

#[derive(Debug, Default, Clone)]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    pub correlation_id: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for ResponseHeader {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        Int32.encode(buf, self.correlation_id)?;
        if version >= 1 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int32::SIZE; // self.correlation_id
        if version >= 1 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}
