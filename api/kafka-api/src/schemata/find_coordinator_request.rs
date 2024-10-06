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

// Version 1 adds KeyType.
//
// Version 2 is the same as version 1.
//
// Version 3 is the first flexible version.
//
// Version 4 adds support for batching via CoordinatorKeys (KIP-699)

#[derive(Debug, Default, Clone)]
pub struct FindCoordinatorRequest {
    /// The coordinator key.
    pub key: String,
    /// The coordinator key type. (Group, transaction, etc.)
    pub key_type: i8,
    /// The coordinator keys.
    pub coordinator_keys: Vec<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for FindCoordinatorRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = FindCoordinatorRequest::default();
        if version <= 3 {
            this.key = NullableString(version >= 3)
                .decode(buf)?
                .unwrap_or_default();
        }
        if version >= 1 {
            this.key_type = Int8.decode(buf)?;
        }
        if version >= 4 {
            this.coordinator_keys = NullableArray(NullableString(true), true)
                .decode(buf)?
                .unwrap_or_default()
                .into_iter()
                .map(|key| key.ok_or_else(|| err_decode_message_null("coordinatorKeys element")))
                .collect::<std::io::Result<Vec<String>>>()?;
        }
        if version >= 3 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
