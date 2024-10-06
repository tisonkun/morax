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

// Version 1 is the same as version 0.
//
// Version 2 is the first flexible version.
//
// Version 3 adds ProducerId and ProducerEpoch, allowing producers to try to resume after an
// INVALID_PRODUCER_EPOCH error
//
// Version 4 adds the support for new error code PRODUCER_FENCED.

#[derive(Debug, Default, Clone)]
pub struct InitProducerIdRequest {
    /// The transactional id, or null if the producer is not transactional.
    pub transactional_id: Option<String>,
    /// The time in ms to wait before aborting idle transactions sent by this producer. This is
    /// only relevant if a TransactionalId has been defined.
    pub transaction_timeout_ms: i32,
    /// The producer id. This is used to disambiguate requests if a transactional id is reused
    /// following its expiration.
    pub producer_id: i64,
    /// The producer's current epoch. This will be checked against the producer epoch on the
    /// broker, and the request will return an error if they do not match.
    pub producer_epoch: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for InitProducerIdRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut res = InitProducerIdRequest {
            transactional_id: NullableString(version >= 3).decode(buf)?,
            transaction_timeout_ms: Int32.decode(buf)?,
            producer_id: if version >= 3 { Int64.decode(buf)? } else { -1 },
            producer_epoch: if version >= 3 { Int16.decode(buf)? } else { -1 },
            ..Default::default()
        };
        if version >= 2 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}
