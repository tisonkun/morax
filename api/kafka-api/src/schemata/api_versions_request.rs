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

// Versions 0 through 2 of ApiVersionsRequest are the same.
//
// Version 3 is the first flexible version and adds ClientSoftwareName and ClientSoftwareVersion.

#[derive(Debug, Default, Clone)]
pub struct ApiVersionsRequest {
    /// The name of the client.
    pub client_software_name: String,
    /// The version of the client.
    pub client_software_version: String,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for ApiVersionsRequest {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self> {
        let mut this = ApiVersionsRequest::default();
        if version >= 3 {
            this.client_software_name = NullableString(true)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("client_software_name"))?;
        }
        if version >= 3 {
            this.client_software_version = NullableString(true)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("client_software_version"))?;
        }
        if version >= 3 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
