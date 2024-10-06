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

use std::fmt;
use std::fmt::Display;
use std::io;

use crate::codec::err_codec_message;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ApiMessageType {
    api_key: i16,
    lowest_supported_version: i16,
    highest_supported_version: i16,
    latest_version_unstable: bool,
}

impl ApiMessageType {
    const fn new(
        api_key: i16,
        lowest_supported_version: i16,
        highest_supported_version: i16,
        latest_version_unstable: bool,
    ) -> Self {
        Self {
            api_key,
            lowest_supported_version,
            highest_supported_version,
            latest_version_unstable,
        }
    }

    pub fn api_key(&self) -> i16 {
        self.api_key
    }

    pub fn lowest_supported_version(&self) -> i16 {
        self.lowest_supported_version
    }

    pub fn highest_supported_version(&self) -> i16 {
        self.highest_supported_version
    }

    pub fn latest_version_unstable(&self) -> bool {
        self.latest_version_unstable
    }
}

impl ApiMessageType {
    pub const PRODUCE: Self = ApiMessageType::new(0, 0, 9, false);
    pub const FETCH: Self = ApiMessageType::new(1, 0, 15, false);
    pub const METADATA: Self = ApiMessageType::new(3, 0, 12, false);
    pub const OFFSET_FETCH: Self = ApiMessageType::new(9, 0, 8, false);
    pub const FIND_COORDINATOR: Self = ApiMessageType::new(10, 0, 4, false);
    pub const JOIN_GROUP: Self = ApiMessageType::new(11, 0, 9, false);
    pub const HEARTBEAT: Self = ApiMessageType::new(12, 0, 4, false);
    pub const SYNC_GROUP: Self = ApiMessageType::new(14, 0, 5, false);
    pub const API_VERSIONS: Self = ApiMessageType::new(18, 0, 3, false);
    pub const CREATE_TOPICS: Self = ApiMessageType::new(19, 0, 7, false);
    pub const INIT_PRODUCER_ID: Self = ApiMessageType::new(22, 0, 4, false);
}

impl TryFrom<i16> for ApiMessageType {
    type Error = io::Error;

    fn try_from(api_key: i16) -> Result<Self, Self::Error> {
        match api_key {
            0 => Ok(ApiMessageType::PRODUCE),
            1 => Ok(ApiMessageType::FETCH),
            3 => Ok(ApiMessageType::METADATA),
            9 => Ok(ApiMessageType::OFFSET_FETCH),
            10 => Ok(ApiMessageType::FIND_COORDINATOR),
            11 => Ok(ApiMessageType::JOIN_GROUP),
            12 => Ok(ApiMessageType::HEARTBEAT),
            14 => Ok(ApiMessageType::SYNC_GROUP),
            18 => Ok(ApiMessageType::API_VERSIONS),
            19 => Ok(ApiMessageType::CREATE_TOPICS),
            22 => Ok(ApiMessageType::INIT_PRODUCER_ID),
            _ => Err(err_codec_message(format!("unknown api key {api_key}"))),
        }
    }
}

impl Display for ApiMessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                ApiMessageType::PRODUCE => "Produce",
                ApiMessageType::FETCH => "Fetch",
                ApiMessageType::METADATA => "Metadata",
                ApiMessageType::OFFSET_FETCH => "OffsetFetch",
                ApiMessageType::FIND_COORDINATOR => "FindCoordinator",
                ApiMessageType::JOIN_GROUP => "JoinGroup",
                ApiMessageType::HEARTBEAT => "Heartbeat",
                ApiMessageType::SYNC_GROUP => "SyncGroup",
                ApiMessageType::API_VERSIONS => "ApiVersions",
                ApiMessageType::CREATE_TOPICS => "CreateTopics",
                ApiMessageType::INIT_PRODUCER_ID => "InitProducerId",
                api_key => unreachable!("unknown api key {:?}", api_key),
            }
        )
    }
}

impl ApiMessageType {
    pub fn request_header_version(&self, api_version: i16) -> i16 {
        // the current different is whether the request is flexible
        fn resolve_request_header_version(flexible: bool) -> i16 {
            if flexible {
                2
            } else {
                1
            }
        }

        match *self {
            ApiMessageType::PRODUCE => resolve_request_header_version(api_version >= 9),
            ApiMessageType::FETCH => resolve_request_header_version(api_version >= 12),
            ApiMessageType::METADATA => resolve_request_header_version(api_version >= 9),
            ApiMessageType::OFFSET_FETCH => resolve_request_header_version(api_version >= 6),
            ApiMessageType::FIND_COORDINATOR => resolve_request_header_version(api_version >= 3),
            ApiMessageType::JOIN_GROUP => resolve_request_header_version(api_version >= 6),
            ApiMessageType::HEARTBEAT => resolve_request_header_version(api_version >= 4),
            ApiMessageType::SYNC_GROUP => resolve_request_header_version(api_version >= 4),
            ApiMessageType::API_VERSIONS => resolve_request_header_version(api_version >= 3),
            ApiMessageType::CREATE_TOPICS => resolve_request_header_version(api_version >= 5),
            ApiMessageType::INIT_PRODUCER_ID => resolve_request_header_version(api_version >= 2),
            _ => unreachable!("unknown api type {}", self.api_key),
        }
    }

    pub fn response_header_version(&self, api_version: i16) -> i16 {
        // the current different is whether the response is flexible
        fn resolve_response_header_version(flexible: bool) -> i16 {
            if flexible {
                1
            } else {
                0
            }
        }

        match *self {
            ApiMessageType::PRODUCE => resolve_response_header_version(api_version >= 9),
            ApiMessageType::FETCH => resolve_response_header_version(api_version >= 12),
            ApiMessageType::METADATA => resolve_response_header_version(api_version >= 9),
            ApiMessageType::OFFSET_FETCH => resolve_response_header_version(api_version >= 6),
            ApiMessageType::FIND_COORDINATOR => resolve_response_header_version(api_version >= 3),
            ApiMessageType::JOIN_GROUP => resolve_response_header_version(api_version >= 6),
            ApiMessageType::HEARTBEAT => resolve_response_header_version(api_version >= 4),
            ApiMessageType::SYNC_GROUP => resolve_response_header_version(api_version >= 4),
            ApiMessageType::API_VERSIONS => {
                // ApiVersionsResponse always includes a v0 header.
                // @see KIP-511 https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers
                0
            }
            ApiMessageType::CREATE_TOPICS => resolve_response_header_version(api_version >= 5),
            ApiMessageType::INIT_PRODUCER_ID => resolve_response_header_version(api_version >= 2),
            _ => unreachable!("unknown api type {}", self.api_key),
        }
    }
}
