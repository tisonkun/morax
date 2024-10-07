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

use std::ops::Range;

use serde::Deserialize;
use serde::Serialize;

use crate::property::TopicProps;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// Index of this log entry, assigned by the broker when the message is published. Guaranteed
    /// to be unique within the topic. It must not be populated by the publisher in a write
    /// call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<i64>,
    /// A padded, base64-encoded string of bytes, encoded with a URL and filename safe alphabet
    /// (sometimes referred to as "web-safe" or "base64url"). Defined by [RFC4648].
    ///
    /// [RFC4648]: https://datatracker.ietf.org/doc/html/rfc4648
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLogRequest {
    pub name: String,
    pub properties: TopicProps,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLogResponse {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendLogRequest {
    pub name: String,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendLogResponse {
    /// The half-open offset range of the appended entry.
    pub offsets: Range<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadLogRequest {
    pub name: String,
    pub offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadLogResponse {
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    /// The broker does not know what happened here, and no actions other than just returning it
    /// back.
    Unexpected,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ErrorCode::Unexpected => write!(f, "unexpected"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub message: String,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} ({}): {}", self.code, self.code as u32, self.message)
    }
}
