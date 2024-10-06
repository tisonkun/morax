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

use serde::Deserialize;
use serde::Serialize;

use crate::property::TopicProps;

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
    pub data: Vec<u8>,
    pub entry_cnt: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendLogResponse {
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadLogRequest {
    pub name: String,
    pub offset: i64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReadLogResponse {
    pub data: Vec<u8>,
}

// TODO(tisonkun): adopts bytes debug fmt or find other robust ways
impl std::fmt::Debug for ReadLogResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadLogResponse")
            .field("data", &String::from_utf8_lossy(&self.data))
            .finish()
    }
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
