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

#[derive(Debug, Default, Clone)]
pub struct Record {
    pub len: i32, // varint
    /// bit 0~7: unused
    pub attributes: i8,
    pub timestamp_delta: i64, // varlong
    pub offset_delta: i32,    // varint
    pub key_len: i32,         // varint
    pub key: Option<Vec<u8>>,
    pub value_len: i32, // varint
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Default, Clone)]
pub struct Header {
    pub key_len: i32, // varint
    pub key: Option<Vec<u8>>,
    pub value_len: i32, // varint
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum CompressionType {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl From<u8> for CompressionType {
    fn from(ty: u8) -> Self {
        match ty {
            0 => CompressionType::None,
            1 => CompressionType::Gzip,
            2 => CompressionType::Snappy,
            3 => CompressionType::Lz4,
            4 => CompressionType::Zstd,
            _ => unreachable!("unknown compression type id: {}", ty),
        }
    }
}
