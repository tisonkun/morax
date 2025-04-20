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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicProperty {
    pub storage: StorageProperty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "scheme")]
#[serde(deny_unknown_fields)]
pub enum StorageProperty {
    #[serde(rename = "s3")]
    S3(S3StorageProperty),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3StorageProperty {
    /// Bucket name.
    pub bucket: String,
    /// Region name.
    pub region: String,
    /// URL prefix of table files, e.g. `/path/to/my/dir/`.
    ///
    /// Default to `/`
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// URL of S3 endpoint, e.g. `https://s3.<region>.amazonaws.com`.
    pub endpoint: String,
    /// Access key ID.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Whether to enable virtual host style, so that API requests will be sent
    /// in virtual host style instead of path style.
    ///
    /// Default to `true`
    #[serde(default = "default_virtual_host_style")]
    pub virtual_host_style: bool,
}

impl fmt::Debug for S3StorageProperty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("prefix", &self.prefix)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

pub fn default_prefix() -> String {
    "/".to_string()
}

pub const fn default_virtual_host_style() -> bool {
    true
}
