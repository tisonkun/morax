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

use error_stack::Result;
use morax_api::property::StorageProperty;
use opendal::Operator;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{0}")]
    OpenDAL(opendal::Error),
}

#[derive(Debug)]
pub struct TopicStorage {
    storage: StorageProperty,
}

impl TopicStorage {
    pub fn new(storage: StorageProperty) -> Self {
        Self { storage }
    }

    pub async fn read_split(&self, topic_id: i64, split_id: Uuid) -> Result<Vec<u8>, StorageError> {
        let op = make_op(self.storage.clone())?;
        let split_url = make_split_url(topic_id, split_id);
        let records = op.read(&split_url).await.map_err(StorageError::OpenDAL)?;
        Ok(records.to_vec())
    }

    pub async fn write_split(&self, topic_id: i64, split: Vec<u8>) -> Result<Uuid, StorageError> {
        let op = make_op(self.storage.clone())?;
        let split_id = Uuid::new_v4();
        let split_url = make_split_url(topic_id, split_id);
        op.write(&split_url, split)
            .await
            .map_err(StorageError::OpenDAL)?;
        Ok(split_id)
    }
}

pub fn make_op(storage: StorageProperty) -> Result<Operator, StorageError> {
    match storage {
        StorageProperty::S3(config) => {
            let mut builder = opendal::services::S3::default()
                .bucket(&config.bucket)
                .region(&config.region)
                .root(&config.prefix)
                .endpoint(&config.endpoint)
                .access_key_id(&config.access_key_id)
                .secret_access_key(&config.secret_access_key);
            if config.virtual_host_style {
                builder = builder.enable_virtual_host_style();
            }
            let builder = Operator::new(builder).map_err(StorageError::OpenDAL)?;
            Ok(builder.finish())
        }
    }
}

fn make_split_url(topic_id: i64, split_id: Uuid) -> String {
    format!("topic_{topic_id}/{split_id}.split")
}
