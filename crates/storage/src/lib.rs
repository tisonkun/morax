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
use morax_protos::property::StorageProps;
use opendal::Operator;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{0}")]
    OpenDAL(opendal::Error),
}

pub struct TopicStorage {
    storage: StorageProps,
}

impl TopicStorage {
    pub fn new(storage: StorageProps) -> Self {
        Self { storage }
    }

    pub async fn read_at(&self, topic_name: &str, split_id: &str) -> Result<Vec<u8>, StorageError> {
        let op = self.op()?;
        let split_url = format!("{topic_name}/{split_id}");
        let records = op.read(&split_url).await.map_err(StorageError::OpenDAL)?;
        Ok(records.to_vec())
    }

    pub async fn write_to(
        &self,
        topic_name: &str,
        records: Vec<u8>,
    ) -> Result<String, StorageError> {
        let op = self.op()?;
        // TODO(tisonkun): whether use a sequential number rather than a UUID
        let split_id = uuid::Uuid::new_v4();
        let split_url = format!("{topic_name}/{split_id}");
        op.write(&split_url, records)
            .await
            .map_err(StorageError::OpenDAL)?;
        Ok(split_id.to_string())
    }

    fn op(&self) -> Result<Operator, StorageError> {
        match self.storage.clone() {
            StorageProps::S3(config) => {
                let builder = Operator::from_config(config).map_err(StorageError::OpenDAL)?;
                Ok(builder.finish())
            }
        }
    }
}
