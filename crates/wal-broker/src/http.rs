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

use std::sync::Arc;

use morax_meta::PostgresMetaService;
use morax_protos::rpc::AppendLogRequest;
use morax_protos::rpc::AppendLogResponse;
use morax_protos::rpc::CreateLogRequest;
use morax_protos::rpc::CreateLogResponse;
use morax_protos::rpc::ErrorCode;
use morax_protos::rpc::ReadLogRequest;
use morax_protos::rpc::ReadLogResponse;
use poem::middleware::AddData;
use poem::middleware::Compression;
use poem::web::Data;
use poem::web::Json;
use poem::EndpointExt;
use poem::Route;

use crate::error::ErrorWithCode;
use crate::wal::WALBroker;

#[poem::handler]
pub async fn health_check() -> poem::Result<String> {
    Ok("OK".to_string())
}

#[poem::handler]
pub async fn create(
    Data(wal): Data<&WALBroker>,
    Json(request): Json<CreateLogRequest>,
) -> poem::Result<Json<CreateLogResponse>> {
    let response = wal
        .create(request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to create log"))
        .map_err(ErrorWithCode::with_fallback_status(ErrorCode::Unexpected))?;
    Ok(Json(response))
}

#[poem::handler]
pub async fn read(
    Data(wal): Data<&WALBroker>,
    Json(request): Json<ReadLogRequest>,
) -> poem::Result<Json<ReadLogResponse>> {
    let response = wal
        .read_at(request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to read log"))
        .map_err(ErrorWithCode::with_fallback_status(ErrorCode::Unexpected))?;
    Ok(Json(response))
}

#[poem::handler]
pub async fn append(
    Data(wal): Data<&WALBroker>,
    Json(request): Json<AppendLogRequest>,
) -> poem::Result<Json<AppendLogResponse>> {
    let response = wal
        .append(request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to append log"))
        .map_err(ErrorWithCode::with_fallback_status(ErrorCode::Unexpected))?;
    Ok(Json(response))
}

pub fn make_api_router(meta: Arc<PostgresMetaService>) -> Route {
    let wal = WALBroker::new(meta);

    let v1_route = Route::new()
        .at("/health", poem::get(health_check))
        .at("/create", poem::post(create))
        .at("/read", poem::post(read))
        .at("/append", poem::post(append))
        .with(Compression::new())
        .with(AddData::new(wal));

    Route::new().nest("v1", v1_route)
}
