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

use backon::BackoffBuilder;
use backon::Retryable;
use error_stack::ResultExt;
use morax_protos::request::AppendLogRequest;
use morax_protos::request::AppendLogResponse;
use morax_protos::request::CreateLogRequest;
use morax_protos::request::CreateLogResponse;
use morax_protos::request::ErrorResponse;
use morax_protos::request::ReadLogRequest;
use morax_protos::request::ReadLogResponse;
use reqwest::Client;
use reqwest::ClientBuilder;
use reqwest::Response;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ClientError(String);

#[derive(Debug, Clone)]
pub enum HTTPResponse<T> {
    Success(T),
    Failure(ErrorResponse),
    Error(ErrorStatus),
}

#[derive(Debug, Clone)]
pub struct ErrorStatus {
    code: StatusCode,
    payload: Vec<u8>,
}

impl std::fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?} ({}): {}",
            self.code.canonical_reason(),
            self.code.as_u16(),
            if self.payload.is_empty() {
                "(no payload)".into()
            } else {
                String::from_utf8_lossy(&self.payload)
            }
        )
    }
}

#[derive(Debug)]
pub struct HTTPClient {
    endpoint: String,
    client: Client,
}

impl HTTPClient {
    pub fn new(
        endpoint: impl Into<String>,
        builder: ClientBuilder,
    ) -> error_stack::Result<Self, ClientError> {
        let endpoint = endpoint.into();
        let make_error = || ClientError(format!("failed to create client: {endpoint:?}"));

        Ok(Self {
            endpoint: endpoint.clone(),
            client: builder.build().change_context_lazy(make_error)?,
        })
    }

    pub async fn health_check<B: BackoffBuilder>(
        &self,
        backoff: Option<B>,
    ) -> error_stack::Result<(), ClientError> {
        let url = format!("{}/v1/health", self.endpoint);
        let make_error = || ClientError(format!("failed to health check: {url:?}"));

        let health_check = || async {
            self.client
                .get(&url)
                .send()
                .await
                .and_then(Response::error_for_status)
        };

        if let Some(backoff) = backoff {
            health_check.retry(backoff).await
        } else {
            health_check().await
        }
        .change_context_lazy(make_error)?;

        Ok(())
    }

    pub async fn create_log(
        &self,
        request: CreateLogRequest,
    ) -> error_stack::Result<HTTPResponse<CreateLogResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to create log: {request:?}"));

        let response = self
            .client
            .post(format!("{}/v1/create", self.endpoint))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }

    pub async fn append_log(
        &self,
        request: AppendLogRequest,
    ) -> error_stack::Result<HTTPResponse<AppendLogResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to append log: {request:?}"));

        let response = self
            .client
            .post(format!("{}/v1/append", self.endpoint))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }

    pub async fn read_log(
        &self,
        request: ReadLogRequest,
    ) -> error_stack::Result<HTTPResponse<ReadLogResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to read log: {request:?}"));

        let response = self
            .client
            .post(format!("{}/v1/read", self.endpoint))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }
}

async fn make_response<T: DeserializeOwned>(
    r: Response,
) -> error_stack::Result<HTTPResponse<T>, ClientError> {
    let make_error = || ClientError("failed to make response".to_string());

    let status = r.status();

    if status.is_success() {
        let result = r.json().await.change_context_lazy(make_error)?;
        return Ok(HTTPResponse::Success(result));
    }

    let payload = r.bytes().await.change_context_lazy(make_error)?;
    if let Ok(resp) = serde_json::from_slice::<ErrorResponse>(&payload) {
        return Ok(HTTPResponse::Failure(resp));
    }

    Ok(HTTPResponse::Error(ErrorStatus {
        code: status,
        payload: payload.to_vec(),
    }))
}
