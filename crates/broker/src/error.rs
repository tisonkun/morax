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

use std::borrow::Borrow;

use morax_protos::request::ErrorCode;
use morax_protos::request::ErrorResponse;
use poem::http::StatusCode;
use poem::IntoResponse;

#[derive(Debug, Clone)]
pub(crate) struct ErrorWithCode {
    inner: ErrorResponse,
}

impl ErrorWithCode {
    pub fn with_fallback_status<T, E>(code: ErrorCode) -> impl FnOnce(T) -> ErrorWithCode
    where
        T: Borrow<error_stack::Report<E>>,
    {
        move |err| {
            let err = err.borrow();
            let message = format!("{err:?}");
            let code = err.downcast_ref::<ErrorCode>().cloned().unwrap_or(code);
            ErrorWithCode {
                inner: ErrorResponse { code, message },
            }
        }
    }
}

impl IntoResponse for ErrorWithCode {
    fn into_response(self) -> poem::Response {
        let status = match self.inner.code {
            ErrorCode::Unexpected => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body =
            serde_json::to_string_pretty(&self.inner).expect("error response is always serialize");

        poem::Response::builder()
            .status(status)
            .content_type(mime::APPLICATION_JSON)
            .body(body)
    }
}

impl From<ErrorWithCode> for poem::Error {
    fn from(value: ErrorWithCode) -> Self {
        poem::Error::from_response(value.into_response())
    }
}
