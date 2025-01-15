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

use std::net::SocketAddr;
use std::sync::Arc;

use error_stack::Result;
use error_stack::ResultExt;
use mea::latch::Latch;
use mea::waitgroup::WaitGroup;
use morax_meta::PostgresMetaService;
use morax_protos::config::ServerConfig;

use crate::broker::bootstrap_broker;
use crate::broker::BrokerBootstrapContext;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ServerError(pub(crate) String);

pub(crate) type ServerFuture<T> = morax_runtime::JoinHandle<Result<T, ServerError>>;

#[derive(Debug)]
pub struct ServerState {
    broker_advertise_addr: SocketAddr,
    broker_fut: ServerFuture<()>,
    shutdown: Arc<Latch>,
}

impl ServerState {
    pub fn broker_advertise_addr(&self) -> SocketAddr {
        self.broker_advertise_addr
    }

    pub fn shutdown_handle(&self) -> impl Fn() {
        let shutdown = self.shutdown.clone();
        move || shutdown.count_down()
    }

    pub fn shutdown(&self) {
        self.shutdown_handle()();
    }

    pub async fn await_shutdown(self) {
        self.shutdown.wait().await;

        match futures::future::try_join_all(vec![flatten(self.broker_fut)]).await {
            Ok(_) => log::info!("Morax server stopped."),
            Err(err) => log::error!(err:?; "Morax server failed."),
        }
    }
}

pub async fn start(config: ServerConfig) -> Result<ServerState, ServerError> {
    let make_error = || ServerError("failed to start server".to_string());
    let shutdown = Arc::new(Latch::new(1));
    let wg = WaitGroup::new();

    // initialize meta service
    let meta_service = PostgresMetaService::new(&config.meta)
        .await
        .map(Arc::new)
        .change_context_lazy(make_error)?;

    // initialize broker
    let (broker_advertise_addr, broker_fut) = bootstrap_broker(BrokerBootstrapContext {
        config: config.broker,
        meta_service: meta_service.clone(),
        wg: wg.clone(),
        shutdown: shutdown.clone(),
    })
    .await?;

    // wait all servers to start and return
    wg.await;
    Ok(ServerState {
        broker_advertise_addr,
        broker_fut,
        shutdown,
    })
}

pub(crate) fn resolve_advertise_addr(
    listen_addr: SocketAddr,
    advertise_addr: Option<&str>,
) -> Result<SocketAddr, ServerError> {
    let make_error = || ServerError("failed to resolve advertise address".to_string());

    match advertise_addr {
        None => {
            if listen_addr.ip().is_unspecified() {
                let ip = local_ip_address::local_ip().change_context_lazy(make_error)?;
                let port = listen_addr.port();
                Ok(SocketAddr::new(ip, port))
            } else {
                Ok(listen_addr)
            }
        }
        Some(advertise_addr) => {
            let advertise_addr = advertise_addr
                .parse::<SocketAddr>()
                .change_context_lazy(make_error)?;
            assert!(
                advertise_addr.ip().is_global(),
                "ip = {}",
                advertise_addr.ip()
            );
            Ok(advertise_addr)
        }
    }
}

async fn flatten<T>(fut: ServerFuture<T>) -> Result<T, ServerError> {
    let make_error = || ServerError("failed to join server future".to_string());
    fut.await.change_context_lazy(make_error)?
}
