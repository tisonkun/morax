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
use std::time::Duration;

use error_stack::Result;
use error_stack::ResultExt;
use latches::task::Latch;
use morax_meta::PostgresMetaService;
use morax_protos::config::ServerConfig;
use morax_runtime::wait_group::WaitGroup;
use poem::listener::Acceptor;
use poem::listener::Listener;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ServerError(String);

type ServerFuture<T> = morax_runtime::JoinHandle<Result<T, ServerError>>;

#[derive(Debug)]
pub struct ServerState {
    kafka_broker_addr: SocketAddr,
    kafka_broker_fut: ServerFuture<()>,
    wal_broker_addr: SocketAddr,
    wal_broker_fut: ServerFuture<()>,
    shutdown: Arc<Latch>,
}

impl ServerState {
    pub fn kafka_broker_addr(&self) -> SocketAddr {
        self.kafka_broker_addr
    }

    pub fn wal_broker_addr(&self) -> SocketAddr {
        self.wal_broker_addr
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

        match futures::future::try_join_all(vec![
            flatten(self.kafka_broker_fut),
            flatten(self.wal_broker_fut),
        ])
        .await
        {
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

    // initialize kafka broker
    let kafka_broker_addr = config.kafka_broker.addr;
    let kafka_broker_listener = tokio::net::TcpListener::bind(kafka_broker_addr)
        .await
        .change_context_lazy(|| {
            ServerError(format!(
                "failed to listen to kafka broker: {kafka_broker_addr}"
            ))
        })?;
    let kafka_broker_addr = kafka_broker_listener.local_addr().change_context_lazy(|| {
        ServerError("failed to get local address of kafka broker".to_string())
    })?;
    let kafka_broker_fut = {
        let fut = morax_kafka_broker::start_broker(
            meta_service.clone(),
            kafka_broker_listener,
            config.kafka_broker,
            wg.clone(),
            shutdown.clone(),
        );

        morax_runtime::api_runtime().spawn(async move { fut.await.change_context_lazy(make_error) })
    };

    // initialize wal broker
    let wal_broker_addr = config.wal_broker.addr;
    let wal_broker_acceptor = poem::listener::TcpListener::bind(&wal_broker_addr)
        .into_acceptor()
        .await
        .change_context_lazy(|| {
            ServerError(format!("failed to listen to wal broker: {wal_broker_addr}"))
        })?;
    let wal_broker_addr = wal_broker_acceptor.local_addr()[0]
        .as_socket_addr()
        .cloned()
        .ok_or_else(|| ServerError("failed to get local address of wal broker".to_string()))?;
    let wal_broker_fut = {
        let shutdown_clone = shutdown.clone();
        let wg_clone = wg.clone();

        let route = morax_wal_broker::make_api_router(meta_service.clone());
        let signal = async move {
            log::info!("WAL Broker has started on [{wal_broker_addr}]");
            drop(wg_clone);

            shutdown_clone.wait().await;
            log::info!("WAL Broker is closing");
        };
        morax_runtime::api_runtime().spawn(async move {
            poem::Server::new_with_acceptor(wal_broker_acceptor)
                .run_with_graceful_shutdown(route, signal, Some(Duration::from_secs(30)))
                .await
                .change_context_lazy(make_error)
        })
    };

    // wait all servers to start and return
    wg.await;
    Ok(ServerState {
        kafka_broker_addr,
        kafka_broker_fut,
        wal_broker_addr,
        wal_broker_fut,
        shutdown,
    })
}

async fn flatten<T>(fut: ServerFuture<T>) -> Result<T, ServerError> {
    let make_error = || ServerError("failed to join server future".to_string());
    fut.await.change_context_lazy(make_error)?
}
