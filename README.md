# Morax

[![Discord][discord-badge]][discord-url]
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[discord-badge]: https://img.shields.io/discord/1291345378246922363?logo=discord&label=discord
[discord-url]: https://discord.gg/RRxbfYGqHM
[license-badge]: https://img.shields.io/crates/l/morax-cli
[license-url]: LICENSE
[actions-badge]: https://github.com/tisonkun/morax/workflows/CI/badge.svg
[actions-url]:https://github.com/tisonkun/morax/actions?query=workflow%3ACI

Morax is aimed at providing message queue and data streaming functionality based on cloud native services:

* Meta service is backed by Postgres compatible relational database services (RDS, Aurora, etc.).
* Data storage is backed by S3 compatible object storage services (S3, MinIO, etc.).

## Usage

Currently, Morax supports basic PubSub APIs for Apache Kafka and WAL Service. You can try it out with the following steps.

1. Start the environment that provides a Postgres instance and a MinIO instance:

    ```shell
    docker compose -f ./dev/docker-compose.yml up
    ```

2. Build the `morax` binary:

    ```shell
    cargo x build
    ```

3. Start the broker:

   ```shell
    ./target/debug/morax start --config-file ./dev/config.toml
    ```

### Try out the Apache Kafka broker

Now, a Kafka broker is running at `localhost:9092`. You can use your favorite Kafka client to interact with it.

You can also get an impression of the interaction by reading the test cases in:

* [rdkafka-tests](tests/rdkafka/tests)
* [rskafka-tests](tests/rskafka/tests)

### Try out the WAL Service broker

Also, a WAL broker is running at `localhost:8848`. You can talk to it with the [`morax-wal-client`](api/wal-client). The wire protocol is HTTP so that all the HTTP ecosystem is ready for use.

You can also get an impression of the interaction by reading the test cases in:

* [wal-tests](tests/wal/tests)

## Design

To support multiple providing message queue and data streaming APIs, Morax is designed as a modular system:

* Common functionalities like logging, async runtime, and protos are shared;
* Interfaces of meta service and data storage are shared;
* Each protocol implements their own wire protocol and message format;
* Each protocol shares the basic topic metadata model, with optional additional specific properties;
* Each protocol shares the basic data storage model, the payload is protocol specific, with a common header;
* Thus, each protocol shares similar publishing/producing APIs;
* On the contrary, each protocol implements their own subscription and consumer group management.

## License

This project is licensed under [Apache License, Version 2.0](https://github.com/tisonkun/logforth/blob/main/LICENSE).
