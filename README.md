# Morax

[![Discord][discord-badge]][discord-url]
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[discord-badge]: https://img.shields.io/discord/1291345378246922363?logo=discord&label=discord
[discord-url]: https://discord.gg/RRxbfYGqHM
[license-badge]: https://img.shields.io/crates/l/morax
[license-url]: LICENSE
[actions-badge]: https://github.com/tisonkun/morax/workflows/CI/badge.svg
[actions-url]:https://github.com/tisonkun/morax/actions?query=workflow%3ACI

Morax is aimed at providing message queue and data streaming functionality based on cloud native services:

* Meta service is backed by Postgres compatible relational database services (RDS, Aurora, etc.).
* Data storage is backed by S3 compatible object storage services (S3, MinIO, etc.).

## Usage

Currently, Morax supports basic PubSub APIs. You can try it out with the following steps.

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


The broker is now running at `localhost:8848`. You can talk to it with the [`morax-client`](sdk/client). The wire protocol is HTTP so that all the HTTP ecosystem is ready for use.

You can also get an impression of the interaction by reading the test cases in:

* [behavior-tests](tests/behavior/tests)

## License

This project is licensed under [Apache License, Version 2.0](https://github.com/tisonkun/logforth/blob/main/LICENSE).
