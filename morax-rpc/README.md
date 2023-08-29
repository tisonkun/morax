# Network and Actor Model

This module contains an Actor Model based network and RPC implementation derived by Spark's implementations, from its "spark-network-common" module, and "rpc" package under the "core" module.

The baseline version is Spark 76a32d3df0fe8e857221662657936e5ae336d3ee. To simplify conveying licenses, all the commits after {placeholder} are made independently unless explict noted.

Besides porting the Scala RPC code into Java code, here are other significant modifications or notes:

* Shuffle-related logics are redundant. I didn't convey all the related methods, and we should finally remove all these dangling code.
* Although the abstractions are similar to Actor Model, we don't implement Supervisor and Discovery (Spark doesn't also).
  * Supervisor is good. But it is also complicated. We tend to use coarse-grained actors each of which represents a node or a module, so Supervisor is not quite necessary.
  * Discover is to be supported with global configuration center; perhaps ZooKeeper, FoundationDB, or a KRaft/Ratis based controller quorum.
* The difference between how Scala and Java handles `Throwable` and `Future` is obvious. We should gradually work out a unified style (idiom) and follow.
* Only a small set of RPC test suites are ported - we need to port more.

## Acknowledgement

This module is derived from [Apache Spark](https://github.com/apache/spark)'s related modules. See details above.
