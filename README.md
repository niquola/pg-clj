# cljp

[![Build Status](https://travis-ci.org/niquola/pg-clj.svg?branch=master)](https://travis-ci.org/niquola/pg-clj)

Clojure-native async non-jdbc driver for postgresql - no middleware abstractions

* async
* support all datatypes
* data dsl for queries (aka honeysql)
* logical replication / notifications
* netty
* jsonb as first class

# ( '/' )


## Dev

```sh

docker run --name pg10 -p 5555:5432 -e POSTGRES_PASSWORD=pass -d postgres:10

lein repl

```


## Usage

## License

Copyright © 2017 niquola

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
