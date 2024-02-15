# Postgres

The postgres package provides a `compatibility` layer between `lib/pq` and `jackq/pgx`.

The motivation of bridging `lib/pq` and `jackq/pgx` is because we are using `lib/pq` at the begining of our project. While migrating to `pgx` we found many things are not compatible with `database/sql`. To make things consistent on our end we build the `compatibility` layer.

This package provide some helper functions taken and modified from https://github.com/golang/pkgsite/tree/master/internal/database.
