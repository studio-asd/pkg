# Postgres

The postgres package provides a `compatibility` layer between `lib/pq` and `jackq/pgx`.

The motivation of bridging `lib/pq` and `jackq/pgx` is because we are using `lib/pq` at the begining of our project. While migrating to `pgx` we found many things are not compatible with `database/sql`. To make things consistent on our end we build the `compatibility` layer.

This package provide some helper functions taken and modified from https://github.com/golang/pkgsite/tree/master/internal/database.

## Disclaimer

This package doesn't guarantee full backwards compatibility between `stdlib` and `jackq/pgx` because `pgx` use a lot of custom error inside the package itself. For example, `sql.ErrNoRows` is `pgx.ErrNoRows` in `pgx`. We tried to convert this back in some cases, but not all.

## Testing

The package provide several opinionated helper functions with the aim to make testing easier. We use `testing.Testing()` to protect the helper function to be used outside of `*_test.go`.
