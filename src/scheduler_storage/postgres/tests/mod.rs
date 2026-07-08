//! Postgres-backend tests. Kept a descendant of `postgres` so they retain access to its private
//! connection internals (`with_conn`) without widening that surface to the crate.

mod correction;
mod correction_tests;
mod inspect;
