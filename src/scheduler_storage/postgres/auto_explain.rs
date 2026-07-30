//! `auto_explain` for the scheduler's queries, behind the feature of the same name.
//!
//! The implementation is [`scheduler_metadata::auto_explain`], next to the settings the harness
//! passes at container start. This module is only the switch: without the feature `with_explain` is
//! `run` alone, so a default build carries no machinery and reads no environment on the path the
//! scheduling cycle takes.

#[cfg(feature = "auto_explain")]
pub(super) use scheduler_metadata::auto_explain::with_explain;

#[cfg(not(feature = "auto_explain"))]
pub(super) async fn with_explain<R>(
    conn: &mut sqlx::postgres::PgConnection,
    run: impl AsyncFnOnce(&mut sqlx::postgres::PgConnection) -> sqlx::Result<R>,
) -> sqlx::Result<R> {
    run(conn).await
}
