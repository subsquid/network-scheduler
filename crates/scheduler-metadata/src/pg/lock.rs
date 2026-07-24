//! Per-dataset advisory lock serialising the API chunk inserts. Without it, concurrent writers at
//! READ COMMITTED can't see each other's uncommitted chunks: both probe clean, and the overlap is
//! only caught later by the async admission gate (one insert silently rejected). The loser now
//! blocks until the winner commits, then loses the probe (409).
//!
//! The scheduler's discovery insert does NOT take this lock — deferred dual-write follow-up;
//! planned shape: `with_dataset_lock` per batch, bounding how long a bulk load holds it.

use anyhow::Context;
use sqlx::postgres::PgConnection;
use sqlx::{Postgres, Transaction};

use crate::error::{IngestError, StorageError};
use crate::ids::DatasetPk;

/// `'SDLK'` = `0x53444C4B`. Namespaces every per-dataset ingest lock. Two-key advisory locks
/// (`pg_locks.objsubid = 2`) occupy a key space disjoint from the scheduler's single-key session
/// lock (`objsubid = 1`, `pg_try_advisory_lock(hashtext(...))`), so the two can never collide.
const DATASET_LOCK_CLASS: i32 = 0x5344_4C4B;

/// A healthy critical section is milliseconds; a wait this long means a wedged holder — fail
/// retryable instead of queueing the pool. Well under the HTTP layer's 30s request timeout.
const LOCK_TIMEOUT: &str = "10s";

/// The locked section as a value: the locked transaction's connection (exclusive reborrow — the
/// tx can't be used or committed behind it) plus the locked dataset. Constructed only by
/// `with_dataset_lock`; the overlap probe requires it, so probing unlocked, another dataset, or
/// outside the locked tx is unrepresentable.
pub struct LockedDataset<'a> {
    conn: &'a mut PgConnection,
    dataset_id: DatasetPk,
}

impl LockedDataset<'_> {
    pub(crate) fn dataset_id(&self) -> DatasetPk {
        self.dataset_id
    }

    /// The locked transaction's connection, for the section's own statements (e.g. the insert).
    pub(crate) fn conn(&mut self) -> &mut PgConnection {
        self.conn
    }
}

/// Run `f` as `dataset_id`'s locked write section, then commit — the lock's release — on `Ok`;
/// any error rolls back. Consuming the tx makes "the section ends at commit" structural.
///
/// Call it last so the lock spans only `f` + commit. Transaction-scoped, so every abort path
/// (error, panic, cancellation) releases it with the rollback. A waiter gives up after
/// `LOCK_TIMEOUT` (55P03 → retryable [`StorageError::LockTimeout`]).
pub(crate) async fn with_dataset_lock<T>(
    mut tx: Transaction<'_, Postgres>,
    dataset_id: DatasetPk,
    op: &'static str,
    f: impl AsyncFnOnce(&mut LockedDataset<'_>) -> Result<T, IngestError>,
) -> Result<T, IngestError> {
    lock_dataset(&mut tx, dataset_id).await?;
    let mut locked = LockedDataset {
        conn: &mut tx,
        dataset_id,
    };
    let out = f(&mut locked).await?;
    tx.commit().await.with_context(|| format!("{op}: commit"))?;
    Ok(out)
}

/// `SET LOCAL lock_timeout`, then `pg_advisory_xact_lock`. Production enters only through
/// `with_dataset_lock`; `pub` solely for the concurrency integration tests.
pub async fn lock_dataset(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetPk,
) -> Result<(), StorageError> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"
    )))
    .execute(&mut **tx)
    .await
    .context("lock_dataset: set timeout")?;
    // DatasetPk is i16 (SMALLINT); widen to the int4 the two-key lock function takes.
    let locked = sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(DATASET_LOCK_CLASS)
        .bind(i32::from(dataset_id.0))
        .execute(&mut **tx)
        .await;
    match locked {
        Ok(_) => Ok(()),
        Err(e) if super::rows::is_lock_timeout(&e) => Err(StorageError::LockTimeout),
        Err(e) => Err(anyhow::Error::new(e).context("lock_dataset").into()),
    }
}
