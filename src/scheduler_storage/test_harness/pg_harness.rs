//! One Postgres container per process, a migrated template database, and a fresh per-case database
//! cloned from it.
//!
//! `PostgresStorage::connect`/`migrate` `block_on` their own runtime, and nested `block_on` panics —
//! so container ops and admin SQL run inside `shared().rt.block_on(...)`, while connect/migrate run
//! from plain sync context.

use std::sync::{Mutex, OnceLock};

use dtor::dtor;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::Mount;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

use crate::scheduler_storage::postgres::{PostgresStorage, explain};

/// Process-wide state: the container lives until process exit, `rt` drives all admin SQL.
struct Shared {
    /// `SHARED` is a never-dropped static, so `ContainerAsync`'s own `Drop` reaping never fires —
    /// `reap_container_at_exit` `rm()`s it at process exit instead. `Mutex<Option<…>>` lets the reaper
    /// take the owned container (`rm()` consumes it) out of the shared static.
    container: Mutex<Option<ContainerAsync<Postgres>>>,
    rt: tokio::runtime::Runtime,
    admin_url: String,
    template: String,
}

static SHARED: OnceLock<Shared> = OnceLock::new();

/// `rm()` the shared container at process exit — the never-dropped static means its `Drop` won't.
/// Signal termination bypasses exit hooks; the `watchdog` feature covers that.
///
/// `#[dtor]`'s `unsafe` only acknowledges its run-after-`main` contract; the body is FFI-free.
#[dtor(unsafe)]
fn reap_container_at_exit() {
    let Some(shared) = SHARED.get() else { return };
    let container = shared.container.lock().ok().and_then(|mut g| g.take());
    if let Some(container) = container {
        if explain::enabled() {
            // Leave it running so its postgres log survives. `mem::forget` skips `ContainerAsync`'s
            // `Drop` reaping.
            eprintln!("{}", explain::left_running_notice(container.id()));
            std::mem::forget(container);
            return;
        }
        // On a fresh thread, not this one: a `#[dtor]` runs after `main`, where the main thread's TLS
        // is gone and `block_on` panics — and a panic in a `#[dtor]` aborts. `rm()` still needs the
        // runtime, so drive it on a spawned thread.
        std::thread::scope(|s| {
            s.spawn(|| {
                let _ = shared.rt.block_on(container.rm());
            });
        });
    }
}

/// PGDATA storage backing for the harness container.
#[derive(Clone, Copy)]
pub enum PgData {
    /// On a tmpfs of `size` (e.g. `"2g"`) — fast, for small per-case DBs.
    Tmpfs { size: &'static str },
    /// On the container's disk — for large DBs that overflow a tmpfs (the mainnet-scale
    /// reshuffle-sim).
    Disk,
}

/// Backing the test suite uses: tiny per-case DBs in RAM.
#[cfg(test)]
const TEST_PGDATA: PgData = PgData::Tmpfs { size: "2g" };

/// The process-wide container, built on first call with `pgdata` (the first call wins).
fn shared(pgdata: PgData) -> &'static Shared {
    SHARED.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build harness runtime");

        let (container, admin_url) = rt.block_on(async {
            // Throwaway cluster — skip WAL flushes (a big speedup for the many tiny transactions), and
            // back dynamic shared memory with mmap'd PGDATA files instead of `/dev/shm`, so
            // parallel-query DSM segments over the full placement can't overflow the container's fixed
            // tmpfs `/dev/shm` (hot pages stay in the page cache, so mmap ≈ posix when RAM is free).
            let mut cmd: Vec<String> = [
                "postgres",
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
                "-c",
                "full_page_writes=off",
                "-c",
                "dynamic_shared_memory_type=mmap",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect();
            if explain::enabled() {
                for &setting in explain::SESSION_SETTINGS {
                    cmd.push("-c".to_string());
                    cmd.push(setting.to_string());
                }
            }
            // Tag pinned for speed/determinism.
            let image = Postgres::default().with_tag("18.4-alpine").with_cmd(cmd);
            let image = match pgdata {
                // postgres:18 keeps PGDATA under /var/lib/postgresql/<ver>; tmpfs puts it in RAM.
                PgData::Tmpfs { size } => {
                    image.with_mount(Mount::tmpfs_mount("/var/lib/postgresql").with_size(size))
                }
                PgData::Disk => image,
            };
            let container = image.start().await.expect("start postgres container");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("postgres host port");
            // The module's default user/password/db are all `postgres`.
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            (container, url)
        });

        let template = "sim_template".to_string();

        rt.block_on(admin_exec(
            &admin_url,
            &format!("CREATE DATABASE {template}"),
        ));

        // Migrate outside `block_on` (nested-runtime rule). Dropping `storage` releases the advisory
        // lock and closes the connection — Postgres won't clone a DB that has open connections.
        let template_url = url_with_db(&admin_url, &template);
        let mut storage = PostgresStorage::connect(&template_url).expect("connect template");
        storage.migrate().expect("migrate template");
        drop(storage);

        Shared {
            container: Mutex::new(Some(container)),
            rt,
            admin_url,
            template,
        }
    })
}

/// Execute one statement on a fresh admin connection. Used only for
/// `CREATE DATABASE`, which cannot run inside a transaction/pool.
async fn admin_exec(admin_url: &str, sql: &str) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(admin_url)
        .await
        .expect("admin connect");
    // In-crate admin DDL (CREATE DATABASE), never user input — hence `AssertSqlSafe`.
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut conn)
        .await
        .expect("admin exec");
}

/// `url` with its database segment swapped to `db`. Assumes a
/// `postgres://user:pass@host:port/<db>` shape with no query string.
fn url_with_db(url: &str, db: &str) -> String {
    let base = url.rsplit_once('/').expect("url has a db segment").0;
    format!("{base}/{db}")
}

/// A fresh database cloned from the migrated template, on the default test backing. `prefix`+`id`
/// must be unique within the process (a duplicate `CREATE DATABASE` panics).
#[cfg(test)]
pub(crate) fn fresh_db(prefix: &str, id: u64) -> PostgresStorage {
    fresh_db_with(TEST_PGDATA, prefix, id)
}

/// [`fresh_db`] with an explicit container backing, for callers whose DB won't fit the test tmpfs —
/// the mainnet-scale reshuffle-sim passes [`PgData::Disk`]. The first call in a process fixes the
/// backing for the shared container; later calls reuse it.
pub fn fresh_db_with(pgdata: PgData, prefix: &str, id: u64) -> PostgresStorage {
    let s = shared(pgdata);
    let name = format!("{prefix}_{id}");
    s.rt.block_on(admin_exec(
        &s.admin_url,
        &format!("CREATE DATABASE {name} TEMPLATE {}", s.template),
    ));
    PostgresStorage::connect(&url_with_db(&s.admin_url, &name)).expect("connect fresh db")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler_storage::test_harness::inspect::StorageInspect;

    #[test]
    fn harness_connects_migrates_and_reads_empty() {
        let storage = fresh_db("harness_smoke", 1);
        assert!(storage.get_chunks(|_| true).is_empty());
        assert!(storage.get_workers(|_| true).is_empty());
    }
}
