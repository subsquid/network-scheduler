//! Test harness: one shared Postgres container per test binary, a migrated
//! template database, and a fresh per-case database cloned from it.
//!
//! Runtime-nesting rule: `PostgresStorage::connect`/`migrate` `block_on` their
//! own runtime, and nested `block_on` panics — so container ops and admin SQL
//! run inside `shared().rt.block_on(...)` while connect/migrate are called from
//! plain sync context.

use std::sync::{Mutex, OnceLock};

use dtor::dtor;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::Mount;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

use crate::scheduler_storage::postgres::PostgresStorage;

/// Process-wide state: the container lives until process exit, `rt` drives all admin SQL.
struct Shared {
    /// Held only to keep the container running for the whole test binary. `SHARED` is a
    /// never-dropped static, so `ContainerAsync`'s own `Drop`-based reaping never fires —
    /// `reap_container_at_exit` takes the container out and `rm()`s it on process exit
    /// instead. `Mutex<Option<…>>` because `rm()` consumes the owned container.
    container: Mutex<Option<ContainerAsync<Postgres>>>,
    rt: tokio::runtime::Runtime,
    admin_url: String,
    template: String,
}

static SHARED: OnceLock<Shared> = OnceLock::new();

/// Remove the shared container at process exit — it lives in a never-dropped static, so
/// `ContainerAsync`'s own `Drop`-based reaping never fires and it would otherwise leak.
/// `rm()` goes through testcontainers' docker client (same daemon, honors `DOCKER_HOST`).
/// Signal termination (e.g. Ctrl-C) bypasses exit hooks — covered by the `watchdog` feature.
///
/// `#[dtor]`'s `unsafe` marker only acknowledges its run-after-`main` contract; the body is FFI-free.
#[dtor(unsafe)]
fn reap_container_at_exit() {
    let Some(shared) = SHARED.get() else { return };
    let container = shared.container.lock().ok().and_then(|mut g| g.take());
    if let Some(container) = container {
        // Remove on a spawned thread, not the main one: this `#[dtor]` runs after `main`,
        // when the main thread's TLS is gone, and tokio's `block_on` panics there (it parks
        // via `std::thread::current()`) — a panic in a `#[dtor]` aborts. `rm()` needs the
        // tokio reactor, so it must still run inside the runtime, just on a fresh thread.
        std::thread::scope(|s| {
            s.spawn(|| {
                let _ = shared.rt.block_on(container.rm());
            });
        });
    }
}

fn shared() -> &'static Shared {
    SHARED.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build harness runtime");

        let (container, admin_url) = rt.block_on(async {
            let container = Postgres::default()
                // The image is pre-pulled; the tag is pinned for speed/determinism.
                .with_tag("18.4-alpine")
                // Durability is pointless for a throwaway cluster; skipping WAL flushes is a
                // large speedup for the sim's many tiny transactions.
                .with_cmd([
                    "postgres",
                    "-c",
                    "fsync=off",
                    "-c",
                    "synchronous_commit=off",
                    "-c",
                    "full_page_writes=off",
                ])
                // Keep the cluster (postgres:18 puts PGDATA under /var/lib/postgresql/<ver>)
                // in memory: per-case databases are tiny and shrink replays clone many.
                .with_mount(Mount::tmpfs_mount("/var/lib/postgresql").with_size("2g"))
                .start()
                .await
                .expect("start postgres container");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("postgres host port");
            // The module defaults to user/password/db all `postgres`.
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            (container, url)
        });

        let template = "sim_template".to_string();

        rt.block_on(admin_exec(
            &admin_url,
            &format!("CREATE DATABASE {template}"),
        ));

        // Migrate OUTSIDE block_on (see module docs). Drop releases the advisory lock and
        // leaves the template connection-free — Postgres refuses to clone a DB with open conns.
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
    // Test-only admin DDL (e.g. CREATE DATABASE), not user input. sqlx 0.9 gates dynamic SQL.
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

/// A fresh database cloned from the migrated template. The caller must ensure
/// `prefix`+`id` is unique within the test binary (a duplicate `CREATE DATABASE` panics).
pub(crate) fn fresh_db(prefix: &str, id: u64) -> PostgresStorage {
    let s = shared();
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
