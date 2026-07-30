//! One Postgres container per test binary, a migrated template database, and a fresh per-case
//! database cloned from it.
//!
//! Its own crate, not a feature of `scheduler-metadata`: Cargo unifies features across workspace
//! members, so a feature there reaches every crate that depends on it.
//!
//! It hands out URLs, not connections — the scheduler wants one locked connection, the metadata
//! service wants pools. [`CaseDb`] is separate from the URL so each can own the database with
//! whatever value outlives its connections.

use std::sync::{Mutex, OnceLock};

use anyhow::Context;
use dtor::dtor;
use sqlx::Connection;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::Mount;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

use scheduler_metadata::explain;

/// Run `f` on a thread inside no runtime, and block until it finishes.
///
/// `Runtime::block_on` panics inside a runtime, and the metadata-service suite is `#[tokio::test]`.
/// A panic in `f` is resumed here, so the caller sees the harness's message rather than the fact
/// that a thread panicked.
fn off_runtime<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    match std::thread::scope(|s| s.spawn(f).join()) {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

struct Shared {
    /// `Option` so the reaper can take the container out: `rm()` consumes it, and this static is
    /// never dropped.
    container: Mutex<Option<ContainerAsync<Postgres>>>,
    rt: tokio::runtime::Runtime,
    admin_url: String,
    template: String,
}

static SHARED: OnceLock<Shared> = OnceLock::new();

/// `rm()` the container at exit — the never-dropped static means its own `Drop` won't. Signal
/// termination bypasses exit hooks; the `watchdog` feature covers that.
///
/// `#[dtor]`'s `unsafe` only acknowledges its run-after-`main` contract; the body is FFI-free.
#[dtor(unsafe)]
fn reap_container_at_exit() {
    let Some(shared) = SHARED.get() else { return };
    let container = shared.container.lock().ok().and_then(|mut g| g.take());
    if let Some(container) = container {
        if explain::enabled() {
            // Leave it up so its log survives; `mem::forget` skips the `Drop` reaping.
            eprintln!("{}", explain::left_running_notice(container.id()));
            std::mem::forget(container);
            return;
        }
        // A `#[dtor]` runs after `main`, where this thread's TLS is gone and `block_on` panics —
        // and a panic here aborts.
        std::thread::scope(|s| {
            s.spawn(|| {
                let _ = shared.rt.block_on(container.rm());
            });
        });
    }
}

/// PGDATA storage backing for the harness container.
#[derive(Clone, Copy, Debug)]
pub enum PgData {
    /// On a tmpfs of `size` (e.g. `"2g"`) — fast, for small per-case DBs.
    Tmpfs { size: &'static str },
    /// On the container's disk, for DBs that overflow a tmpfs (the mainnet-scale reshuffle-sim).
    Disk,
}

/// The process-wide container, built on first call with `pgdata` (the first call wins).
fn shared(pgdata: PgData) -> &'static Shared {
    SHARED.get_or_init(|| off_runtime(|| build_shared(pgdata)))
}

/// Off the caller's thread: starting the container and migrating the template both need a runtime,
/// and the caller may already be in one.
fn build_shared(pgdata: PgData) -> Shared {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build harness runtime");

    let (container, admin_url) = rt.block_on(async {
        // Throwaway cluster, so skip WAL flushes. `dynamic_shared_memory_type=mmap` puts
        // parallel-query segments in PGDATA instead of the container's small `/dev/shm`.
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
    let template_url = url_with_db(&admin_url, &template);

    rt.block_on(async {
        admin_exec(&admin_url, &format!("CREATE DATABASE {template}")).await;
        let mut conn = sqlx::PgConnection::connect(&template_url)
            .await
            .expect("connect template");
        scheduler_metadata::pg::MIGRATOR
            .run(&mut conn)
            .await
            .expect("migrate template");
        // Explicitly: Postgres won't clone a database that still has a connection.
        conn.close().await.expect("close template connection");
    });

    Shared {
        container: Mutex::new(Some(container)),
        rt,
        admin_url,
        template,
    }
}

/// One statement on a fresh admin connection — `CREATE`/`DROP DATABASE` cannot run in a transaction
/// or a pool.
async fn admin_exec(admin_url: &str, sql: &str) {
    admin_try_exec(admin_url, sql)
        .await
        .unwrap_or_else(|err| panic!("{err:#}"));
}

/// Non-panicking [`admin_exec`], for [`CaseDb::drop`].
async fn admin_try_exec(admin_url: &str, sql: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(admin_url)
        .await
        .context("admin connect")?;
    // In-crate DDL, never user input — hence `AssertSqlSafe`.
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut conn)
        .await
        .with_context(|| format!("admin exec: {sql}"))?;
    Ok(())
}

/// A case's database, dropped with this guard, so PGDATA holds the cases running at once rather
/// than every case the process has run. Without it the suite filled its tmpfs mid-run and took the
/// cluster down, failing every case after that on connect.
///
/// Not `Clone`: a second guard would drop the database under a case still using it. Bind it —
/// `let (url, _db) = …`; `let (url, _)` drops it immediately.
#[derive(Debug)]
#[must_use = "the database is dropped with this guard; bind it for as long as the URL is used"]
pub struct CaseDb {
    name: String,
}

impl Drop for CaseDb {
    fn drop(&mut self) {
        // Keep the data whenever the container is kept.
        if explain::enabled() {
            return;
        }
        let Some(shared) = SHARED.get() else { return };
        // FORCE: sqlx closes the socket without waiting for the backend, and Postgres refuses to
        // drop a database that still has one.
        let sql = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        // Never panic: the case may already be unwinding, and panicking in a `Drop` then aborts.
        // Hence joining by hand instead of `off_runtime`, which resumes the panic.
        let dropped = std::thread::scope(|s| {
            s.spawn(|| shared.rt.block_on(admin_try_exec(&shared.admin_url, &sql)))
                .join()
        });
        match dropped {
            Ok(Ok(())) => {}
            Ok(Err(err)) => eprintln!("pg_harness: could not drop {}: {err:#}", self.name),
            Err(_) => eprintln!("pg_harness: the thread dropping {} panicked", self.name),
        }
    }
}

/// `url` with its database segment swapped to `db`.
fn url_with_db(url: &str, db: &str) -> String {
    let base = url.rsplit_once('/').expect("url has a db segment").0;
    format!("{base}/{db}")
}

/// URL of a fresh migrated database, plus the [`CaseDb`] that owns it — hold the guard for as long
/// as the URL is used. The first call in a process fixes `pgdata`.
///
/// The URL is a plain `String` because callers clone it into spawned writers, and a cloned guard
/// would drop the database under them. Declare the guard before the pool that uses it (locals drop
/// in reverse) or last in a struct (fields drop in order).
///
/// # Panics
///
/// If the container cannot start, or if `prefix`+`id` is not unique within the process.
pub fn fresh_db_url(pgdata: PgData, prefix: &str, id: u64) -> (String, CaseDb) {
    let s = shared(pgdata);
    let name = format!("{prefix}_{id}");
    let create = format!("CREATE DATABASE {name} TEMPLATE {}", s.template);
    off_runtime(|| s.rt.block_on(admin_exec(&s.admin_url, &create)));
    (url_with_db(&s.admin_url, &name), CaseDb { name })
}
