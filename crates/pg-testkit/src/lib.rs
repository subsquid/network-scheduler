//! One Postgres container per test binary, a migrated template database, and a fresh per-case
//! database cloned from it.
//!
//! Its own crate so every suite that tests against this schema shares one harness — the scheduler's
//! storage tests and simulations, the reshuffle-sim, and the metadata service. Each is a separate
//! process, so each gets its own container.
//!
//! A crate and not a feature of `scheduler-metadata`: Cargo unifies features across workspace
//! members built together, so a feature there would put a docker client in `metadata-service`, which
//! ships as an image. Nothing can unify a crate on.
//!
//! It migrates its template with this workspace's `MIGRATOR`, so it is this project's harness, not a
//! general-purpose one.
//!
//! It hands out URLs, not connections. The scheduler wants one `PgConnection` holding its advisory
//! lock; the metadata service wants several pools; a URL is all they have in common — and keeping
//! [`CaseDb`] separate from it lets each own the database with whatever value outlives its
//! connections.

use std::sync::{Mutex, OnceLock};

use anyhow::Context;
use dtor::dtor;
use sqlx::Connection;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::Mount;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

use scheduler_metadata::explain;

/// Run `f` on a thread that is inside no runtime, and block until it finishes.
///
/// Every entry point here has to work from both kinds of suite: the scheduler's are plain sync
/// functions, the metadata service's are `#[tokio::test]`. `Runtime::block_on` panics when called
/// from within a runtime, so the harness never runs its own `block_on` on the caller's thread.
///
/// A panic in `f` is resumed on the caller rather than unwrapped, so the failure a case actually
/// sees is the harness's own message — "admin exec: CREATE DATABASE …" — and not the fact that some
/// thread panicked.
fn off_runtime<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    match std::thread::scope(|s| s.spawn(f).join()) {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

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
#[derive(Clone, Copy, Debug)]
pub enum PgData {
    /// On a tmpfs of `size` (e.g. `"2g"`) — fast, for small per-case DBs.
    Tmpfs { size: &'static str },
    /// On the container's disk — for large DBs that overflow a tmpfs (the mainnet-scale
    /// reshuffle-sim).
    Disk,
}

/// The process-wide container, built on first call with `pgdata` (the first call wins).
fn shared(pgdata: PgData) -> &'static Shared {
    SHARED.get_or_init(|| off_runtime(|| build_shared(pgdata)))
}

/// Built on the harness thread: the container start and the template migration both need a
/// runtime, and this may be called from inside someone else's.
fn build_shared(pgdata: PgData) -> Shared {
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
        // Explicitly, not by drop: Postgres won't clone a database that still has a connection.
        conn.close().await.expect("close template connection");
    });

    Shared {
        container: Mutex::new(Some(container)),
        rt,
        admin_url,
        template,
    }
}

/// Execute one statement on a fresh admin connection. Used only for `CREATE`/`DROP DATABASE`,
/// which cannot run inside a transaction/pool.
async fn admin_exec(admin_url: &str, sql: &str) {
    admin_try_exec(admin_url, sql)
        .await
        .unwrap_or_else(|err| panic!("{err:#}"));
}

/// [`admin_exec`] for callers that must not panic — see [`CaseDb::drop`].
async fn admin_try_exec(admin_url: &str, sql: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(admin_url)
        .await
        .context("admin connect")?;
    // In-crate admin DDL, never user input — hence `AssertSqlSafe`.
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut conn)
        .await
        .with_context(|| format!("admin exec: {sql}"))?;
    Ok(())
}

/// A case's database, dropped when this guard is — so PGDATA holds the cases running at once, not
/// every case the process has run.
///
/// Without it the scheduler suite passed ~130 databases and filled its 2 GiB tmpfs mid-run. That
/// surfaces nowhere near its cause: the WAL write that hit `ENOSPC` panicked a backend, crash
/// recovery could not write WAL either, and the cluster went down — every case after it failed on
/// connect, none of them for a reason of its own.
///
/// Not `Clone`: a second guard would let the first drop pull the database out from under a case
/// still using it. Bind it to a named local — `let (url, _db) = …` — for as long as the URL is in
/// use; `let (url, _)` drops it there and then, taking the database with it.
#[derive(Debug)]
#[must_use = "the database is dropped with this guard; bind it for as long as the URL is used"]
pub struct CaseDb {
    name: String,
}

impl Drop for CaseDb {
    fn drop(&mut self) {
        // `explain` mode leaves the container up for post-mortem; keep the case's data too.
        if explain::enabled() {
            return;
        }
        let Some(shared) = SHARED.get() else { return };
        // FORCE: the case's connections close as its values drop, and sqlx closes a socket without
        // waiting for the backend to go — Postgres refuses to drop a database that still has one.
        let sql = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        // A failing case is already unwinding, and panicking in a `Drop` during unwind aborts the
        // process — that would replace the real failure with an abort. Report and move on; the
        // container's exit reclaims the space regardless.
        // Joined by hand rather than through `off_runtime`, which turns a panicking harness thread
        // back into a panic here.
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

/// `url` with its database segment swapped to `db`. Assumes a
/// `postgres://user:pass@host:port/<db>` shape with no query string.
fn url_with_db(url: &str, db: &str) -> String {
    let base = url.rsplit_once('/').expect("url has a db segment").0;
    format!("{base}/{db}")
}

/// URL of a fresh migrated database, cloned from the template, plus the [`CaseDb`] that owns it —
/// hold that for as long as anything uses the URL. The first call in a process fixes `pgdata` for
/// the shared container; later calls reuse it.
///
/// The URL is a plain `String` so callers can clone it into spawned writers; cloning a guard with
/// it would drop the database while they were still on it.
///
/// The guard has to outlive everything connected to the database, and the two drop orders pull in
/// opposite directions: locals drop in reverse declaration order, so declare it *before* the pool
/// or runtime that uses it; struct fields drop in declaration order, so declare it *last*.
///
/// # Panics
///
/// If the container or its template cannot be brought up, or if `prefix`+`id` is not unique within
/// the process — `CREATE DATABASE` of a name that already exists is an error, and one case reusing
/// another's database would not be a test worth running.
pub fn fresh_db_url(pgdata: PgData, prefix: &str, id: u64) -> (String, CaseDb) {
    let s = shared(pgdata);
    let name = format!("{prefix}_{id}");
    let create = format!("CREATE DATABASE {name} TEMPLATE {}", s.template);
    off_runtime(|| s.rt.block_on(admin_exec(&s.admin_url, &create)));
    (url_with_db(&s.admin_url, &name), CaseDb { name })
}
