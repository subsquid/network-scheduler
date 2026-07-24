//! One Postgres container per test binary (never-dropped static, reaped by the `watchdog` feature);
//! a fresh migrated database per test; router mounted and driven via tower oneshot.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use secrecy::SecretString;
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, postgres::PgPoolOptions};
use tokio::sync::OnceCell;

use metadata_service::config::{IngesterConfig, TokensConfig};
use metadata_service::{AppState, TokenStore, build_router};
use scheduler_metadata::PgIngest;
use scheduler_metadata::pg::MIGRATOR;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};

pub const ADMIN_TOKEN: &str = "admin-secret";
pub const INGESTER_TOKEN: &str = "ingester-secret";

/// Ingester scope: `ds-missing` is in scope but never created, so a request to it reaches
/// DatasetNotFound (404) instead of stopping at 403 — this is what distinguishes 404 from 403.
fn ingester_scope() -> std::collections::BTreeSet<String> {
    ["ds-a", "ds-missing"]
        .into_iter()
        .map(String::from)
        .collect()
}

struct SharedPg {
    _container: ContainerAsync<Postgres>, // never dropped; watchdog reaps at process exit
    admin_url: String,
}
static PG: OnceCell<SharedPg> = OnceCell::const_new();

async fn shared_pg() -> &'static SharedPg {
    PG.get_or_init(|| async {
        let container = Postgres::default()
            .with_tag("18.4-alpine")
            .start()
            .await
            .expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.expect("host port");
        let admin_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        SharedPg {
            _container: container,
            admin_url,
        }
    })
    .await
}

fn url_with_db(url: &str, db: &str) -> String {
    let base = url.rsplit_once('/').expect("url has a db segment").0;
    format!("{base}/{db}")
}

/// Fresh migrated database; unique name per call. Small pools so parallel tests don't exhaust PG.
async fn fresh_db() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let pg = shared_pg().await;
    let name = format!("md_{}", N.fetch_add(1, Ordering::Relaxed));

    let mut admin = PgConnection::connect(&pg.admin_url)
        .await
        .expect("admin connect");
    // In-crate DDL, never user input.
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
        .execute(&mut admin)
        .await
        .expect("create db");

    let url = url_with_db(&pg.admin_url, &name);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    MIGRATOR.run(&pool).await.expect("migrate");
    pool.close().await;
    url
}

pub struct TestApp {
    pub router: Router,
}

pub async fn test_app() -> TestApp {
    let db_url = fresh_db().await;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("pool");
    let ingest = PgIngest::new(pool);
    let tokens = TokenStore::from_config(TokensConfig {
        admins: vec![SecretString::from(ADMIN_TOKEN.to_string())],
        ingesters: vec![IngesterConfig {
            token: SecretString::from(INGESTER_TOKEN.to_string()),
            datasets: ingester_scope(),
        }],
    })
    .expect("valid test token config");
    let state = AppState::new(ingest, tokens);
    TestApp {
        router: build_router(state, 20 * 1024 * 1024, std::time::Duration::from_secs(30)),
    }
}

// ---- request/response helpers ----
pub fn request(method: &str, uri: &str, token: Option<&str>, body: Option<Value>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    match body {
        Some(v) => b
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => b.body(Body::empty()).unwrap(),
    }
}

pub async fn send(app: &TestApp, req: Request<Body>) -> Response<Body> {
    use tower::ServiceExt; // oneshot
    app.router.clone().oneshot(req).await.unwrap()
}

/// `send(app, request(...))` in one call — the shape every integration-test site uses.
pub async fn call(
    app: &TestApp,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> Response<Body> {
    send(app, request(method, uri, token, body)).await
}

pub async fn body_json(resp: Response<Body>) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

pub async fn body_text(resp: Response<Body>) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ---- input builders ----
pub fn schema_json(tables: &[(&str, &[&str])]) -> Value {
    let mut t = serde_json::Map::new();
    for (name, fields) in tables {
        t.insert(
            (*name).to_string(),
            json!({ "fields": fields, "default_fields": [] }),
        );
    }
    json!({ "tables": t })
}

pub fn chunk_json(id: &str, first: u64, last: u64, schema_id: i32) -> Value {
    json!({ "id": id, "first_block": first, "last_block": last, "size": 1, "schema_id": schema_id })
}

// ---- flow helpers ----
pub async fn create_dataset(
    app: &TestApp,
    name: &str,
    tables: &[(&str, &[&str])],
) -> Response<Body> {
    call(
        app,
        "POST",
        "/datasets",
        Some(ADMIN_TOKEN),
        Some(json!({ "location": format!("s3://{name}"), "schema": schema_json(tables) })),
    )
    .await
}

/// Register a WRITE schema as the (scoped) ingester; returns the write schema id.
pub async fn register_write_schema(app: &TestApp, name: &str, tables: &[(&str, &[&str])]) -> i32 {
    let resp = call(
        app,
        "POST",
        &format!("/datasets/{name}/write-schemas"),
        Some(INGESTER_TOKEN),
        Some(schema_json(tables)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    body_json(resp).await["schema_id"].as_i64().unwrap() as i32
}

/// Promote a READ schema as admin; returns the raw response for status assertions.
pub async fn promote_read_schema(
    app: &TestApp,
    name: &str,
    tables: &[(&str, &[&str])],
    token: &str,
) -> Response<Body> {
    call(
        app,
        "PUT",
        &format!("/datasets/{name}/read-schema"),
        Some(token),
        Some(schema_json(tables)),
    )
    .await
}

/// Create `name` (seeds write+read schema) and return its write schema id.
pub async fn setup(app: &TestApp, name: &str, tables: &[(&str, &[&str])]) -> i32 {
    assert!(
        create_dataset(app, name, tables)
            .await
            .status()
            .is_success()
    );
    register_write_schema(app, name, tables).await
}
