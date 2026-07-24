//! DB-backed integration cases: routing, auth, and request/response serialization. The per-error
//! status mapping is covered exhaustively by the `error` unit test, and the ingest semantics by the
//! `scheduler-metadata` PgIngest tests, so these exercise the HTTP wiring, not that logic again.

mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::json;

#[tokio::test]
async fn create_dataset_create_then_idempotent() {
    let app = test_app().await;
    let resp = create_dataset(&app, "ds-a", &[("blocks", &["number"])]).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = body_json(resp).await;
    assert_eq!(body["name"], "ds-a");
    assert_eq!(body["created"], true);

    // Re-creating the same dataset is a 200 with created=false.
    let resp = create_dataset(&app, "ds-a", &[("blocks", &["number"])]).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["created"], false);
}

/// Dataset-naming invariant, enforced in the create handler: location must carry a scheme, name
/// must not.
#[tokio::test]
async fn create_dataset_enforces_scheme_on_location_not_name() {
    let app = test_app().await;
    let schema = schema_json(&[("blocks", &["number"])]);

    // A schemeless location → 400.
    let resp = call(
        &app,
        "POST",
        "/datasets",
        Some(ADMIN_TOKEN),
        Some(json!({ "location": "base-sepolia", "schema": schema.clone() })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(resp).await["error"], "invalid_payload");

    // An explicit name carrying a scheme → 400.
    let resp = call(
        &app,
        "POST",
        "/datasets",
        Some(ADMIN_TOKEN),
        Some(json!({ "location": "s3://base-sepolia", "name": "s3://base-sepolia", "schema": schema.clone() })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(resp).await["error"], "invalid_payload");

    // Canonical: scheme on the location, name derived scheme-free → created.
    let resp = call(
        &app,
        "POST",
        "/datasets",
        Some(ADMIN_TOKEN),
        Some(json!({ "location": "s3://base-sepolia", "schema": schema })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert_eq!(body_json(resp).await["name"], "base-sepolia");
}

#[tokio::test]
async fn register_write_schema_returns_id_and_is_idempotent() {
    let app = test_app().await;
    assert!(
        create_dataset(&app, "ds-a", &[("blocks", &["number"])])
            .await
            .status()
            .is_success()
    );
    // create seeded write schema id 1; re-registering identical content dedups to it.
    let a = register_write_schema(&app, "ds-a", &[("blocks", &["number"])]).await;
    assert!(a > 0);
    let b = register_write_schema(&app, "ds-a", &[("blocks", &["number"])]).await;
    assert_eq!(a, b);
}

#[tokio::test]
async fn get_read_schema_and_write_schemas() {
    let app = test_app().await;
    let s = setup(&app, "ds-a", &[("blocks", &["number"])]).await;

    // GET read-schema returns the current read schema (ingester-scoped).
    let resp = call(
        &app,
        "GET",
        "/datasets/ds-a/read-schema",
        Some(INGESTER_TOKEN),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body["read_schema_id"].as_i64().unwrap() > 0);
    assert!(body["schema"]["tables"]["blocks"].is_object());

    // GET write-schemas lists the write registry with active flags (no `current` field anymore).
    let resp = call(
        &app,
        "GET",
        "/datasets/ds-a/write-schemas",
        Some(INGESTER_TOKEN),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let schemas = body_json(resp).await["schemas"].as_array().unwrap().clone();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0]["schema_id"], s);
    assert!(schemas[0].get("current").is_none());
    assert_eq!(schemas[0]["active"], false); // no chunks pin it yet

    // GET one write schema by id.
    let resp = call(
        &app,
        "GET",
        &format!("/datasets/ds-a/write-schemas/{s}"),
        Some(INGESTER_TOKEN),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["schema_id"], s);

    // A non-integer id renders as our JSON ErrorBody (400), not axum's plaintext 400.
    let resp = call(
        &app,
        "GET",
        "/datasets/ds-a/write-schemas/not-an-int",
        Some(INGESTER_TOKEN),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(resp).await["error"], "invalid_payload");
}

/// Read-schema promotion is admin-only: an ingester token is 403, the admin token succeeds.
#[tokio::test]
async fn read_schema_promotion_requires_admin() {
    let app = test_app().await;
    let _ = setup(&app, "ds-a", &[("blocks", &["number"])]).await;

    // Ingester cannot promote a read schema.
    let resp = promote_read_schema(&app, "ds-a", &[("blocks", &["number"])], INGESTER_TOKEN).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Admin can — widening the read schema (adds a field) is a compatible superset.
    let resp = promote_read_schema(
        &app,
        "ds-a",
        &[("blocks", &["number", "hash"])],
        ADMIN_TOKEN,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_json(resp).await["read_schema_id"].as_i64().unwrap() > 0);
}

/// A write schema can name fields the current read schema doesn't expose — v1 has no compatibility
/// gate, and those fields are hidden at read time (the read is a pure projection).
#[tokio::test]
async fn register_write_schema_with_extra_fields_succeeds() {
    let app = test_app().await;
    let _ = setup(&app, "ds-a", &[("blocks", &["number"])]).await;
    // Read schema only has `blocks.number`; a write adding `hash` is accepted (hash is hidden).
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-a/write-schemas",
        Some(INGESTER_TOKEN),
        Some(schema_json(&[("blocks", &["number", "hash"])])),
    )
    .await;
    assert!(resp.status().is_success());
    assert!(body_json(resp).await["schema_id"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn post_chunks_happy_path() {
    let app = test_app().await;
    let s = setup(&app, "ds-a", &[("blocks", &["number"])]).await;
    let batch = json!({ "chunks": [chunk_json("c1", 0, 99, s), chunk_json("c2", 100, 199, s)] });
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-a/chunks",
        Some(INGESTER_TOKEN),
        Some(batch),
    )
    .await;
    // Full success inserts everything: 204 No Content, no body.
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

/// One end-to-end error case: a POST /chunks failure renders as its mapped status with the
/// structured JSON body (the full variant→status table is the `error` unit test's job).
#[tokio::test]
async fn repost_same_batch_returns_409_with_duplicate_ids() {
    let app = test_app().await;
    let s = setup(&app, "ds-a", &[("blocks", &["number"])]).await;
    let batch = json!({ "chunks": [chunk_json("c1", 0, 99, s), chunk_json("c2", 100, 199, s)] });
    assert_eq!(
        send(
            &app,
            request(
                "POST",
                "/datasets/ds-a/chunks",
                Some(INGESTER_TOKEN),
                Some(batch.clone())
            )
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-a/chunks",
        Some(INGESTER_TOKEN),
        Some(batch),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = body_json(resp).await;
    assert_eq!(body["error"], "duplicate_chunks");
    assert_eq!(body["duplicates"], json!(["c1", "c2"]));
}

#[tokio::test]
async fn correction_unknown_old_chunk_returns_409() {
    let app = test_app().await;
    let s = setup(&app, "ds-a", &[("blocks", &["number"])]).await;
    let body = json!({ "corrections": [{
        "old_chunk_id": "old",
        "replacement": chunk_json("new", 0, 99, s)
    }] });
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-a/corrections",
        Some(INGESTER_TOKEN),
        Some(body),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = body_json(resp).await;
    assert_eq!(body["error"], "correction_rejected");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("unknown old chunk id")
    );
}

#[tokio::test]
async fn head_reflects_pending_chunks() {
    let app = test_app().await;
    let s = setup(&app, "ds-a", &[("blocks", &["number"])]).await;
    let batch = json!({ "chunks": [chunk_json("c1", 0, 99, s), chunk_json("c2", 100, 199, s)] });
    call(
        &app,
        "POST",
        "/datasets/ds-a/chunks",
        Some(INGESTER_TOKEN),
        Some(batch),
    )
    .await;
    let resp = send(
        &app,
        request("GET", "/datasets/ds-a/head", Some(INGESTER_TOKEN), None),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["stored_head"], 199);
}

#[tokio::test]
async fn status_pending_for_stored_chunk() {
    let app = test_app().await;
    let s = setup(&app, "ds-a", &[("blocks", &["number"])]).await;
    call(
        &app,
        "POST",
        "/datasets/ds-a/chunks",
        Some(INGESTER_TOKEN),
        Some(json!({ "chunks": [chunk_json("c1", 0, 99, s)] })),
    )
    .await;
    let resp = call(
        &app,
        "GET",
        "/datasets/ds-a/chunks/c1/status",
        Some(INGESTER_TOKEN),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["status"], "pending");
}

// ---- auth ----

#[tokio::test]
async fn no_token_returns_401() {
    let app = test_app().await;
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-a/chunks",
        None,
        Some(json!({ "chunks": [] })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(resp.headers().get("www-authenticate").unwrap(), "Bearer");
    assert_eq!(body_json(resp).await["error"], "unauthorized");
}

#[tokio::test]
async fn unknown_token_returns_401() {
    let app = test_app().await;
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-a/chunks",
        Some("garbage"),
        Some(json!({ "chunks": [] })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn ingester_out_of_scope_returns_403() {
    let app = test_app().await;
    assert!(
        create_dataset(&app, "ds-b", &[("blocks", &["number"])])
            .await
            .status()
            .is_success()
    );
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-b/chunks",
        Some(INGESTER_TOKEN),
        Some(json!({ "chunks": [] })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(resp).await["error"], "forbidden");
}

#[tokio::test]
async fn ingester_cannot_post_datasets_returns_403() {
    let app = test_app().await;
    let resp = call(
        &app,
        "POST",
        "/datasets",
        Some(INGESTER_TOKEN),
        Some(json!({ "location": "ds-a", "schema": schema_json(&[("blocks", &["number"])]) })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn admin_cannot_ingest_returns_403() {
    let app = test_app().await;
    let resp = call(
        &app,
        "POST",
        "/datasets/ds-a/chunks",
        Some(ADMIN_TOKEN),
        Some(json!({ "chunks": [] })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

/// Authorization runs before any DB access, so an in-scope-but-nonexistent dataset must reach the
/// handler and 404 — not be masked as a 403.
#[tokio::test]
async fn dataset_not_found_returns_404_not_403() {
    let app = test_app().await;
    let resp = call(
        &app,
        "GET",
        "/datasets/ds-missing/head",
        Some(INGESTER_TOKEN),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert_eq!(body["error"], "dataset_not_found");
    assert_eq!(body["dataset"], "ds-missing");
}

// ---- open routes ----

#[tokio::test]
async fn health_needs_no_token() {
    let app = test_app().await;
    let resp = call(&app, "GET", "/health", None, None).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_needs_no_token() {
    let app = test_app().await;
    let _ = call(&app, "GET", "/health", None, None).await; // creates a counter series
    let resp = call(&app, "GET", "/metrics", None, None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_text(resp).await.contains("http_requests_total"));
}

/// Observability: error codes, domain-mutation counters, and request latency surface on /metrics.
#[tokio::test]
async fn metrics_expose_error_codes_business_counters_and_latency() {
    let app = test_app().await;
    let _ = setup(&app, "ds-a", &[("blocks", &["number"])]).await;

    // An unauthenticated request is a 401, tagged with the error code "unauthorized".
    let resp = call(&app, "GET", "/datasets/ds-a/read-schema", None, None).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    // A domain mutation bumps a business counter.
    let _ = register_write_schema(&app, "ds-a", &[("blocks", &["number"])]).await;

    let m = body_text(call(&app, "GET", "/metrics", None, None).await).await;
    assert!(
        m.contains(r#"error_responses_total{code="unauthorized"}"#),
        "error_responses missing the unauthorized code:\n{m}"
    );
    assert!(
        m.contains("write_schemas_registered_total"),
        "business counter missing:\n{m}"
    );
    assert!(
        m.contains("http_request_duration_seconds"),
        "latency histogram missing:\n{m}"
    );
}
