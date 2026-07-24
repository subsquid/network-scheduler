//! Keeps `docs/metadata-service-api.md` and the router in lockstep with the `#[utoipa::path]` annotations.

use metadata_service::openapi::{ApiDoc, render_markdown};
use utoipa::OpenApi;

const DOC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/metadata-service-api.md");

/// `docs/metadata-service-api.md` must match what the annotations render. Bless with
/// `UPDATE_API_DOC=1 cargo test -p metadata-service --test api_doc`.
#[test]
fn api_markdown_is_current() {
    let rendered = render_markdown(&ApiDoc::openapi());
    if std::env::var("UPDATE_API_DOC").is_ok() {
        std::fs::write(DOC_PATH, &rendered).expect("write docs/metadata-service-api.md");
        return;
    }
    let committed = std::fs::read_to_string(DOC_PATH).unwrap_or_default();
    assert_eq!(
        committed, rendered,
        "docs/metadata-service-api.md is stale — regenerate with UPDATE_API_DOC=1 cargo test -p metadata-service --test api_doc"
    );
}

/// Every annotated (method, path) must exist in the real router: a request to it must not 404.
/// Auth rejects unauthenticated requests with 401 and `/health` may 503 on a dead pool — both
/// prove the route exists. Catches annotations drifting from `build_router`.
#[tokio::test]
async fn every_documented_route_exists() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let mut app = metadata_service::test_router_without_db();

    for (path, methods) in spec["paths"].as_object().unwrap() {
        for method in methods.as_object().unwrap().keys() {
            let url = path.replace("{name}", "ds").replace("{id}", "1");
            let req = Request::builder()
                .method(method.to_uppercase().as_str())
                .uri(&url)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();
            let status = (&mut app).oneshot(req).await.unwrap().status();
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "documented route {} {} is not in build_router",
                method.to_uppercase(),
                path
            );
        }
    }
}
