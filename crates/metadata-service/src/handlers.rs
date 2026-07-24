//! The eight route handlers. Auth extractor first (never parses a body for an unauthorized caller);
//! the body-consuming `ApiJson` is always last.

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use scheduler_metadata::DatasetOutcome;
use scheduler_metadata::{DatasetSchema, NewDataset, SchemaId};

use crate::AppState;
use crate::auth::{Actor, DatasetAuth};
use crate::dto::*;
use crate::error::{ApiJson, ServiceError};

fn validate_schema(schema: &DatasetSchema) -> Result<(), ServiceError> {
    schema
        .validate()
        .map_err(|e| ServiceError::Payload(format!("invalid schema: {e}")))
}

// Authorization is enforced by the route's auth layer (see `build_router`): `require_admin` here,
// `require_dataset_scope` for the `DatasetAuth` handlers below. Mutations emit a best-effort audit
// event on the `audit` tracing target, attributing the committed change to the caller's actor id.
pub async fn create_dataset(
    Extension(actor): Extension<Actor>,
    State(st): State<AppState>,
    ApiJson(req): ApiJson<CreateDatasetRequest>,
) -> Result<(StatusCode, Json<CreateDatasetResponse>), ServiceError> {
    validate_schema(&req.schema)?;
    let dataset: NewDataset = req.into();
    dataset
        .validate()
        .map_err(|e| ServiceError::Payload(e.to_string()))?;
    let name = dataset.name.clone();
    let outcome = st.ingest.create_dataset(dataset).await?;
    let created = matches!(outcome, DatasetOutcome::Created);
    if created {
        st.metrics.datasets_created.inc();
        tracing::info!(target: "audit", actor = %actor.0, action = "create_dataset", dataset = %name, "dataset created");
    }
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(CreateDatasetResponse { name, created })))
}

/// Register a WRITE schema (ingester-scoped). Gated against the dataset's current read schema.
pub async fn post_write_schema(
    ing: DatasetAuth,
    Extension(actor): Extension<Actor>,
    State(st): State<AppState>,
    ApiJson(schema): ApiJson<DatasetSchema>,
) -> Result<Json<WriteSchemaResponse>, ServiceError> {
    validate_schema(&schema)?;
    let id = st
        .ingest
        .register_write_schema(&ing.dataset, schema)
        .await?;
    st.metrics.write_schemas_registered.inc();
    tracing::info!(target: "audit", actor = %actor.0, action = "register_write_schema", dataset = %ing.dataset, schema_id = id.0, "write schema registered");
    Ok(Json(id.into()))
}

/// List the dataset's WRITE schemas with active flags (ingester-scoped).
pub async fn get_write_schemas(
    ing: DatasetAuth,
    State(st): State<AppState>,
) -> Result<Json<SchemaListResponse>, ServiceError> {
    let schemas = st
        .ingest
        .list_write_schemas(&ing.dataset)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(Json(SchemaListResponse { schemas }))
}

/// One WRITE schema by id (ingester-scoped).
pub async fn get_write_schema(
    ing: DatasetAuth,
    State(st): State<AppState>,
    Path((_, id)): Path<(String, String)>,
) -> Result<Json<WriteSchemaViewResponse>, ServiceError> {
    // Parse the id ourselves so a non-integer renders as our JSON ErrorBody, not axum's plaintext 400.
    let id: i32 = id
        .parse()
        .map_err(|_| ServiceError::Payload(format!("schema id {id:?} is not an integer")))?;
    let schema = st
        .ingest
        .get_write_schema(&ing.dataset, SchemaId(id))
        .await?;
    Ok(Json(WriteSchemaViewResponse {
        schema_id: id,
        schema,
    }))
}

/// Promote the dataset's current READ schema (ADMIN only). Gated against every live write schema.
pub async fn put_read_schema(
    Extension(actor): Extension<Actor>,
    State(st): State<AppState>,
    Path(name): Path<String>,
    ApiJson(schema): ApiJson<DatasetSchema>,
) -> Result<Json<ReadSchemaResponse>, ServiceError> {
    validate_schema(&schema)?;
    let id = st.ingest.promote_read_schema(&name, schema).await?;
    st.metrics.read_schemas_promoted.inc();
    tracing::info!(target: "audit", actor = %actor.0, action = "promote_read_schema", dataset = %name, read_schema_id = id.0, "read schema promoted");
    Ok(Json(id.into()))
}

/// The dataset's current READ schema (ingester-scoped).
pub async fn get_read_schema(
    ing: DatasetAuth,
    State(st): State<AppState>,
) -> Result<Json<CurrentReadSchemaResponse>, ServiceError> {
    let (id, schema) = st.ingest.current_read_schema(&ing.dataset).await?;
    Ok(Json(CurrentReadSchemaResponse {
        read_schema_id: id.0,
        schema,
    }))
}

pub async fn post_chunks(
    ing: DatasetAuth,
    Extension(actor): Extension<Actor>,
    State(st): State<AppState>,
    ApiJson(req): ApiJson<InsertChunksRequest>,
) -> Result<StatusCode, ServiceError> {
    let sent = req.chunks.len();
    let duplicates = st.ingest.insert_chunks(&ing.dataset, req.chunks).await?;
    let inserted = sent - duplicates.len();
    st.metrics.chunks_inserted.inc_by(inserted as u64);
    st.metrics.chunks_duplicate.inc_by(duplicates.len() as u64);
    // Log whenever chunks were committed — including the 409 partial-duplicate path below, where the
    // new chunks landed but the response reports the duplicates.
    if inserted > 0 {
        tracing::info!(target: "audit", actor = %actor.0, action = "insert_chunks", dataset = %ing.dataset, inserted, duplicates = duplicates.len(), "chunks inserted");
    }
    if duplicates.is_empty() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ServiceError::DuplicateChunks { duplicates })
    }
}

pub async fn post_corrections(
    ing: DatasetAuth,
    Extension(actor): Extension<Actor>,
    State(st): State<AppState>,
    ApiJson(req): ApiJson<CorrectionsRequest>,
) -> Result<Json<CorrectionsResponse>, ServiceError> {
    let corrected = req.corrections.len();
    let old_ids: Vec<String> = req
        .corrections
        .iter()
        .map(|c| c.old_chunk_id.clone())
        .collect();
    // All-or-nothing: on Ok every listed correction was applied.
    st.ingest
        .register_corrections(&ing.dataset, req.corrections)
        .await?;
    st.metrics.corrections_applied.inc_by(corrected as u64);
    tracing::info!(target: "audit", actor = %actor.0, action = "register_corrections", dataset = %ing.dataset, corrected, old_chunk_ids = ?old_ids, "corrections applied");
    Ok(Json(CorrectionsResponse { corrected }))
}

pub async fn get_head(
    ing: DatasetAuth,
    State(st): State<AppState>,
) -> Result<Json<HeadResponse>, ServiceError> {
    Ok(Json(st.ingest.head(&ing.dataset).await?.into()))
}

pub async fn get_chunk_status(
    ing: DatasetAuth,
    State(st): State<AppState>,
    // Tuple captures path segments in order (name, id); name is re-derived by DatasetAuth.
    Path((_, id)): Path<(String, String)>,
) -> Result<Json<ChunkStatusResponse>, ServiceError> {
    let status = st.ingest.chunk_status(&ing.dataset, &id).await?;
    Ok(Json(ChunkStatusResponse {
        status: status.into(),
    }))
}

pub async fn health(State(st): State<AppState>) -> StatusCode {
    match st.ingest.ping().await {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            tracing::warn!(error = %e, "health check failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

pub async fn metrics(State(st): State<AppState>) -> Response {
    let mut buf = String::new();
    match prometheus_client::encoding::text::encode(&mut buf, &st.metrics.registry) {
        Ok(()) => (
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            buf,
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = ?e, "encoding metrics failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
