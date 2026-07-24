//! Service error type, JSON error body, IngestError→status mapping, and a JSON extractor that
//! renders parse failures in our error shape.

use axum::extract::FromRequest;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, extract::Request};
use scheduler_metadata::IngestError;
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("missing or invalid bearer token")]
    Unauthorized,
    #[error("token not permitted for this route or dataset")]
    Forbidden,
    #[error("internal error")]
    Internal,
    #[error("invalid request payload: {0}")]
    Payload(String),
    #[error("{} chunk id(s) already exist", .duplicates.len())]
    DuplicateChunks { duplicates: Vec<String> },
    #[error(transparent)]
    Ingest(#[from] IngestError),
}

impl From<JsonRejection> for ServiceError {
    fn from(r: JsonRejection) -> Self {
        Self::Payload(r.body_text())
    }
}

/// `{ error, message, …structured fields }`. Empty/None fields are omitted.
#[derive(Debug, Serialize, Default)]
pub struct ErrorBody {
    pub error: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conflicts_with: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub duplicates: Vec<String>,
}

impl ErrorBody {
    fn simple(error: &'static str, message: impl Into<String>) -> Self {
        Self {
            error,
            message: message.into(),
            ..Default::default()
        }
    }

    /// The generic 500 (status, body), constructed identically wherever an internal error surfaces.
    fn internal() -> (StatusCode, Self) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Self::simple("internal", "internal server error"),
        )
    }
}

/// Single source of truth for the IngestError->(status, body) mapping, shared by `IntoResponse` and
/// tests. Consumes `e` so its fields move into the body; the total match means a new `IngestError`
/// variant fails to compile here.
pub(crate) fn map_ingest_error(e: IngestError) -> (StatusCode, ErrorBody) {
    match e {
        IngestError::DatasetNotFound { name } => (
            StatusCode::NOT_FOUND,
            ErrorBody {
                error: "dataset_not_found",
                message: format!("dataset {name:?} not found"),
                dataset: Some(name),
                ..Default::default()
            },
        ),
        // 400, not 404: a client-supplied schema_id is a client error.
        IngestError::SchemaNotFound { schema_id } => (
            StatusCode::BAD_REQUEST,
            ErrorBody {
                error: "schema_not_found",
                message: format!("schema {schema_id} does not exist for this dataset"),
                schema_id: Some(schema_id.0),
                ..Default::default()
            },
        ),
        IngestError::UnknownTable { schema_id, table } => (
            StatusCode::BAD_REQUEST,
            ErrorBody {
                error: "unknown_table",
                message: format!("schema {schema_id} has no table {table:?}"),
                schema_id: Some(schema_id.0),
                table: Some(table),
                ..Default::default()
            },
        ),
        IngestError::InvalidChunkRange {
            chunk_id,
            first_block,
            last_block,
            size,
        } => (
            StatusCode::BAD_REQUEST,
            ErrorBody {
                error: "invalid_chunk_range",
                message: format!(
                    "chunk {chunk_id:?} has an unstorable range/size \
                     (first_block={first_block}, last_block={last_block}, size={size})"
                ),
                chunk_id: Some(chunk_id),
                ..Default::default()
            },
        ),
        IngestError::RangeOverlap {
            dataset,
            chunk_id,
            conflicts_with,
        } => (
            StatusCode::CONFLICT,
            ErrorBody {
                error: "range_overlap",
                message: format!("chunk {chunk_id:?} overlaps live chunk(s)"),
                dataset: Some(dataset),
                chunk_id: Some(chunk_id),
                conflicts_with,
                ..Default::default()
            },
        ),
        IngestError::DuplicateInBatch { chunk_id } => (
            StatusCode::BAD_REQUEST,
            ErrorBody {
                error: "duplicate_in_batch",
                message: format!("chunk {chunk_id:?} appears more than once in the batch"),
                chunk_id: Some(chunk_id),
                ..Default::default()
            },
        ),
        IngestError::CorrectionRejected { reason } => (
            StatusCode::CONFLICT,
            ErrorBody {
                error: "correction_rejected",
                message: format!("correction rejected: {reason}"),
                ..Default::default()
            },
        ),
        IngestError::ConcurrentSchemaChange { dataset } => (
            StatusCode::CONFLICT,
            ErrorBody {
                error: "concurrent_schema_change",
                message: "the dataset schema was changed concurrently; re-read and retry"
                    .to_owned(),
                dataset: Some(dataset),
                ..Default::default()
            },
        ),
        IngestError::DatasetBusy => (
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorBody {
                error: "dataset_busy",
                message: "timed out waiting for the dataset's ingest lock; retry".to_owned(),
                ..Default::default()
            },
        ),
        IngestError::Database(err) => {
            // Display chain (`{:#}`), not Debug: the source chain stays useful server-side without
            // the Debug dump that can embed a DSN/password. Never sent to the client.
            tracing::error!(error = format!("{err:#}"), "ingest database error");
            ErrorBody::internal()
        }
    }
}

/// The bounded static error code (`ErrorBody.error`) attached to a response as an extension, so
/// `track_metrics` can count errors by code without `IntoResponse` needing `AppState`.
#[derive(Clone, Copy)]
pub(crate) struct ErrorCode(pub(crate) &'static str);

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        // Carry the WWW-Authenticate flag out of the match so there is one tail: build the body,
        // tag the error code (for the metrics counter), attach the header only for 401.
        let (status, body, www_authenticate) = match self {
            ServiceError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                ErrorBody::simple("unauthorized", "missing or invalid bearer token"),
                true,
            ),
            ServiceError::Forbidden => (
                StatusCode::FORBIDDEN,
                ErrorBody::simple("forbidden", "token not permitted for this route or dataset"),
                false,
            ),
            ServiceError::Internal => {
                tracing::error!(
                    "internal service error (misconfigured route or request extraction)"
                );
                let (status, body) = ErrorBody::internal();
                (status, body, false)
            }
            ServiceError::Payload(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorBody::simple("invalid_payload", msg),
                false,
            ),
            ServiceError::DuplicateChunks { duplicates } => (
                StatusCode::CONFLICT,
                ErrorBody {
                    error: "duplicate_chunks",
                    message: format!(
                        "{} chunk id(s) already exist and were not overwritten",
                        duplicates.len()
                    ),
                    duplicates,
                    ..Default::default()
                },
                false,
            ),
            ServiceError::Ingest(e) => {
                let (status, body) = map_ingest_error(e);
                (status, body, false)
            }
        };
        let code = ErrorCode(body.error);
        let mut resp = (status, Json(body)).into_response();
        resp.extensions_mut().insert(code);
        if www_authenticate {
            resp.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        resp
    }
}

/// JSON extractor that renders parse failures as our `ErrorBody` (400) instead of axum's plaintext.
/// Use this everywhere a body is parsed.
pub struct ApiJson<T>(pub T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ServiceError;
    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(v) = Json::<T>::from_request(req, state).await?;
        Ok(ApiJson(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler_metadata::SchemaId;

    // U2: exhaustive (status, error-code) mapping for every IngestError variant.
    #[test]
    fn ingest_error_to_status_and_code() {
        let cases: Vec<(IngestError, StatusCode, &str)> = vec![
            (
                IngestError::DatasetNotFound { name: "d".into() },
                StatusCode::NOT_FOUND,
                "dataset_not_found",
            ),
            (
                IngestError::SchemaNotFound {
                    schema_id: SchemaId(7),
                },
                StatusCode::BAD_REQUEST,
                "schema_not_found",
            ),
            (
                IngestError::UnknownTable {
                    schema_id: SchemaId(7),
                    table: "t".into(),
                },
                StatusCode::BAD_REQUEST,
                "unknown_table",
            ),
            (
                IngestError::InvalidChunkRange {
                    chunk_id: "c".into(),
                    first_block: 9,
                    last_block: 0,
                    size: 1,
                },
                StatusCode::BAD_REQUEST,
                "invalid_chunk_range",
            ),
            (
                IngestError::RangeOverlap {
                    dataset: "d".into(),
                    chunk_id: "c".into(),
                    conflicts_with: vec!["x".into()],
                },
                StatusCode::CONFLICT,
                "range_overlap",
            ),
            (
                IngestError::DuplicateInBatch {
                    chunk_id: "c".into(),
                },
                StatusCode::BAD_REQUEST,
                "duplicate_in_batch",
            ),
            (
                IngestError::CorrectionRejected { reason: "r".into() },
                StatusCode::CONFLICT,
                "correction_rejected",
            ),
            (
                IngestError::ConcurrentSchemaChange {
                    dataset: "d".into(),
                },
                StatusCode::CONFLICT,
                "concurrent_schema_change",
            ),
            (
                IngestError::Database(anyhow::anyhow!("boom")),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
            ),
        ];
        for (err, status, code) in cases {
            let (s, body) = map_ingest_error(err);
            assert_eq!(s, status);
            assert_eq!(body.error, code);
        }
    }
}
