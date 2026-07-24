//! Static bearer tokens. Two disjoint roles: Admin (create datasets) and Ingester (scoped to a set
//! of dataset names). Authorization is enforced by the `require_admin` / `require_dataset_scope`
//! middleware layers (see `build_router`), so a route is guarded by construction rather than by each
//! handler remembering to opt in. Tokens are held only as SHA-256 digests and compared constant-time
//! over a fixed 32 bytes (subtle), so timing leaks neither which token matched nor the presented
//! length. Tokens must be high-entropy random strings: a weak token is guessable regardless.
//!
//! Each token resolves to a [`Principal`] whose `id` is a short digest fingerprint used to attribute
//! audit log events to an actor without exposing the token. (A configured, human-readable id can
//! replace the fingerprint later without touching the middleware or handlers.)

use std::collections::HashSet;
use std::sync::Arc;

use axum::RequestExt;
use axum::extract::{FromRequestParts, Path, Request, State};
use axum::http::{HeaderMap, header, request::Parts};
use axum::middleware::Next;
use axum::response::Response;
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::AppState;
use crate::config::TokensConfig;
use crate::error::ServiceError;

/// What a token is authorized to do: `Admin` (create datasets, promote read schemas) or `Ingester`
/// scoped to a set of dataset names.
#[derive(Clone)]
pub enum Role {
    Admin,
    Ingester(Arc<HashSet<String>>),
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::Admin => "admin",
            Role::Ingester(_) => "ingester",
        })
    }
}

/// An authenticated caller: a stable, non-secret actor `id` plus its role.
#[derive(Clone)]
pub struct Principal {
    pub id: Arc<str>,
    pub role: Role,
}

/// Bearer tokens paired with their principal, keyed by SHA-256 digest.
pub struct TokenStore {
    entries: Vec<([u8; 32], Principal)>,
}

fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Short, non-secret actor id: the first 4 digest bytes as hex. One-way, so it identifies the token
/// in audit logs without exposing it.
fn fingerprint(digest: &[u8; 32]) -> String {
    digest[..4].iter().map(|b| format!("{b:02x}")).collect()
}

fn push_token(
    entries: &mut Vec<([u8; 32], Principal)>,
    seen: &mut HashSet<[u8; 32]>,
    raw: &str,
    role: Role,
) -> anyhow::Result<()> {
    // Trim to match the presented token, which arrives trimmed (see `bearer`).
    let token = raw.trim();
    anyhow::ensure!(!token.is_empty(), "empty bearer token in config");
    let d = digest(token);
    anyhow::ensure!(seen.insert(d), "duplicate bearer token in config");
    let id = Arc::from(fingerprint(&d));
    entries.push((d, Principal { id, role }));
    Ok(())
}

impl TokenStore {
    /// # Errors
    ///
    /// Fails fast — before the service accepts traffic — on any token config that would otherwise
    /// silently break auth: no tokens at all (every request would 401), an empty token (a dead entry
    /// nothing can match), or a token reused across entries (ambiguous, last-match-wins authorization).
    pub fn from_config(cfg: TokensConfig) -> anyhow::Result<Self> {
        let mut entries = Vec::with_capacity(cfg.admins.len() + cfg.ingesters.len());
        let mut seen = HashSet::new();
        for token in &cfg.admins {
            push_token(&mut entries, &mut seen, token.expose_secret(), Role::Admin)?;
        }
        for ing in &cfg.ingesters {
            let scopes: HashSet<String> = ing.datasets.iter().cloned().collect();
            push_token(
                &mut entries,
                &mut seen,
                ing.token.expose_secret(),
                Role::Ingester(Arc::new(scopes)),
            )?;
        }
        anyhow::ensure!(!entries.is_empty(), "tokens config defines no tokens");
        Ok(Self { entries })
    }

    /// Constant-time resolve of a presented bearer token. Scans every entry without early exit so
    /// timing reveals neither which token matched nor the presented length. `None` ⇒ unknown (401).
    #[must_use]
    pub fn authenticate(&self, presented: &str) -> Option<Principal> {
        let presented_digest = digest(presented);
        let mut found: Option<Principal> = None;
        for (stored, principal) in &self.entries {
            if bool::from(stored[..].ct_eq(&presented_digest[..])) {
                found = Some(principal.clone());
            }
        }
        found
    }

    /// Number of configured tokens (admins + ingesters); for the startup log. Not security-sensitive.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Extract the raw bearer token as a SecretString. Any failure ⇒ Unauthorized (401).
fn bearer(headers: &HeaderMap) -> Result<SecretString, ServiceError> {
    let value = headers
        .get(header::AUTHORIZATION)
        .ok_or(ServiceError::Unauthorized)?
        .to_str()
        .map_err(|_| ServiceError::Unauthorized)?;
    let (scheme, token) = value.split_once(' ').ok_or(ServiceError::Unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(ServiceError::Unauthorized);
    }
    let token = token.trim();
    if token.is_empty() {
        return Err(ServiceError::Unauthorized);
    }
    Ok(SecretString::from(token.to_owned()))
}

/// Authenticate the request's bearer token against the current token store.
fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<Principal, ServiceError> {
    let token = bearer(headers)?;
    match state.authenticate(token.expose_secret()) {
        Some(principal) => Ok(principal),
        None => {
            tracing::debug!(reason = "unknown_bearer_token", "unauthenticated request");
            Err(ServiceError::Unauthorized)
        }
    }
}

/// The actor a request authenticated as, stashed for handlers to attribute audit events to.
#[derive(Clone)]
pub struct Actor(pub Arc<str>);

/// Layer for `POST /datasets`: admin token only. 401 missing/unknown, 403 ingester token.
pub async fn require_admin(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ServiceError> {
    let Principal { id, role } = authenticate(&state, req.headers())?;
    match role {
        Role::Admin => {
            req.extensions_mut().insert(Actor(id));
            Ok(next.run(req).await)
        }
        Role::Ingester(_) => {
            tracing::warn!(actor = %id, route = "admin", "denied: ingester token on admin route");
            Err(ServiceError::Forbidden)
        }
    }
}

/// The dataset a request was authorized for, stashed by [`require_dataset_scope`] for the handler's
/// [`DatasetAuth`] to read. Guarantees the handler operates on the name that was scope-checked.
#[derive(Clone)]
pub struct AuthedDataset(pub String);

/// Layer for the per-dataset ingester routes: authenticate, then require an ingester token scoped to
/// the `{name}` path segment. 401 missing/unknown, 403 wrong-role or out-of-scope. Runs before the
/// handler and before any DB access, so an out-of-scope dataset returns 403 whether or not it exists.
pub async fn require_dataset_scope(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ServiceError> {
    let Principal { id, role } = authenticate(&state, req.headers())?;
    let Path(params) = req
        .extract_parts::<Path<std::collections::HashMap<String, String>>>()
        .await
        .map_err(|_| ServiceError::Internal)?;
    let name = params.get("name").cloned().ok_or(ServiceError::Internal)?;
    match &role {
        Role::Ingester(scopes) if scopes.contains(&name) => {
            req.extensions_mut().insert(Actor(id));
            req.extensions_mut().insert(AuthedDataset(name));
            Ok(next.run(req).await)
        }
        _ => {
            tracing::warn!(
                actor = %id,
                dataset = %name,
                role = %role,
                "denied: token not scoped for dataset"
            );
            Err(ServiceError::Forbidden)
        }
    }
}

/// The authorized dataset name, read from the extension [`require_dataset_scope`] set. Absent only if
/// a route is wired without that layer — a bug that must fail closed (500), never run unauthorized.
pub struct DatasetAuth {
    pub dataset: String,
}

impl FromRequestParts<AppState> for DatasetAuth {
    type Rejection = ServiceError;
    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthedDataset>()
            .map(|d| DatasetAuth {
                dataset: d.0.clone(),
            })
            .ok_or(ServiceError::Internal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{IngesterConfig, TokensConfig};

    fn store() -> TokenStore {
        TokenStore::from_config(TokensConfig {
            admins: vec![SecretString::from("A".to_string())],
            ingesters: vec![IngesterConfig {
                token: SecretString::from("I".to_string()),
                datasets: ["ds-a".to_string()].into_iter().collect(),
            }],
        })
        .expect("valid token config")
    }

    #[test]
    fn admin_token_resolves_to_admin() {
        assert!(matches!(
            store().authenticate("A"),
            Some(Principal {
                role: Role::Admin,
                ..
            })
        ));
    }

    #[test]
    fn ingester_token_resolves_with_scope() {
        let Some(Principal {
            role: Role::Ingester(scopes),
            ..
        }) = store().authenticate("I")
        else {
            panic!("expected ingester role");
        };
        assert!(scopes.contains("ds-a"));
        assert!(!scopes.contains("ds-b"));
    }

    #[test]
    fn unknown_token_is_none() {
        assert!(store().authenticate("nope").is_none());
    }

    #[test]
    fn tokens_get_distinct_fingerprints() {
        let s = store();
        let a = s.authenticate("A").unwrap().id;
        let i = s.authenticate("I").unwrap().id;
        assert_eq!(a.len(), 8);
        assert_ne!(a, i);
    }

    #[test]
    fn duplicate_token_across_roles_is_rejected() {
        let cfg = TokensConfig {
            admins: vec![SecretString::from("dup".to_string())],
            ingesters: vec![IngesterConfig {
                token: SecretString::from("dup".to_string()),
                datasets: ["ds-a".to_string()].into_iter().collect(),
            }],
        };
        assert!(TokenStore::from_config(cfg).is_err());
    }

    #[test]
    fn empty_config_is_rejected() {
        let cfg = TokensConfig {
            admins: vec![],
            ingesters: vec![],
        };
        assert!(TokenStore::from_config(cfg).is_err());
    }
}
