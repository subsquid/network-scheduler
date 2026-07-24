# metadata-service — HTTP API

Ingesters report chunk metadata here instead of the scheduler discovering it from S3. A 2xx insert is durably accepted — overlap is decided at insert time, under a per-dataset lock (ADR 0003).

Generated from the `#[utoipa::path]` annotations — do not edit by hand. Regenerate: `UPDATE_API_DOC=1 cargo test -p metadata-service --test api_doc`.

Authentication: `Authorization: Bearer <token>`. Admin endpoints need an admin token; dataset endpoints need a token scoped to that dataset; `/health` and `/metrics` are open.

## `POST /datasets`

Create a dataset. Its schema is seeded as both the first write schema and the current read schema. Idempotent by name: re-creating an existing dataset changes nothing and returns 200.

Request body: [`CreateDatasetRequest`](#createdatasetrequest)

Responses:
- `200` Dataset already existed; unchanged — [`CreateDatasetResponse`](#createdatasetresponse)
- `201` Dataset created — [`CreateDatasetResponse`](#createdatasetresponse)
- `400` Invalid name, location, or schema — [`ErrorBody`](#errorbody)

## `POST /datasets/{name}/chunks`

Report chunks the ingester has written to storage. A 2xx means durably accepted: the overlap decision happens in this call (see ADR 0003), so the ingester can safely resume past the reported range. Re-sending a batch is safe — already-present ids come back as duplicates.

Path parameters:
- `name` (string) — Dataset name

Request body: [`InsertChunksRequest`](#insertchunksrequest)

Responses:
- `204` All chunks inserted
- `409` `duplicate_chunks`: listed ids were already present (new ones in the batch WERE committed). `range_overlap`: nothing committed; the batch overlaps a live chunk — [`ErrorBody`](#errorbody)
- `503` `dataset_busy`: lock wait timed out behind a wedged writer; retry — [`ErrorBody`](#errorbody)

## `GET /datasets/{name}/chunks/{id}/status`

Path parameters:
- `name` (string) — Dataset name
- `id` (string) — Chunk id

Responses:
- `200` pending | admitted | rejected | not_found — [`ChunkStatusResponse`](#chunkstatusresponse)

## `POST /datasets/{name}/corrections`

Register chunk replacements after a reorg: each old chunk is superseded 1:1 by a new chunk covering the same block range. Success means all listed corrections are durably REGISTERED (all-or-nothing), not yet applied: the old chunk keeps serving until workers confirm holding the replacement, then the scheduler performs the swap. A registered correction is never rejected later — replacements are exempt from the overlap gate by design.

Path parameters:
- `name` (string) — Dataset name

Request body: [`CorrectionsRequest`](#correctionsrequest)

Responses:
- `200` All corrections registered; the scheduler swaps each in once workers confirm its replacement — [`CorrectionsResponse`](#correctionsresponse)
- `409` Rejected wholesale (unknown old chunk, changed range, duplicate, or unavailable old chunk) — [`ErrorBody`](#errorbody)

## `GET /datasets/{name}/head`

Where ingestion should resume: the highest block any non-rejected chunk covers, or null for an empty dataset. The ingester continues from the next block.

Path parameters:
- `name` (string) — Dataset name

Responses:
- `200` The resume point — [`HeadResponse`](#headresponse)

## `GET /datasets/{name}/read-schema`

The dataset's current read schema — what readers should decode under right now.

Path parameters:
- `name` (string) — Dataset name

Responses:
- `200` The current read schema — [`CurrentReadSchemaResponse`](#currentreadschemaresponse)
- `404` Never promoted — [`ErrorBody`](#errorbody)

## `PUT /datasets/{name}/read-schema`

Set the dataset's current read schema: the single superset schema every reader decodes under. Admin-only because it governs all consumers, not one ingester's writes.

Path parameters:
- `name` (string) — Dataset name

Request body: [`DatasetSchema`](#datasetschema)

Responses:
- `200` Promoted (idempotent by content) — [`ReadSchemaResponse`](#readschemaresponse)
- `409` Concurrent schema change; retry — [`ErrorBody`](#errorbody)

## `GET /datasets/{name}/write-schemas`

List every schema the dataset's chunks may be pinned to. `active` means a live chunk still pins it — readers must keep being able to decode those.

Path parameters:
- `name` (string) — Dataset name

Responses:
- `200` Every write schema, flagged active when a live chunk pins it — [`SchemaListResponse`](#schemalistresponse)

## `POST /datasets/{name}/write-schemas`

Announce the schema an ingester is about to write chunks under. Returns the schema id the ingester must pin in every inserted chunk; identical content always yields the same id.

Path parameters:
- `name` (string) — Dataset name

Request body: [`DatasetSchema`](#datasetschema)

Responses:
- `200` Registered (idempotent by content) — [`WriteSchemaResponse`](#writeschemaresponse)
- `400` Invalid schema — [`ErrorBody`](#errorbody)

## `GET /datasets/{name}/write-schemas/{id}`

Fetch one write schema by id — e.g. to decode chunks pinned to it.

Path parameters:
- `name` (string) — Dataset name
- `id` (integer) — Write schema id

Responses:
- `200` The schema — [`WriteSchemaViewResponse`](#writeschemaviewresponse)
- `404` No such schema for this dataset — [`ErrorBody`](#errorbody)

## `GET /health`

Liveness probe: verifies the database is reachable.

Responses:
- `200` Database reachable
- `503` Database unreachable

## `GET /metrics`

Prometheus metrics for scraping.

Responses:
- `200` Prometheus text exposition

# Schemas

## `ChunkStatusResponse`

| field | type | required | notes |
|---|---|---|---|
| `status` | [`ChunkStatusWire`](#chunkstatuswire) | yes |  |

## `ChunkStatusWire`

One of: `"pending"`, `"admitted"`, `"rejected"`, `"not_found"`

## `Correction`

| field | type | required | notes |
|---|---|---|---|
| `old_chunk_id` | string | yes |  |
| `replacement` | [`IngestChunk`](#ingestchunk) | yes |  |

## `CorrectionsRequest`

| field | type | required | notes |
|---|---|---|---|
| `corrections` | array of [`Correction`](#correction) | yes |  |

## `CorrectionsResponse`

| field | type | required | notes |
|---|---|---|---|
| `corrected` | integer | yes | Count of corrections registered (the swap itself happens later, scheduler-side). |

## `CreateDatasetRequest`

| field | type | required | notes |
|---|---|---|---|
| `location` | string | yes |  |
| `name` | object |  |  |
| `schema` | [`DatasetSchema`](#datasetschema) | yes |  |

## `CreateDatasetResponse`

| field | type | required | notes |
|---|---|---|---|
| `created` | boolean | yes |  |
| `name` | string | yes |  |

## `CurrentReadSchemaResponse`

| field | type | required | notes |
|---|---|---|---|
| `read_schema_id` | integer | yes |  |
| `schema` | [`DatasetSchema`](#datasetschema) | yes |  |

## `DatasetSchema`

| field | type | required | notes |
|---|---|---|---|
| `tables` | map of [`TableSchema`](#tableschema) | yes |  |

## `ErrorBody`

| field | type | required | notes |
|---|---|---|---|
| `chunk_id` | object |  |  |
| `conflicts_with` | array of string |  |  |
| `dataset` | object |  |  |
| `detail` | object |  |  |
| `duplicates` | array of string |  |  |
| `error` | string | yes |  |
| `message` | string | yes |  |
| `schema_id` | object |  |  |
| `table` | object |  |  |

## `HeadResponse`

| field | type | required | notes |
|---|---|---|---|
| `stored_head` | object |  |  |

## `IngestChunk`

| field | type | required | notes |
|---|---|---|---|
| `first_block` | integer | yes |  |
| `id` | string | yes |  |
| `last_block` | integer | yes |  |
| `last_block_hash` | object |  |  |
| `last_block_timestamp` | object |  |  |
| `schema_id` | integer | yes | Explicit — never COALESCE-stamped. A superseded id is legal. |
| `size` | integer | yes |  |
| `tables` | object |  | Table *names* present, resolved against `schema_id`'s tables in sorted-name order (unknown → `UnknownTable`). `None` or a full list = all present (NULL bitmap); `Some([])` = none (all-zero). |

## `InsertChunksRequest`

| field | type | required | notes |
|---|---|---|---|
| `chunks` | array of [`IngestChunk`](#ingestchunk) | yes |  |

## `ReadSchemaResponse`

| field | type | required | notes |
|---|---|---|---|
| `read_schema_id` | integer | yes |  |

## `SchemaInfoDto`

| field | type | required | notes |
|---|---|---|---|
| `active` | boolean | yes |  |
| `schema` | [`DatasetSchema`](#datasetschema) | yes |  |
| `schema_id` | integer | yes |  |

## `SchemaListResponse`

| field | type | required | notes |
|---|---|---|---|
| `schemas` | array of [`SchemaInfoDto`](#schemainfodto) | yes |  |

## `TableSchema`

| field | type | required | notes |
|---|---|---|---|
| `default_fields` | array of string | yes | The implicit projection for callers that omit a column list; a subset of `fields`. |
| `fields` | array of string | yes |  |

## `WriteSchemaResponse`

| field | type | required | notes |
|---|---|---|---|
| `schema_id` | integer | yes |  |

## `WriteSchemaViewResponse`

| field | type | required | notes |
|---|---|---|---|
| `schema` | [`DatasetSchema`](#datasetschema) | yes |  |
| `schema_id` | integer | yes |  |
