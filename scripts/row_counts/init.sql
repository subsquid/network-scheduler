CREATE DATABASE IF NOT EXISTS testnet;

CREATE TABLE IF NOT EXISTS testnet.dataset_chunks (
            dataset LowCardinality(String) NOT NULL,
            id String NOT NULL,
            size UInt64 NOT NULL,
            files LowCardinality(String) NOT NULL,
            last_block_hash Nullable(String),
            last_block_timestamp UInt64,
            row_counts Map(LowCardinality(String), UInt32) DEFAULT map()
        ) ENGINE = ReplacingMergeTree() ORDER BY (dataset, id);

INSERT INTO testnet.dataset_chunks
    (dataset, id, size, files, last_block_hash, last_block_timestamp)
VALUES
(
    's3://base',
    '0000000000/0000000000-0000573739-cd076ab0',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0000000000/0000573740-0001146939-e632b81c',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0000000000/0001146940-0001543239-148c34fa',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0000000000/0001543240-0001850659-1c5d2eaf',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0000000000/0001850660-0001911641-cf44aa39',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0001911642/0001911642-0001960839-f7626146',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0001911642/0001960840-0001967559-bd2f9293',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0001911642/0001967560-0001973459-3b5f276f',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
),
(
    's3://base',
    '0001911642/0001973460-0001982819-565c28a3',
    0,
    'blocks.parquet,logs.parquet,statediffs.parquet,traces.parquet,transactions.parquet',
    NULL,
    0
);
