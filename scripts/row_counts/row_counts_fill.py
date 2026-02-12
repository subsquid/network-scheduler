from argparse import ArgumentParser
import os
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
import clickhouse_connect
import os
import sys
import timeit
from urllib.parse import urlparse, ParseResult
from loguru import logger
from typing import Tuple, Dict, List, Optional
from pathlib import PurePosixPath

import pyarrow.parquet as pq
import pyarrow.fs as pafs

class AwsConfig:
    def __init__(self):
        self.key = os.environ['AWS_ACCESS_KEY_ID']
        self.secret = os.environ['AWS_SECRET_ACCESS_KEY'] 
        self.endpoint = strip_scheme(os.environ['AWS_S3_ENDPOINT'])

class ClickhouseConfig:
    def __init__(self, host, user, pw, db):
        self.host = host if host else os.environ['CLICKHOUSE_URL']
        self.username = user if user else os.environ['CLICKHOUSE_USER']
        self.password = pw if pw else os.environ['CLICKHOUSE_PASSWORD']
        self.database = db if db else os.environ['CLICKHOUSE_DATABASE']

def strip_scheme(url):
   parsed_result = urlparse(url)
   return parsed_result.geturl().replace('https://', '', 1)

#### Processing

def process_dataset(aws, clickhouse, dataset, limit=None):
    client = get_clickhouse_connection(clickhouse)

    update = UpdateRowCounts(dataset, 100)
    cursor_ds, cursor_id = None, None
    errors_count = 0
    
    while True:
        chunks, new_cursor = get_chunks_from_db(client, dataset, cursor_ds, cursor_id)
        if not chunks:
            break
        # to make sure that we do not process the same rows over and over again
        cursor_ds, cursor_id = new_cursor

        if test_mode_on:
            use_ssl = False
        else:
            use_ssl = True
    
        for (curr_dataset, chunk_id, files) in chunks:
            row_counts = parquet_row_counts_from_s3_paths(files, aws, "auto", use_ssl)
            if row_counts is None:
                logger.error(f"Error calculating row_counts for: {curr_dataset}/{chunk_id}")
                errors_count += 1
                continue
            
            update_available = update.add(curr_dataset, chunk_id, row_counts)
            if update_available:
                logger.info(f"update_available: {update_available}")
                client.command(update_available)

        update_available = update.finish()
        if update_available:
            logger.info(f"update_available: {update_available}")
            client.command(update_available)

    if errors_count > 0:
        logger.error(f"error encoutered: {errors_count}")
    client.close()
        
def get_chunks_from_db(
    ch,
    dataset: Optional[str],
    last_dataset: Optional[str] = None,
    last_id: Optional[str] = None,
) -> Tuple[List[Tuple[str, str, List[str]]], Optional[Tuple[str, str]]]:
    """
    Keyset pagination:
      - fetch rows ordered by (dataset, id)
      - only return rows with (dataset, id) > (last_dataset, last_id) when cursor is provided
    Returns: (grouped_paths, new_cursor)
      - new_cursor is (dataset, id) of the last row in this batch, or None if no rows.
    """
    logger.info(f"process: {dataset}")

    shards_count = int(os.environ["SHARD_COUNT"])
    shards_current = int(os.environ["SHARD_INDEX"])
    batch_size = int(os.environ["BATCH_SIZE"])

    # Build WHERE clauses
    where = [
        "length(mapKeys(row_counts)) = 0",
        f"(sipHash64(dataset, id) % {shards_count}) = {shards_current}",
    ]

    # Optional dataset filter
    if dataset is not None:
        where.append(f"dataset = {dataset!r}")

    # Keyset cursor filter (tuple comparison)
    if last_dataset is not None and last_id is not None:
        where.append(f"(dataset, id) > ({last_dataset!r}, {last_id!r})")

    where_sql = " AND ".join(where)

    sql = f"""
        SELECT dataset, id, files
          FROM dataset_chunks
         WHERE {where_sql}
      ORDER BY dataset, id
         LIMIT {batch_size}
    """
    logger.info(f"query: {sql}")
    
    result = ch.query(sql)

    grouped_paths: List[Tuple[str, str, List[str]]] = []
    new_cursor: Optional[Tuple[str, str]] = None

    for ds, chunk_id, files_str in result.result_rows:
        files = [f.strip() for f in files_str.split(",") if f.strip()]
        chunk_paths = [f"{ds}/{chunk_id}/{file_name}" for file_name in files]
        grouped_paths.append((ds, chunk_id, chunk_paths))
        new_cursor = (ds, chunk_id)

    return grouped_paths, new_cursor


def get_clickhouse_connection(cfg):
    return clickhouse_connect.get_client(
        host=cfg.host,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
    )

def _as_bucket_key(path: str) -> str:
    # Accept either "s3://bucket/key" or "bucket/key"
    if path.startswith("s3://"):
        u = urlparse(path)
        bucket = u.netloc
        key = u.path.lstrip("/")
        return f"{bucket}/{key}"
    return path.lstrip("/")


def parquet_row_counts_from_s3_paths(
    s3_paths: List[str],
    aws_config,
    region: str = "us-east-1",
    use_ssl: bool = True,
) -> Optional[Dict[str, int]]:
    """
    Given ["s3://bucket/prefix/file.parquet", ...]
    return {"file": row_count, ...}
    """

    scheme = "https" if use_ssl else "http"

    fs = pafs.S3FileSystem(
        access_key=aws_config.key,
        secret_key=aws_config.secret,
        endpoint_override=aws_config.endpoint,
        region=region,
        scheme=scheme,
    )

    row_counts: Dict[str, int] = {}

    for path in s3_paths:
        base_name = PurePosixPath(path).stem
        bucket_key = _as_bucket_key(path)

        try:
            parquet_file = pq.ParquetFile(bucket_key, filesystem=fs)
            row_counts[base_name] = parquet_file.metadata.num_rows

        except Exception as exc:
            # Replace with structured logging in production
            logger.error(f"[ERROR] Failed reading parquet metadata: {path} | {exc}")
            return None

    return row_counts

def _map_literal(row_counts: Dict[str, int]) -> str:
    """
    {"part-0001": 12, "part-0002": 34} ->
      map('part-0001', 12, 'part-0002', 34)

    Empty dict -> map()
    """
    if not row_counts:
        return "map()"

    parts = []
    for key, value in row_counts.items():
        if value < 0:
            raise ValueError("row_counts values must be non-negative")
        parts.append(f"'{key}', {value}")

    return f"map({', '.join(parts)})"

class UpdateRowCounts:
    def __init__(self, bucket: str, batch_amount: int):
        self.count = 0
        self.batch_amount = batch_amount
        self.dataset = bucket
        self.rc_col = ""
        self.keys = ""

    def add(self, dataset: str, chunk_id: str, row_counts: Dict[str, int]) -> Optional[str]:
        """
        Add one (id -> row_counts_map) update to the current batch.
        Returns finalized SQL string when a batch is completed; otherwise None.
        """
        finalized = None
        map_sql = _map_literal(row_counts)

        if self.count == self.batch_amount:
            finalized = self.finish()
        elif dataset != self.dataset:
            finalized = self.finish()
        
        if self.count == 0:
            self.dataset = dataset
            self.rc_col = f"row_counts = multiIf(id = '{chunk_id}', {map_sql}"
            self.keys = f"'{chunk_id}'"
            self.count = 1
        else:
            self.rc_col += f", id = '{chunk_id}', {map_sql}"
            self.keys += f", '{chunk_id}'"
            self.count += 1

        return finalized

    def finish(self) -> str:
        if self.count == 0:
            return ""

        # else branch: keep existing value if id didn't match
        self.rc_col += ", row_counts)"

        if test_mode_on():
            first_line = "ALTER TABLE dataset_chunks"
        else:
            first_line = "ALTER TABLE dataset_chunks_local ON CLUSTER DEFAULT"

        sql = f"""{first_line}
            UPDATE {self.rc_col}
             WHERE dataset = '{self.dataset}'
               AND id IN ({self.keys})
        """
        self.dataset = None
        self.rc_col = ""
        self.keys = ""
        self.count = 0
        
        return sql

def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return True

    
def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-d', '--dataset', help='dedicated to one dataset')
    parser.add_argument('-t', '--test', action='store_true',         default=env_flag("LOCAL_TEST"),  help='dedicated to one dataset')

    args = parser.parse_args()

    return args

def set_test_mode(t: bool):
    global global_test_mode
    global_test_mode = t

def test_mode_on():
    return global_test_mode

if __name__ == "__main__":

    logger.remove()
    logger.add(sys.stdout, colorize=False)

    args = parse_args()
    aws = AwsConfig()
    client = ClickhouseConfig(None, None, None, None)

    set_test_mode(args.test)

    logger.info("start processing")

    process_dataset(aws, client, args.dataset)

    logger.info("success")

