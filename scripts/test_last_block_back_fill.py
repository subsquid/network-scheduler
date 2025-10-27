from boto3 import client
import clickhouse_connect
from datetime import datetime
import duckdb
from loguru import logger
import os
import sys
import timeit
from urllib.parse import urlparse, ParseResult

from last_block_back_fill import AwsConfig, create_secrets, get_clickhouse_connection, process_dataset

def list_chunks(bucket, limit=None):
    s3 = client('s3')
    chunks = {}

    max_keys = limit if limit else 1000
    continue_with = ''
    cnt = 0
    result =  s3.list_objects_v2(Bucket=bucket, MaxKeys=1000)
    while True:
        for key in result['Contents']:
            tmp = key['Key'].split('/')
            chunk = tmp[0] + '/' + tmp[1]
            chunks[chunk] = True
        if not result['IsTruncated']:
            break
        cnt += 1
        if cnt >= limit:
            break
        continue_with = result['NextContinuationToken']
        result =  s3.list_objects_v2(Bucket=bucket, MaxKeys=max_keys, ContinuationToken=continue_with)
    return chunks

def get_all_rows(con, bucket, parq):
    myobject = f"s3://{bucket}/{parq}"
    con.sql(f"select number, hash, timestamp from '{myobject}'").arrow()

def check_table(ch):
    if not global_test_mode:
        return

    logger.debug("HAVE:")
    result = ch.query("select * from dataset_chunks")
    for row in result.result_rows:
        logger.debug(row)

def prepare_test_data(aws, bucket, limit=None):
    if not global_test_mode:
        return

    chunks = list_chunks(bucket, limit)
    ch = get_clickhouse_connection()

    create_table(ch)

    cnt = 0
    with duckdb.connect() as con:
        create_secrets(con, aws)
        for chunk in chunks:
            file = chunk + '/blocks.parquet'
            logger.debug(file)
                
            ch.insert(
                'dataset_chunks',
                [[bucket, chunk, 0, '']],
                column_names=['dataset', 'id', 'size', 'files'],
            )

            if limit:
                cnt += 1
                if cnt >= limit:
                    logger.debug(f"limit ({limit}) reached")
                    break

    check_table(ch)
    ch.close()

def create_table(ch):
    ch.command("""
        CREATE TABLE IF NOT EXISTS dataset_chunks (
            dataset LowCardinality(String) NOT NULL,
            id String NOT NULL,
            size UInt64 NOT NULL,
            files LowCardinality(String) NOT NULL,
            last_block_hash Nullable(String),
            last_block_timestamp UInt64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (dataset, id)
    """)

if __name__ == "__main__":

    logger.remove()
    logger.add(sys.stdout, colorize=True)

    bucket = 'solana-mainnet-1'
    file = '0325654344/0325837552-0325838188-M12Qur8v/blocks.parquet'

    logger.info(f"start processing {bucket}")

    aws = AwsConfig()

    global global_test_mode
    global_test_mode = True

    limit = 10
    prepare_test_data(aws, bucket, limit=limit)
    t = timeit.Timer(lambda: process_dataset(aws, bucket, limit=limit))
    logger.info(f'processing of {limit if limit else "all"} took {t.timeit(1)}s')
    check_table(get_clickhouse_connection())

    """
    with duckdb.connect() as con:
        stmt = secrets_statement(aws)
        con.execute(stmt)

        it = 10

        t = timeit.Timer(lambda: get_all_rows(con, bucket, file))
        total = t.timeit(it)
        print(f'all rows: {total/it}')

        t = timeit.Timer(lambda: get_row_by_block(con, bucket, file))
        total = t.timeit(it)
        print(f'by block: {total/it}')
    """    
    logger.info("success")


