from argparse import ArgumentParser
import clickhouse_connect
from datetime import datetime
import duckdb
from loguru import logger
import os
import sys
import timeit
from urllib.parse import urlparse, ParseResult

def get_chunks_from_db(ch, dataset):
    return ch.query(f"""
        select id from dataset_chunks
         where dataset = '{dataset}'
           and (last_block_hash is NULL or last_block_timestamp = 0)
         order by id
    """)

def strip_scheme(url):
   parsed_result = urlparse(url)
   return parsed_result.geturl().replace('https://', '', 1)

class AwsConfig:
    def __init__(self):
        self.key = os.environ['AWS_ACCESS_KEY_ID']
        self.secret = os.environ['AWS_SECRET_ACCESS_KEY']
        self.endpoint = strip_scheme(os.environ['AWS_ENDPOINT_URL'])

class ChConfig:
    def __init__(self, host, user, pw, db):
        self.host = host
        self.username = user
        self.password = pw
        self.database = db

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('datasets', help='datasets to process')
    parser.add_argument('-o', '--host', help='clickhouse host')
    parser.add_argument('-u', '--user', help='clickhouse username')
    parser.add_argument('-p', '--password', help='clickhouse password')
    parser.add_argument('-d', '--database', help='clickhouse database name')

    args = parser.parse_args()

    return args.datasets, ChConfig(
        args.host,
        args.user,
        args.password,
        args.database,
    )

def secrets_statement(cfg):
    return f"""create or replace secret (
        type s3,
        url_style 'path',
        region 'auto',
        key_id '{cfg.key}',
        secret '{cfg.secret}',
        endpoint '{cfg.endpoint}')
    """

def create_secrets(con, cfg):
    stmt = secrets_statement(cfg)
    con.execute(stmt)

def extract_last_from_name(parq):
    s = parq.split('/')
    s = s[1].split('-')
    return int(s[1])

def get_row_by_block(con, bucket, parq):
    myobject = f"s3://{bucket}/{parq}"
    block = extract_last_from_name(parq)
    return con.sql(f"select hash, timestamp from '{myobject}' where number = {block}").arrow()

def get_clickhouse_connection(cfg):
    return clickhouse_connect.get_client(
        host=cfg.host,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
    )

def process_dataset(aws, chcfg, bucket, limit=None):
    logger.info(f"processing dataset {bucket}")
    ch = get_clickhouse_connection(chcfg)
    rows = get_chunks_from_db(ch, bucket).result_rows
    cnt = 0
    last_timestamp = 0
    with duckdb.connect() as con:
        create_secrets(con, aws)

        for row in rows:
            chunk = row[0]
            file = chunk + '/blocks.parquet'
            logger.debug(file)
            ar = get_row_by_block(con, bucket, file)
            for batch in ar:
                p = batch.to_pandas()
                break

            hash = p.iat[0, 0]
            stmp = p.iat[0, 1]

            if not check_timestamp(stmp, last_timestamp):
               logger.error(f"weird timestamp: {stmp}")
               break

            last_timestamp = stmp 

            store( 
                ch,
                bucket,
                chunk,
                hash,
                stmp,
            )

            cnt += 1
            if limit:
                if cnt >= limit:
                    logger.debug(f"limit ({limit}) reached")
                    break

    logger.info(f"processed {cnt} chunks")
    ch.close()

def check_timestamp(cur_timestamp, last_timestamp):
    if cur_timestamp < last_timestamp:
        logger.error(f"DATETIME: {cur_timestamp} < {last_timestamp}")
        return False

    dt = datetime.fromtimestamp(cur_timestamp)
    if dt.year >= 2009:
        return True

    logger.debug(f"DATETIME: {dt}")

    dt = datetime.fromtimestamp(cur_timestamp/1e3)
    if dt.Year >= 2009:
        return True

    logger.error(f"DATETIME: {dt}")
    
    return False

def store(ch, bucket, chunk, hash, timestamp):
    ch.command(f"""alter table dataset_chunks
                   update last_block_hash = '{hash}',
                          last_block_timestamp = {timestamp}
                    where id = '{chunk}'
                      and dataset = '{bucket}'""")

if __name__ == "__main__":

    logger.remove()
    logger.add(sys.stdout, colorize=False)

    logger.info("start processing")

    bucket = 'solana-mainnet-1'

    global global_test_mode
    global_test_mode = False

    aws = AwsConfig()
    datasets, ch = parse_args()

    with open(datasets, encoding="utf-8") as f:
        for bucket in f:
            process_dataset(aws, ch, bucket.strip())

    logger.info("success")


