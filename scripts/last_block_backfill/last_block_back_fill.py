from argparse import ArgumentParser
import clickhouse_connect
from datetime import datetime
import duckdb
import linecache
from loguru import logger
import os
import pandas
import sys
import timeit
from urllib.parse import urlparse, ParseResult

DATASETS_WITH_BLOCK_TIME = set(['s3://hl-explorer-blocks'])

def get_chunks_from_db(ch, dataset):
    return ch.query(f"""
        select id from dataset_chunks
         where dataset = '{dataset}'
           and (last_block_hash is NULL or last_block_timestamp = 0)
         order by id
    """)

def get_dedicated_chunks_from_db(ch, dataset):
    n = int(os.environ['JOB_COMPLETION_INDEX'])
    return ch.query(f"""
        select id from dataset_chunks
         where dataset = '{dataset}'
           and (last_block_hash is NULL or last_block_timestamp = 0)
         order by id
         limit 100000 offset {n*100000} 
    """)

def get_bucket_to_process(ch):
    n = int(os.environ['JOB_COMPLETION_INDEX'])
    return ch.query(f"""
        select distinct dataset
          from dataset_chunks
         where dataset not in ('s3://solana-mainnet-3', 's3://solana-mainnet-2', 's3://eclipse-mainnet')
         order by dataset
         limit 1 offset {n}
    """).result_rows[0][0]

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
        self.host = host if host else os.environ['CLICKHOUSE_URL']
        self.username = user if user else os.environ['CLICKHOUSE_USER']
        self.password = pw if pw else os.environ['CLICKHOUSE_PASSWORD']
        self.database = db if db else os.environ['CLICKHOUSE_DATABASE']

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-d', '--dataset', help='dedicated to one dataset')
    parser.add_argument('-t', '--test', action='store_true', help='dedicated to one dataset')

    args = parser.parse_args()

    return args

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

def install_and_load_httpfs(con):
    con.execute("install httpfs; load httpfs")

def extract_last_from_name(parq):
    s = parq.split('/')
    s = s[1].split('-')
    return int(s[1])

def get_row_by_block(con, bucket, parq):
    myobject = f"{bucket}/{parq}"
    block = extract_last_from_name(parq)
    tp = "block_time" if bucket in DATASETS_WITH_BLOCK_TIME else "timestamp"
    return con.sql(f"select hash, {tp} from '{myobject}' where number = {block}").arrow()

def get_clickhouse_connection(cfg):
    return clickhouse_connect.get_client(
        host=cfg.host,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
    )

class Upd:
    def __init__(self, bucket, group):
        self.cnt = 0
        self.group = group
        self.bucket = bucket
        self.hash_col = ''
        self.tmst_col = ''
        self.keys = ''

    def add(self, key, hash, stmp):
        finalized = None
        if self.cnt == 0 or self.cnt == self.group:
            if self.cnt == self.group:
               finalized = self.finish()
        
            self.hash_col = f"last_block_hash = multiIf(id = '{key}', '{hash}'"
            self.tmst_col = f"last_block_timestamp = multiIf(id = '{key}', {stmp}"
            self.keys = f"'{key}'"
            self.cnt = 1
        else:
            self.hash_col += f", id = '{key}', '{hash}'"
            self.tmst_col += f", id = '{key}', {stmp}"
            self.keys += f", '{key}'"
            self.cnt += 1

        return finalized
    
    def finish(self):
        if self.cnt == 0:
            return ''

        self.hash_col += ', last_block_hash)'
        self.tmst_col += ', last_block_timestamp)'

        if test_mode_on(): 
            first_line = "alter table dataset_chunks"
        else:
            first_line = "alter table dataset_chunks_local on cluster default"

        return f"""{first_line}
            update {self.hash_col},
                   {self.tmst_col}
             where dataset = '{self.bucket}'
               and id in ({self.keys})
        """

def process_dataset(aws, chcfg, dataset, limit=None):
    ch = get_clickhouse_connection(chcfg)

    bucket = dataset if dataset else get_bucket_to_process(ch)

    logger.info(f"processing dataset {bucket}")

    upd = Upd(dataset, 100)

    rows = get_dedicated_chunks_from_db(ch, bucket).result_rows if dataset else get_chunks_from_db(ch, bucket).result_rows

    cnt = 0
    last_timestamp = 0
    with duckdb.connect() as con:
        install_and_load_httpfs(con)
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

            ok, new_timestamp = check_and_adapt_timestamp(stmp, last_timestamp)

            if not ok:
               logger.error(f"weird timestamp: {stmp}")
               break

            last_timestamp, stmp = stmp, new_timestamp 

            update = upd.add(chunk, hash, stmp)
            if update:
               ch.command(update)

            cnt += 1
            if limit:
                if cnt >= limit:
                    logger.debug(f"limit ({limit}) reached")
                    break

    update = upd.finish()
    if update:
        ch.command(update)

    logger.info(f"processed {cnt} chunks")
    ch.close()

def check_and_adapt_timestamp(cur_timestamp, last_timestamp):
    if last_timestamp != 0 and cur_timestamp < last_timestamp:
        logger.error(f"DATETIME: {cur_timestamp} < {last_timestamp}")
        return False, None

    if isinstance(cur_timestamp, pandas.Timestamp):
        return True, cur_timestamp.value // (10**6)

    dt = datetime.fromtimestamp(cur_timestamp/1000)
    if dt.year >= 2009:
        return True, cur_timestamp

    new_timestamp = cur_timestamp * 1000

    dt = datetime.fromtimestamp(new_timestamp/1000)
    if dt.year >= 2009:
        return True, new_timestamp

    logger.error(f"DATETIME: {dt}")
    
    return False, None

def set_test_mode(t):
    global global_test_mode
    global_test_mode = t

def test_mode_on():
    return global_test_mode

if __name__ == "__main__":

    logger.remove()
    logger.add(sys.stdout, colorize=False)

    args = parse_args()

    aws = AwsConfig()
    ch = ChConfig(None, None, None, None)

    set_test_mode(args.test)

    logger.info("start processing")

    process_dataset(aws, ch, args.dataset)

    logger.info("success")

