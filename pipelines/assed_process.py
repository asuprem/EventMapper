import sys, os
sys.path.append(os.getcwd())

import pdb
import redis, kafka
import utils.helper_utils as helper_utils


import click
import utils.helper_utils as helper_utils
import time, pdb, json
import kafka, redis
from datetime import datetime, timedelta

TIME_DELTA_MINIMAL = timedelta(seconds=60)
DOWNLOAD_PREPEND = './downloads/'


@click.command()
@click.argument("logdir")
@click.argument("importkey")
@click.argument("exportkey")
def main(logdir, importkey, exportkey):
    pdb.set_trace()
    kafka_import = importkey.replace(":","_")
    kafka_export = exportkey.replace(":","_")
    pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
    r=redis.Redis(connection_pool = pool)
    kafka_producer = kafka.KafkaProducer()
    kafka_consumer = kafka.KafkaConsumer(kafka_import, "auto_offset_reset"="earliest")

    for message in kafka_consumer:

    










if __name__ == "__main__":
    main()