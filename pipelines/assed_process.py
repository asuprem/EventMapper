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


@click.command()
@click.argument("logdir")
@click.argument("importkey")
@click.argument("exportkey")
@click.argument("processscript")
def main(logdir, importkey, exportkey, processscript):
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    #helper_utils.setup_pid(pid_name, logdir=logdir)

    
    kafka_import = importkey.replace(":","_")
    kafka_export = exportkey.replace(":","_")
    pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
    r=redis.Redis(connection_pool = pool)
    kafka_producer = kafka.KafkaProducer()
    kafka_consumer = kafka.KafkaConsumer(kafka_import, auto_offset_reset="earliest")

    for message in kafka_consumer:
        pdb.set_trace()
    










if __name__ == "__main__":
    main()