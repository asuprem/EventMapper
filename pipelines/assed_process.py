import sys, os
sys.path.append(os.getcwd())


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
    
    seek_partition = r.get(exportkey+":partition")
    seek_offset = r.get(exportkey+":offset")
    seek_partition = 0 if seek_partition is None else int(seek_partition)
    seek_offset = 0 if seek_offset is None else int(seek_offset)
    
    kafka_producer = kafka.KafkaProducer()
    kafka_consumer = kafka.KafkaConsumer()
    TopicPartition = kafka.TopicPartition(kafka_import, seek_partition)
    kafka_consumer.assign([TopicPartition])
    kafka_consumer.seek(TopicPartition, seek_offset)
    
    for message in kafka_consumer:
        item = json.loads(message.value.decode())

        r.set(exportkey+":partition", message.partition)
        r.set(exportkey+":offset", message.offset)
        r.set(exportkey+":timestamp", message.timestamp)

        pdb.set_trace()
    










if __name__ == "__main__":
    main() #pylint: disable=no-value-for-parameter