import sys, os
sys.path.append(os.getcwd())

import kafka, redis
import pdb, click, json
import utils.helper_utils as helper_utils

@click.command()
@click.argument("importkey")
@click.argument("exportkey")
@click.option("--seekval", type=int)
def main(importkey, exportkey, seekval):

    kafka_import = importkey.replace(":","_")
    helper_utils.std_flush("Generated kafka import key %s"%kafka_import)
    kafka_export = exportkey.replace(":","_")
    helper_utils.std_flush("Generated kafka export key %s"%kafka_export)
    pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
    r=redis.Redis(connection_pool = pool)
    helper_utils.std_flush("Connected to redis")

    seek_partition = r.get(exportkey+":partition")
    seek_offset = r.get(exportkey+":offset")
    seek_partition = 0 if seek_partition is None else int(seek_partition)
    seek_offset = 0 if seek_offset is None else int(seek_offset)+1
    helper_utils.std_flush("Obtained seek partition for kafka at Partition %i -- Offset %i"%(seek_partition, seek_offset))

    if seekval is not None:
        seek_offset = seekval
        helper_utils.std_flush("Replaced seek offset for kafka at Partition %i -- Offset %i"%(seek_partition, seek_offset))
    helper_utils.std_flush("\n\n")


    kafka_consumer = kafka.KafkaConsumer()
    helper_utils.std_flush("Generated kafka consumer")
    TopicPartition = kafka.TopicPartition(kafka_import, seek_partition)
    kafka_consumer.assign([TopicPartition])
    kafka_consumer.seek(TopicPartition, seek_offset)
    helper_utils.std_flush("Set kafka consumer seek")

    for message in kafka_consumer:
        #pdb.set_trace()
        jsval = json.loads(message.value.decode())
        helper_utils.std_flush(jsval["streamtype"])


if __name__ == "__main__":
    main()