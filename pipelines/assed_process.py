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
@click.argument("processscriptdir")
@click.argument("pidname")
@click.option("--debug", type=int)
@click.option("--seekval", type=int)
def main(logdir, importkey, exportkey, processscript, processscriptdir, pidname, debug, seekval):
    if debug is None:
        debug = 0
    if debug:
        helper_utils.std_flush("[%s] -- DEBUG_MODE -- Active"%helper_utils.readable_time())
    pid_name = pidname
    if not debug:
        helper_utils.setup_pid(pid_name, logdir=logdir)
    

    # Import processscript
    helper_utils.std_flush("[%s] -- Initializing ASSED-Process %s"%(helper_utils.readable_time(), pidname))
    moduleImport = __import__("pipelines.%s.%s"%(processscriptdir, processscript), fromlist=[processscript])
    MessageProcessor = getattr(moduleImport, processscript)
    if debug:
        MessageProcessor = MessageProcessor(debug=True)
    else:
        MessageProcessor = MessageProcessor()
    helper_utils.std_flush("[%s] -- Imported Module %s"%(helper_utils.readable_time(), processscript))
    
    kafka_import = importkey.replace(":","_")
    helper_utils.std_flush("[%s] -- Generated kafka import key %s"%(helper_utils.readable_time(), kafka_import))
    kafka_export = exportkey.replace(":","_")
    helper_utils.std_flush("[%s] -- Generated kafka export key %s"%(helper_utils.readable_time(), kafka_export))
    pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
    r=redis.Redis(connection_pool = pool)
    helper_utils.std_flush("[%s] -- Connected to redis with ConnectionPool on port 6379"%helper_utils.readable_time())
    
    seek_partition = r.get(exportkey+":partition")
    seek_offset = r.get(exportkey+":offset")
    seek_partition = 0 if seek_partition is None else int(seek_partition)
    seek_offset = 0 if seek_offset is None else int(seek_offset)+1
    helper_utils.std_flush("[%s] -- Obtained seek partition for kafka at Partition %i -- Offset %i"%(helper_utils.readable_time(), seek_partition, seek_offset))

    # replace seek value
    if debug:
        if seekval is not None:
            seek_offset = seekval
            helper_utils.std_flush("[%s] -- DEBUG -- Replaced seek offset for kafka at Partition %i -- Offset %i"%(helper_utils.readable_time(), seek_partition, seek_offset))

    
    kafka_producer = kafka.KafkaProducer()
    helper_utils.std_flush("[%s] -- Generated kafka producer"%helper_utils.readable_time())
    kafka_consumer = kafka.KafkaConsumer()
    helper_utils.std_flush("[%s] -- Generated kafka consumer"%helper_utils.readable_time())
    TopicPartition = kafka.TopicPartition(kafka_import, seek_partition)
    kafka_consumer.assign([TopicPartition])
    kafka_consumer.seek(TopicPartition, seek_offset)
    helper_utils.std_flush("[%s] -- Set kafka consumer seek"%helper_utils.readable_time())
    
    message_correct_counter = 0
    message_fail_counter = 0
    message_counter = 0

    for message in kafka_consumer:
        item = json.loads(message.value.decode())
        processedMessage = MessageProcessor.process(item)
        # Push the message to kafka...if true
        if type(processedMessage) != type(tuple()):
            raise ValueError("[%s] -- ERROR -- Invalid type %s for processedMessage. MessageProcessor.process() must return tuple of (bool,message)."%(helper_utils.readable_time(), str(type(processedMessage))))
        if not processedMessage[0]:
            message_fail_counter+=1
        else:
            if not debug:
                byted = bytes(json.dumps(processedMessage[1]), encoding="utf-8")
                kafka_producer.send(kafka_export, byted)
                kafka_producer.flush()
            message_correct_counter+=1
        message_counter += 1
        
        if not debug:
            r.set(exportkey+":partition", message.partition)
            r.set(exportkey+":offset", message.offset)
            r.set(exportkey+":timestamp", message.timestamp)
        
        if message_counter%1000 == 0:
            helper_utils.std_flush("[%s] -- Processed %i messages with %i failures and %i successes"%(helper_utils.readable_time(), message_counter, message_fail_counter, message_correct_counter))
        
    
if __name__ == "__main__":
    main() #pylint: disable=no-value-for-parameter