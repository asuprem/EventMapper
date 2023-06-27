import sys, os
sys.path.append(os.getcwd())

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
@click.argument("dataprocessor")
@click.argument("dataprocessorscriptdir")
@click.argument("pidname")
def main(logdir, importkey, exportkey, dataprocessor, dataprocessorscriptdir, pidname):
    TOP_OF_FILE_START = True
    pid_name = pidname
    helper_utils.setup_pid(pid_name, logdir=logdir)

    # Import processscript
    helper_utils.std_flush("[%s] -- Initializing ASSED-input-buffer %s"%(helper_utils.readable_time(), pidname))
    moduleImport = __import__("pipelines.%s.%s"%(dataprocessorscriptdir, dataprocessor), fromlist=[dataprocessor])
    DataProcessor = getattr(moduleImport, dataprocessor)
    DataProcessor = DataProcessor()
    helper_utils.std_flush("[%s] -- Imported Data processor %s"%(helper_utils.readable_time(),dataprocessor))

    # Set up connections
    pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
    r=redis.Redis(connection_pool = pool)
    kafka_key = exportkey.replace(":","_")
    kafka_producer = kafka.KafkaProducer()

    message_refresh = 7200
    skip_count = 0
    process_count = 0
    time_slept = 0
    message_timer = time.time()

    # Get earliest file to parse...
    helper_utils.std_flush("[%s] -- Searching for files"%helper_utils.readable_time())
    finishedUpToTime = r.get(importkey)
    granularTime = 0
    if finishedUpToTime is None:
        finishedUpToTime = 0
    else:
        finishedUpToTime = int(finishedUpToTime.decode())

    if finishedUpToTime == 0:
        # TODO CHANGE TO 7 days after setup is complete...
        helper_utils.std_flush("[%s] -- No value for previous stop. Starting from 7 days prior"%helper_utils.readable_time())
        currentTime = datetime.now() - timedelta(days=7)
        foundFlag = 0
        while foundFlag == 0:
            filePath = DataProcessor.getInputPath(currentTime)
            if os.path.exists(filePath):
                # We found the most recent file, and increment our counter
                finishedUpToTime = currentTime
                foundFlag = 1
            else:
                # If our search is too broad - i.e. we are a month behind, ignore
                currentTime+=TIME_DELTA_MINIMAL
                timeDeltaOutputStream = (datetime.now() - currentTime)
                if timeDeltaOutputStream.days  == 0 and timeDeltaOutputStream.seconds  <= 1:
                    foundFlag = -1
    else:
        # I.E. if we already have a timestmap from pervious execution, we will read files that are a minute behind, and catch up to the granular time
        helper_utils.std_flush("[%s] -- Starting File tracking at %s"%(helper_utils.readable_time(),str(datetime.fromtimestamp(finishedUpToTime/1000.0))))
        granularTime = finishedUpToTime
        finishedUpToTime = datetime.fromtimestamp(granularTime/1000.0) - timedelta(seconds = 60)
        TOP_OF_FILE_START = False
    if TOP_OF_FILE_START:
        # Otherwise, we start from the beginning of the 'first' file...
        helper_utils.std_flush("[%s] -- Starting at first file"%(helper_utils.readable_time()))
        granularTime = 0
        finishedUpToTime = datetime.fromtimestamp(granularTime/1000.0) - timedelta(seconds = 60)
        
    
    prevGranular = granularTime

    helper_utils.std_flush("[%s] -- Starting Stream Tracking for %s"%(helper_utils.readable_time(), importkey))
    while True:
        if time.time() - message_timer > message_refresh:
            message_timer = time.time()
            helper_utils.std_flush("[%s] -- Processed %i items, with %i items skipped and %i seconds slept in the last %i seconds"%(helper_utils.readable_time(), process_count, skip_count, time_slept, message_refresh))
            process_count, skip_count, time_slept = 0, 0, 0
            
            
        if (datetime.now() - finishedUpToTime).total_seconds() < 60:
            helper_utils.std_flush("[%s] -- Sleeping"%(helper_utils.readable_time()))
            waitTime = 120 -  (datetime.now() - finishedUpToTime).seconds
            time.sleep(waitTime)
            time_slept+=waitTime
        else:
            filePath = DataProcessor.getInputPath(finishedUpToTime)
            helper_utils.std_flush("[%s] -- Trying to read %s"%(helper_utils.readable_time(), filePath))
            if not os.path.exists(filePath):
                waitTime = (datetime.now()-finishedUpToTime).total_seconds()
                #Difference is less than Two minutes
                if waitTime < 120:
                    waitTime = 120 - waitTime
                    time.sleep(waitTime)
                    time_slept+=waitTime
                else:
                    # Difference is more than two minutes - we can increment the the by one minute for the next ones
                    finishedUpToTime += TIME_DELTA_MINIMAL
            # Now we have file
            else:
                with open(filePath, 'r') as fileRead:
                    for line in fileRead:
                        try:
                            jsonVersion = json.loads(line)        
                        except ValueError as e:
                            helper_utils.std_flush("[%s] -- WARNING -- Possible warning for %s file for %s with error %s"%(helper_utils.readable_time(), filePath, importkey, str(e)))
                            continue

                        if "timestamp_ms" not in jsonVersion:
                            jsonVersion["timestamp_ms"] = int(jsonVersion["timestamp"])

                        if granularTime > int(jsonVersion["timestamp_ms"]):
                            # skip already finished this...
                            skip_count+=1
                            continue

                        else:
                            # Have not done this item yet...
                            # process
                            processed_data = DataProcessor.process(jsonVersion)
                            byted = bytes(json.dumps(processed_data), encoding="utf-8")
                            kafka_producer.send(kafka_key, byted)
                            kafka_producer.flush()
                            

                            granularTime = int(jsonVersion["timestamp_ms"])
                            r.set(importkey, granularTime)
                            process_count += 1
                            if granularTime - prevGranular > 86400000:
                                helper_utils.std_flush("[%s] -- Finished with %s"%(helper_utils.readable_time(), str(datetime.fromtimestamp(granularTime/1000.0))))
                                prevGranular = granularTime
                finishedUpToTime += TIME_DELTA_MINIMAL


if __name__ == "__main__":
    main()  #pylint: disable=no-value-for-parameter
