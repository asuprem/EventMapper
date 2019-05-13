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
@click.argument("exportkey")
@click.argument("pidname")
def main(logdir, exportkey, pidname):
    TOP_OF_FILE_START = True
    pid_name = pidname
    helper_utils.setup_pid(pid_name, logdir=logdir)

    pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
    r=redis.Redis(connection_pool = pool)
    kafka_key = exportkey.replace(":","_")
    kafka_producer = kafka.KafkaProducer()

    # Get earliest file to parse...
    helper_utils.std_flush("Searching for files")
    finishedUpToTime = r.get(exportkey)
    granularTime = 0
    if finishedUpToTime is None:
        finishedUpToTime = 0
    else:
        finishedUpToTime = int(finishedUpToTime.decode())

    if finishedUpToTime == 0:
        # TODO CHANGE TO 7 days after setup is complete...
        helper_utils.std_flush("No value for previous stop. Starting from 7 days prior")
        currentTime = datetime.now() - timedelta(days=7)
        foundFlag = 0
        while foundFlag == 0:
            filePath = getInputPath(currentTime)
            if os.path.exists(filePath):
                #we found the most recent file, and increment our counter
                finishedUpToTime = currentTime
                foundFlag = 1
            else:
                #if our search is too broad - i.e. we are a month behind, ignore
                currentTime+=TIME_DELTA_MINIMAL
                timeDeltaOutputStream = (datetime.now() - currentTime)
                if timeDeltaOutputStream.days  == 0 and timeDeltaOutputStream.seconds  <= 1:
                    foundFlag = -1
    else:
        # I.E. if we already have a timestmap from pervious execution, we will read files that are a minute behind, and catch up to the granular time
        helper_utils.std_flush("Starting File tracking at %s"%str(datetime.fromtimestamp(finishedUpToTime/1000.0)))
        granularTime = finishedUpToTime
        finishedUpToTime = datetime.fromtimestamp(granularTime/1000.0) - timedelta(seconds = 60)
        TOP_OF_FILE_START = False
    if TOP_OF_FILE_START:
        # Otherwise, we start from the beginning of the 'first' file...
        finishedUpToTime -= timedelta(seconds=finishedUpToTime.second)
        granularTime = 0
    
    prevGranular = granularTime

    helper_utils.std_flush("Starting Stream Tracking for %s"%exportkey)
    while True:
        if (datetime.now() - finishedUpToTime).total_seconds() < 60:
            waitTime = 120 -  (datetime.now() - finishedUpToTime).seconds
            time.sleep(waitTime)
        else:
            filePath = getInputPath(finishedUpToTime)
            if not os.path.exists(filePath):
                waitTime = (datetime.now()-finishedUpToTime).total_seconds()
                #Difference is less than Two minutes
                if waitTime < 120:
                    waitTime = 120 - waitTime
                    time.sleep(waitTime)
                else:
                    # Difference is more than two minutes - we can increment the the by one minute for the next ones
                    finishedUpToTime += TIME_DELTA_MINIMAL
            # Not we have file
            else:
                with open(filePath, 'r') as fileRead:
                    for line in fileRead:
                        try:
                            jsonVersion = json.loads(line)        
                        except ValueError as e:
                            helper_utils.std_flush("Possible warning for %s file for %s with error %s"%(filePath, exportkey, str(e)))
                            continue
                        
                        if granularTime > int(jsonVersion["timestamp_ms"]):
                            # skip already finished this...
                            continue

                        else:
                            # Have not done this item yet...
                            # process
                            byted = bytes(json.dumps(extractTweet(jsonVersion)), encoding="utf-8")
                            kafka_producer.send(kafka_key, byted)
                            kafka_producer.flush()

                            granularTime = int(jsonVersion["timestamp_ms"])
                            r.set(exportkey, granularTime)
                            if granularTime - prevGranular > 86400000:
                                helper_utils.std_flush("Finished with %s"%(str(datetime.fromtimestamp(granularTime/1000.0))))
                                prevGranular = granularTime
                finishedUpToTime += TIME_DELTA_MINIMAL



def extractTweet(jsonVersion):
    write_version = {}
    write_version["id_str"] = jsonVersion["id_str"]
    write_version["text"] = jsonVersion["text"]
    write_version["location"] = jsonVersion["place"]["full_name"] if jsonVersion["place"] is not None else ""
    write_version["latitude"] = jsonVersion["coordinates"]["coordinates"][1] if jsonVersion["coordinates"] is not None else None
    write_version["longitude"] = jsonVersion["coordinates"]["coordinates"][0] if jsonVersion["coordinates"] is not None else None
    write_version["streamtype"] = jsonVersion["twitter"]
    write_version["timestamp"] = jsonVersion["timestamp"]
    write_version["link"] = "https://twitter.com/statuses/"+jsonVersion["id_str"]
    return write_version


    """
    producer.send("topic1", bytes("value312312324", encoding="utf-8"))
    consumer.close()
    >>> consumer=KafkaConsumer("topic1")
    >>> for msg in consumer:
    ...     print(msg.value)
    ...     time.sleep(5)
    # export as exportkey:indexeditem
    """

def getInputPath(_time):
    pathDir = os.path.join(DOWNLOAD_PREPEND + '%s_%s_%s_%s' % ('tweets', 'landslide','en', _time.year), '%02d' % _time.month,
                                        '%02d' % _time.day, '%02d' % _time.hour)
    filePath = os.path.join(pathDir, '%02d.json' % _time.minute)
    return filePath



if __name__ == "__main__":
    main()  #pylint: disable=no-value-for-parameter
