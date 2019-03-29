import sys, time, os, json, codecs
from datetime import datetime

from utils.helper_utils import readable_time
from utils.file_utils import load_config, setup_pid, std_flush
from utils.CONSTANTS import *


if __name__ == '__main__':

    startTime = time.time()
    configOriginal = load_config(TOPIC_CONFIG_PATH)

    # Launch Conitinuous General News Streamer
    #   News Tweets Downloads into news_tweets_2019/
    cnt = ContinuousNewsStreamer()
    cnt.start()
    
    # TODO Develop Continuous News Crawler
    #   Download into news_crawler_2019/
    crwl = ContinuousNewsCrawler()
    crwl.start()
    
    # Launch Scheduled General News Streamer(s)
    #   Use NewsDownloader to perform general download into news_unstructured_2019/
    sgns = ScheduledGeneralNewsStreamer()
    sgns.start()


    # Launch Continuous Specific Source Streamer(s)
    #   download into whatever they download into
    #   HighConf Streamer manages all these
    for topics in highConfidenceContinuousConfig:
        if execType == "subprocess":
            subprocess [env]/bin/python workers/HighConfSpecSourceStreams/execName.py >> [logfile].log
            # subprocess python needs API acceess to create PID file
            # We will check PID once in a while
            # API interface access from ASSED
            """
                createPID()
                saveData(data)
                setUpGranulatity(def = 60)
                internals
                    .granularity = 60s
                    .pathBaseName = trmm, noaaRain, etc

            """
        #if execType == "class":



    # Launch Scheduled Specific Source Streamer(s)
    #   download into whatever they download into
    #   HighConf Streamer manages all these every once in a while
    for topics in highConfidenceScheduledConfig:
        if execType == "subprocess":
            subprocess [env]/bin/python workers/HighConfSpecSourceStreams/execName.py >> [logfile].log
            # subprocess python needs API acceess to create PID file
            # We will check PID and relaunch @ frequency if PID is not alive
            # API interface access from ASSED
            """
                createPID()
                saveData(data)
                setUpGranulatity(def = 60)
                internals
                    .pathBaseName = trmm, noaaRain, etc

            """
        #if execType == "class":



    # Get the eventlang pairs 
    for physicalEvent in configOriginal['topic_names'].keys():
        if configOriginal['topic_names'][physicalEvent]["high_confidence"]["valid"] == 0:
            continue
        for language in configOriginal['topic_names'][physicalEvent]["languages"]:
            # extract source_file for current diasters
            source_data = {}
            source_list = config['keyws'][keyword]['news_source']
            # extract sources from source list
            with open ('../'+source_list, 'r') as sources:
                for line in sources:
                    lines_json = json.loads(line)
                    source_data[lines_json['name']] = lines_json
            
            # now we have a list of sources
            #for eaach source, we'll import the neccesary library and use the getFeeds to get the titles...?
            write_data = {}
            for source in source_data:
                #source is the name field
                #if source_data[source]['library'] == "landslide":
                #so this does 'from news_downloaders.google import google_downloader <-- google_downloader is the class'
                print "Retrieving: ", source, "       using library: ", source_data[source]['library'], " on ", datetime.now()
                feed_reader = __import__('news_downloaders.' + source_data[source]['library'],fromlist=[source_data[source]['library']+'_downloader'])
                feed_reader = getattr(feed_reader,source_data[source]['library']+'_downloader')
                feed_read = feed_reader(config)
                data = feed_read.get(source_data[source]['url'], params=source_data[source]['parameters'])
                #here we clear save the data:
                #no need for time check I think

                #create directory
                date = datetime.utcnow()
                dir_ = os.path.join('%s_%s_%s' % ('../downloads/news', keyword, date.year), '%02d' % date.month, '%02d' %date.day, '%02d' % date.hour)
                path = os.path.join(dir_, '%02d.json' % date.minute)
                #create directory if it doesn't exist yet
                try:
                    os.makedirs(dir_)
                except:
                    pass
                #bugfix got rid of empty files yay!
                output = open(path, 'a')
                data_write = '\n'.join([json.dumps(data[item]) for item in data])
                output.write(data_write)
                output.close()


