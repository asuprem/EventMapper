'''
This file iterates through tweets_unstructured_[YEAR]/ files and collates them to 
'''

import sys, time, pdb, os, json, codecs
from datetime import datetime, timedelta
import multiprocessing
from utils.file_utils import load_config
from utils.helper_utils import setup_pid, readable_time, std_flush
from utils.CONSTANTS import *

from stream_collector_src.StreamFilesProcessor import StreamFilesProcessor


if __name__ == "__main__":
    #set up the PID for this
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    setup_pid(pid_name)


    #Load the keywords
    keywordConfig = load_config(TOPIC_CONFIG_PATH)
    errorQueue = multiprocessing.Queue()
    messageQueue = multiprocessing.Queue()

    keyStreamConfig = {}
    # for each keyword-lang pair type, launch a StreamFilesProcessor
    for physicalEvent in keywordConfig['keyws_twitter'].keys():
        for language in keywordConfig['keyws_twitter'][physicalEvent]:
            eventLangTuple = (physicalEvent,language)
            keyStreamConfig[eventLangTuple] = {}
            keyStreamConfig[eventLangTuple]['name'] = physicalEvent
            keyStreamConfig[eventLangTuple]['lang'] = language
            keyStreamConfig[eventLangTuple]['keywords'] = keywordConfig['keyws_twitter'][physicalEvent][language]
            keyStreamConfig[eventLangTuple]['postpone'] = False
            std_flush(" ".join(["Deploying",str(eventLangTuple), "at", readable_time()]))
            try:
                keyStreamConfig[eventLangTuple]['processor'] = StreamFilesProcessor(  None, 
                                                                                keyStreamConfig[eventLangTuple]['keywords'], 
                                                                                "_".join([physicalEvent,language]), 
                                                                                errorQueue,
                                                                                messageQueue, 
                                                                                SOCIAL_STREAMER_FILE_CHECK_COUNT )
            except AssertionError:
                std_flush(" ".join([str(eventLangTuple), " does not have files to start. Posponing launch 2 hr at", readable_time()]))
                keyStreamConfig[eventLangTuple]['postpone'] = True
            keyStreamConfig[eventLangTuple]['launchTime'] = datetime.now()
            #TODO launch the File Processor

            if not keyStreamConfig[eventLangTuple]['postpone']:
                keyStreamConfig[eventLangTuple]['processor'].start()

    configCheckTimer = time.time()

    while True:
        if time.time() - configCheckTimer > STREAM_COLLECTOR_CONFIG_TIME_CHECK:

            configCheckTimer = time.time()
            std_flush( " ".join(["Checking configuration at", readable_time()]))
            configReload = load_config(TOPIC_CONFIG_PATH)
            
            configChangeFlag = False
            #First we check reloaded and for each changed, we replace
            for physicalEvent in configReload['keyws_twitter'].keys():
                for language in configReload['keyws_twitter'][physicalEvent]:
                    eventLangTuple = (physicalEvent,language)
                    if eventLangTuple not in keyStreamConfig:
                        #new pair
                        
                        keyStreamConfig[eventLangTuple] = {}
                        keyStreamConfig[eventLangTuple]['name'] = physicalEvent
                        keyStreamConfig[eventLangTuple]['keywords'] = configReload['keyws_twitter'][physicalEvent][language]
                        keyStreamConfig[eventLangTuple]['lang'] = language
                        keyStreamConfig[eventLangTuple]['postpone'] = False
                        if not configChangeFlag:
                            std_flush( "Changes have been made to Multiprocessing config file")
                            configChangeFlag = True
                        std_flush( "New event-language pair added: ", str(eventLangTuple))
                        std_flush( "   with keywords: ", str(keyStreamConfig[eventLangTuple]['keywords']))
                    else:
                        if keyStreamConfig[eventLangTuple]['keywords'] != configReload['keyws_twitter'][physicalEvent][language]:
                            if not configChangeFlag:
                                std_flush( "Changes have been made to Multiprocessing config file")
                                configChangeFlag = True
                            std_flush( "Keyword changes made to event-language pair: ", str(eventLangTuple))
                            std_flush( "    Old keywords: ", str(keyStreamConfig[eventLangTuple]['keywords']))
                            keyStreamConfig[eventLangTuple]['keywords'] = configReload['keyws_twitter'][physicalEvent][language]
                            std_flush( "    New keywords: ", str(keyStreamConfig[eventLangTuple]['keywords']))

            deleteEventLangTuples = []
            for eventLangTuple in keyStreamConfig:
                if eventLangTuple[0] not in configReload['keyws_twitter'].keys():
                    #This event type has been deleted
                    deleteEventLangTuples.append(eventLangTuple)
                else:
                    #Event type exists, but lanuage has been deleted
                    if eventLangTuple[1] not in configReload['keyws_twitter'][eventLangTuple[0]]:
                        deleteEventLangTuples.append(eventLangTuple)
            for eventLangTuple in deleteEventLangTuples:
                del keyStreamConfig[eventLangTuple]
                if not configChangeFlag:
                    std_flush( "Changes have been made to Multiprocessing config file")
                    configChangeFlag = True
                std_flush( "Deleted event-language pair: ", str(eventLangTuple))

            if configChangeFlag:
                #TODO Relaunch
                try:
                    keyStreamConfig[eventLangTuple]['processor'].terminate()
                except:
                    pass
                std_flush(" ".join(["Shutdown",str(eventLangTuple), "at", readable_time()]))
                std_flush(" ".join(["Redeploying",str(eventLangTuple), "due to configChange at", readable_time()]))
                try:
                    keyStreamConfig[eventLangTuple]['processor'] = StreamFilesProcessor(  None, 
                                                                                    keyStreamConfig[eventLangTuple]['keywords'], 
                                                                                    "_".join([physicalEvent,language]), 
                                                                                    errorQueue,
                                                                                    messageQueue, 
                                                                                    SOCIAL_STREAMER_FILE_CHECK_COUNT )
                except AssertionError:
                    std_flush(" ".join([str(eventLangTuple), " does not have files to start. Posponing launch 2 hr at", readable_time()]))
                    keyStreamConfig[eventLangTuple]['postpone'] = True
                keyStreamConfig[eventLangTuple]['launchTime'] = datetime.now()
                #TODO launch the File Processor

                if not keyStreamConfig[eventLangTuple]['postpone']:
                    keyStreamConfig[eventLangTuple]['processor'].start()
            else:
                std_flush( "No changes have been made to Multiprocessing config file")


        while not errorQueue.empty():
            #TODO get error, time, restart
            _rootName, _error = errorQueue.get()
            std_flush(" ".join([_rootName, "crashed with error: ", str(_error)]))
            
            eLT = _rootName.split("_")
            eventLangTuple = (eLT[0], eLT[1])
            try:
                keyStreamConfig[eventLangTuple]['processor'].terminate()
            except:
                pass
            std_flush(" ".join(["Shutdown",str(eventLangTuple), "at", readable_time()]))
            std_flush(" ".join(["Redeploying",str(eventLangTuple), "at", readable_time()]))
            try:
                keyStreamConfig[eventLangTuple]['processor'] = StreamFilesProcessor(  None, 
                                                                                keyStreamConfig[eventLangTuple]['keywords'], 
                                                                                "_".join([physicalEvent,language]), 
                                                                                errorQueue,
                                                                                messageQueue, 
                                                                                SOCIAL_STREAMER_FILE_CHECK_COUNT )
                keyStreamConfig[eventLangTuple]['postpone'] = False
            except AssertionError:
                std_flush(" ".join([str(eventLangTuple), " does not have files to start. Posponing launch 2 hr at", readable_time()]))
                keyStreamConfig[eventLangTuple]['postpone'] = True
            keyStreamConfig[eventLangTuple]['launchTime'] = datetime.now()
            #TODO launch the File Processor

            if not keyStreamConfig[eventLangTuple]['postpone']:
                keyStreamConfig[eventLangTuple]['processor'].start()

        for eventLangTuple in keyStreamConfig:
            if keyStreamConfig[eventLangTuple]["postpone"]:
                if (datetime.now() - keyStreamConfig[eventLangTuple]["launchTime"]).seconds  > STREAM_PROCESSOR_POSTPONE_SECONDS:
                    std_flush(" ".join(["Attempting relaunch of", str(eventLangTuple), " after postponement at", readable_time()]))
                    #We can try to relaunch. maybe files have been created by this point
                    try:
                        keyStreamConfig[eventLangTuple]['processor'].terminate()
                    except:
                        pass
                    std_flush(" ".join(["Shutdown",str(eventLangTuple), "at", readable_time()]))
                    std_flush(" ".join(["Redeploying",str(eventLangTuple), "at", readable_time()]))
                    try:
                        keyStreamConfig[eventLangTuple]['processor'] = StreamFilesProcessor(  None, 
                                                                                        keyStreamConfig[eventLangTuple]['keywords'], 
                                                                                        "_".join([physicalEvent,language]), 
                                                                                        errorQueue,
                                                                                        messageQueue, 
                                                                                        SOCIAL_STREAMER_FILE_CHECK_COUNT )
                        keyStreamConfig[eventLangTuple]['postpone'] = False
                    except AssertionError:
                        std_flush(" ".join([str(eventLangTuple), " does not have files to start. Posponing launch 2 hr at", readable_time()]))
                        keyStreamConfig[eventLangTuple]['postpone'] = True
                    keyStreamConfig[eventLangTuple]['launchTime'] = datetime.now()
                    #TODO launch the File Processor

                    if not keyStreamConfig[eventLangTuple]['postpone']:
                        keyStreamConfig[eventLangTuple]['processor'].start()

            

        while not messageQueue.empty():
            std_flush(messageQueue.get())



