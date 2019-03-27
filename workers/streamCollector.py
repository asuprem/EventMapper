'''
This file iterates through tweets_unstructured_[YEAR]/ files and collates them to 
'''

import sys, time, pdb, os, json, codecs
from datetime import datetime, timedelta
import multiprocessing
from utils.file_utils import load_config
from utils.helper_utils import setup_pid, readable_time, std_flush
from utils.CONSTANTS import *



if __name__ == "__main__":
    #set up the PID for this
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    setup_pid(pid_name)


    #Load the keywords
    keywordConfig = load_config(TOPIC_CONFIG_PATH)
    errorQueue = multiprocessing.Queue()
    messageQueue = multiprocessing.Queue()

    keyStreamConfig = {}
    # for each keyword-lang pair type, launch a streamKeyProcessor
    for physicalEvent in keywordConfig['keyws_twitter'].keys():
        for language in keywordConfig['keyws_twitter'][physicalEvent]:
            eventLangTuple = (physicalEvent,language)
            keyStreamConfig[eventLangTuple] = {}
            keywordConfig[eventLangTuple]['name'] = physicalEvent
            keywordConfig[eventLangTuple]['lang'] = language
            keywordConfig[eventLangTuple]['keywords'] = configOriginal['keyws_twitter'][physicalEvent][language]

    #TODO launch the keywordConfig streamer
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
                    if eventLangTuple not in keywordConfig:
                        #new pair
                        keywordConfig[eventLangTuple] = {}
                        keywordConfig[eventLangTuple]['name'] = physicalEvent
                        keywordConfig[eventLangTuple]['keywords'] = configReload['keyws_twitter'][physicalEvent][language]
                        keywordConfig[eventLangTuple]['lang'] = language
                        if not configChangeFlag:
                            std_flush( "Changes have been made to Multiprocessing config file")
                            configChangeFlag = True
                        std_flush( "New event-language pair added: ", str(eventLangTuple))
                        std_flush( "   with keywords: ", str(keywordConfig[eventLangTuple]['keywords']))
                    else:
                        if keywordConfig[eventLangTuple]['keywords'] != configReload['keyws_twitter'][physicalEvent][language]:
                            if not configChangeFlag:
                                std_flush( "Changes have been made to Multiprocessing config file")
                                configChangeFlag = True
                            std_flush( "Keyword changes made to event-language pair: ", str(eventLangTuple))
                            std_flush( "    Old keywords: ", str(keywordConfig[eventLangTuple]['keywords']))
                            keywordConfig[eventLangTuple]['keywords'] = configReload['keyws_twitter'][physicalEvent][language]
                            std_flush( "    New keywords: ", str(keywordConfig[eventLangTuple]['keywords']))

            deleteEventLangTuples = []
            for eventLangTuple in keywordConfig:
                if eventLangTuple[0] not in configReload['keyws_twitter'].keys():
                    #This event type has been deleted
                    deleteEventLangTuples.append(eventLangTuple)
                else:
                    #Event type exists, but lanuage has been deleted
                    if eventLangTuple[1] not in configReload['keyws_twitter'][eventLangTuple[0]]:
                        deleteEventLangTuples.append(eventLangTuple)
            for eventLangTuple in deleteEventLangTuples:
                del keywordConfig[eventLangTuple]
                if not configChangeFlag:
                    std_flush( "Changes have been made to Multiprocessing config file")
                    configChangeFlag = True
                std_flush( "Deleted event-language pair: ", str(eventLangTuple))

            if configChangeFlag:
                #TODO Relaunch
            else:
                std_flush( "No changes have been made to Multiprocessing config file")


        while not errorQueue.empty():
            #TODO get error, time, restart
            

        while not messageQueue.empty():
            std_flush(messageQueue.get())



