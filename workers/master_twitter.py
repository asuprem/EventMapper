# Genral imports
import sys, time, pdb, os, json, codecs
from datetime import datetime
from copy import deepcopy
from math import floor

# Multiprocessing import
import multiprocessing
from master_twitter_src.TweetProcess import TweetProcess
from master_twitter_src.KeyServer import KeyServer

# Utils import
from utils.file_utils import load_config
from utils.helper_utils import dict_equal, setup_pid, readable_time

CONFIG_PATH = 'config/multiprocess.json'
CONFIG_TIME_CHECK = 60*2

if __name__ == '__main__':
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    setup_pid(pid_name)
    #Set up configOriginal dict
    configOriginal = load_config(CONFIG_PATH)

    '''Error queue - This is the queue for errors; Each time process crashes, it will inform this queue'''
    errorQueue = multiprocessing.Queue()
    '''streamerConfig - streamerConfig'''
    streamerConfig = {}
    '''keyServer - determines which keys are assigned'''
    keyServer = KeyServer(configOriginal)

    
    '''Launch the Streamer with all keywords'''
    keywords = []
    APIKeys = keyServer.get_key()
    for physicalEvent in configOriginal['keyws_twitter'].keys():
        for language in configOriginal['keyws_twitter'][physicalEvent]:
            
            
            eventLangTuple = (physicalEvent,language)
            #print " ".join(["Deploying", physicalEvent,language, "at", readable_time()])
            streamerConfig[eventLangTuple] = {}
            streamerConfig[eventLangTuple]['name'] = physicalEvent
            streamerConfig[eventLangTuple]['keywords'] = configOriginal['keyws_twitter'][physicalEvent][language]
            streamerConfig[eventLangTuple]['lang'] = language
            keywords += streamerConfig[eventLangTuple]['keywords']

    
    tweetStreamer = TweetProcess(keywords,APIKeys[1],errorQueue)
    tweetStreamer.start()
    print " ".join(["Deployed",'unstructured streamer', "at", readable_time(),"with key", APIKeys[0]])
    configCheckTimer = time.time()
    while True:
        if time.time() - configCheckTimer > CONFIG_TIME_CHECK:

            configCheckTimer = time.time()
            print " ".join(["Checking configuration at", readable_time()])
            configReload = load_config(CONFIG_PATH)
            
            configChangeFlag = False
            keyServer.update(configReload)
            #First we check reloaded and for each changed, we replace
            for physicalEvent in configReload['keyws_twitter'].keys():
                for language in configReload['keyws_twitter'][physicalEvent]:
                    eventLangTuple = (physicalEvent,language)
                    if eventLangTuple not in streamerConfig:
                        #new pair
                        streamerConfig[eventLangTuple] = {}
                        streamerConfig[eventLangTuple]['name'] = physicalEvent
                        streamerConfig[eventLangTuple]['keywords'] = configReload['keyws_twitter'][physicalEvent][language]
                        streamerConfig[eventLangTuple]['lang'] = language
                        if not configChangeFlag:
                            print "Changes have been made to Multiprocessing config file"
                            configChangeFlag = True
                        print "New event-language pair added: ", str(eventLangTuple)
            deleteEventLangTuples = []
            for eventLangTuple in streamerConfig:
                if eventLangTuple[0] not in configReload['keyws_twitter'].keys():
                    #This event type has been deleted
                    deleteEventLangTuples.append(eventLangTuple)
                else:
                    #Event type exists, but lanuage has been deleted
                    if eventLangTuple[1] not in configReload['keyws_twitter'][eventLangTuple[0]]:
                        deleteEventLangTuples.append(eventLangTuple)
            for eventLangTuple in deleteEventLangTuples:
                del streamerConfig[eventLangTuple]
                if not configChangeFlag:
                    print "Changes have been made to Multiprocessing config file"
                    configChangeFlag = True
                print "Deleted event-language pair: ", str(eventLangTuple)

            if configChangeFlag:
                keywords = []
                for eventLangTuple in streamerConfig:
                    keywords += streamerConfig[eventLangTuple]['keywords']
                #relaunch
                try:
                    tweetStreamer.terminate()
                except:
                    pass
                tweetStreamer = TweetProcess(keywords,APIKeys[1],errorQueue)
                tweetStreamer.start()
                print " ".join(["Deployed",'unstructured streamer', "at", readable_time(),"with key", APIKeys[0]])
            else:
                print "No changes have been made to Multiprocessing config file"
                

        if int(floor (time.time() % 120)) < 2:
            print " ".join(["Checking crashes at", readable_time()])
        while not errorQueue.empty():
            
            _type, _error = errorQueue.get()
            print " ".join([_type, "crashed with error "]), error_
            print "        at ", readable_time()
            APIKeys = keyServer.refresh_key(APIKeys[0])
            try:
                tweetStreamer.terminate()
            except:
                pass
            tweetStreamer = TweetProcess(keywords,APIKeys[1],errorQueue)
            tweetStreamer.start()
    
            print " ".join(["Restarted", _type, "at" , readable_time()])
        time.sleep(5)






