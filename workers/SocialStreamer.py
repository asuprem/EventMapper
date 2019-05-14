# Genral imports
import sys, time, pdb, os, json, codecs
from datetime import datetime
from copy import deepcopy
from math import floor

# Multiprocessing import
import multiprocessing
from SocialStreamerSrc.TweetProcess import TweetProcess
from SocialStreamerSrc.KeyServer import KeyServer

# Utils import
from utils.file_utils import load_config
from utils.helper_utils import dict_equal, setup_pid, readable_time, std_flush
import utils.CONSTANTS as CONSTANTS

SOCIAL_STREAMER_FIRST_FILE_CHECK = True


if __name__ == '__main__':
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    setup_pid(pid_name)
    #Set up configOriginal dict
    configOriginal = load_config(CONSTANTS.TOPIC_CONFIG_PATH)
    Streamers = {}
    for _streamer_ in configOriginal["SocialStreamers"]:
        Streamers[_streamer_] = {}
        Streamers[_streamer_]["name"] = configOriginal["SocialStreamers"][_streamer_]["name"]
        Streamers[_streamer_]["type"] = configOriginal["SocialStreamers"][_streamer_]["type"]
        Streamers[_streamer_]["apikey"] = configOriginal["SocialStreamers"][_streamer_]["apikey"]
        Streamers[_streamer_]["apimax"] = configOriginal["SocialStreamers"][_streamer_]["apimax"]
        _scriptname = configOriginal["SocialStreamers"][_streamer_]["script"]
        moduleImport = __import__("SocialStreamerSrc.%s"%_scriptname, fromlist=[_scriptname])
        Streamers[_streamer_]["executor"] = getattr(moduleImport, _scriptname)
        Streamers[_streamer_]["keyserver"] = KeyServer(load_config(CONSTANTS.ASSED_CONFIG), key_mode=Streamers[_streamer_]["apikey"], key_max=Streamers[_streamer_]["apimax"])

    
    pdb.set_trace()

    '''Error queue - This is the queue for errors; Each time process crashes, it will inform this queue'''
    errorQueue = multiprocessing.Queue()
    messageQueue = multiprocessing.Queue()
    '''streamerConfig - streamerConfig'''
    streamerConfig = {}
    '''keyServer - determines which keys are assigned'''
    keyServer = KeyServer(load_config(CONSTANTS.ASSED_CONFIG))

    
    '''Launch the Streamer with all keywords'''
    keywords = []
    APIKeys = keyServer.get_key()
    for physicalEvent in configOriginal['topic_names'].keys():
        for language in configOriginal['topic_names'][physicalEvent]["languages"]:
            eventLangTuple = (physicalEvent,language)
            streamerConfig[eventLangTuple] = {}
            streamerConfig[eventLangTuple]['name'] = physicalEvent
            streamerConfig[eventLangTuple]['keywords'] = configOriginal['topic_names'][physicalEvent]["languages"][language]
            streamerConfig[eventLangTuple]['lang'] = language
            keywords += streamerConfig[eventLangTuple]['keywords']

    
    tweetStreamer = TweetProcess(keywords,APIKeys[1],errorQueue, messageQueue)
    tweetStreamer.start()
    std_flush(" ".join(["Deployed",'unstructured streamer', "at", readable_time(),"with key", APIKeys[0]]))
    configCheckTimer = time.time()
    fileCheckTimer = time.time()
    crashCheckInfoDumpTimer = time.time()
    while True:
        if time.time() - configCheckTimer > CONSTANTS.SOCIAL_STREAMER_CONFIG_TIME_CHECK:

            configCheckTimer = time.time()
            std_flush( " ".join(["Checking configuration at", readable_time()]))
            configReload = load_config(CONSTANTS.TOPIC_CONFIG_PATH)
            
            configChangeFlag = False
            keyServer.update(load_config(CONSTANTS.ASSED_CONFIG))
            #First we check reloaded and for each changed, we replace
            for physicalEvent in configReload['topic_names'].keys():
                for language in configReload['topic_names'][physicalEvent]["languages"]:
                    eventLangTuple = (physicalEvent,language)
                    if eventLangTuple not in streamerConfig:
                        #new pair
                        streamerConfig[eventLangTuple] = {}
                        streamerConfig[eventLangTuple]['name'] = physicalEvent
                        streamerConfig[eventLangTuple]['keywords'] = configReload['topic_names'][physicalEvent]["languages"][language]
                        streamerConfig[eventLangTuple]['lang'] = language
                        if not configChangeFlag:
                            std_flush( "Changes have been made to Multiprocessing config file")
                            configChangeFlag = True
                        std_flush( "New event-language pair added: ", str(eventLangTuple))
                        std_flush( "   with keywords: ", str(streamerConfig[eventLangTuple]['keywords']))
                    else:
                        if streamerConfig[eventLangTuple]['keywords'] != configReload['topic_names'][physicalEvent]["languages"][language]:
                            if not configChangeFlag:
                                std_flush( "Changes have been made to Multiprocessing config file")
                                configChangeFlag = True
                            std_flush( "Keyword changes made to event-language pair: ", str(eventLangTuple))
                            std_flush( "    Old keywords: ", str(streamerConfig[eventLangTuple]['keywords']))
                            streamerConfig[eventLangTuple]['keywords'] = configReload['topic_names'][physicalEvent]["languages"][language]
                            std_flush( "    New keywords: ", str(streamerConfig[eventLangTuple]['keywords']))

            deleteEventLangTuples = []
            for eventLangTuple in streamerConfig:
                if eventLangTuple[0] not in configReload['topic_names'].keys():
                    #This event type has been deleted
                    deleteEventLangTuples.append(eventLangTuple)
                else:
                    #Event type exists, but lanuage has been deleted
                    if eventLangTuple[1] not in configReload['topic_names'][eventLangTuple[0]]["languages"]:
                        deleteEventLangTuples.append(eventLangTuple)
            for eventLangTuple in deleteEventLangTuples:
                del streamerConfig[eventLangTuple]
                if not configChangeFlag:
                    std_flush( "Changes have been made to Multiprocessing config file")
                    configChangeFlag = True
                std_flush( "Deleted event-language pair: ", str(eventLangTuple))

            if configChangeFlag:
                keywords = []
                for eventLangTuple in streamerConfig:
                    keywords += streamerConfig[eventLangTuple]['keywords']
                #relaunch
                try:
                    tweetStreamer.terminate()
                except:
                    pass
                APIKeys = keyServer.refresh_key(APIKeys[0])
                tweetStreamer = TweetProcess(keywords,APIKeys[1],errorQueue, messageQueue)
                tweetStreamer.start()
                std_flush( " ".join(["Deployed",'unstructured streamer', "at", readable_time(),"with key", APIKeys[0]]))
            else:
                std_flush( "No changes have been made to Multiprocessing config file")

        #Crash checks        
        if time.time() - crashCheckInfoDumpTimer > CONSTANTS.SOCIAL_STREAMER_CRASH_TIME_CHECK:
            crashCheckInfoDumpTimer = time.time()
            std_flush( " ".join(["No crashes at", readable_time()]))


        #File write checks
        if time.time() - fileCheckTimer > CONSTANTS.SOCIAL_STREAMER_FILE_TIME_CHECK:
            fileCheckTimer = time.time()
            fileCheckCounter = 0
            
            pathPrepend = './downloads/'
            #we check last three files
            nowTime = datetime.now()
            if nowTime.minute < 4:
                #easiest error avoidance for backstop
                continue
            #range of minute files to check
            nowTimeMinute = [nowTime.minute - item for item in range(1,4)]
            pathDir = os.path.join(pathPrepend + '%s_%s_%s' % ('tweets', 'unstructured', nowTime.year), '%02d' % nowTime.month,
                                    '%02d' % nowTime.day, '%02d' % (nowTime.hour))
            for _minute in nowTimeMinute:
                fileName = os.path.join(pathDir, '%02d.json' % _minute)
                if not os.path.exists(fileName):
                    fileCheckCounter+=1
            if SOCIAL_STREAMER_FIRST_FILE_CHECK:
                #wait for next check
                std_flush( " ".join(["Unstructured downloader may not be creating files at",readable_time(), ". Waiting for next check."]))
                SOCIAL_STREAMER_FIRST_FILE_CHECK = False
            else:
                #Restart
                if fileCheckCounter == CONSTANTS.SOCIAL_STREAMER_FILE_CHECK_COUNT:
                    std_flush( " ".join(["Unstructured downloader no longer creating files at",readable_time()]))
                    APIKeys = keyServer.refresh_key(APIKeys[0])
                    try:
                        tweetStreamer.terminate()
                    except:
                        pass
                    tweetStreamer = TweetProcess(keywords,APIKeys[1],errorQueue, messageQueue)
                    tweetStreamer.start()
                    std_flush( " ".join(["Restarted unstructured streamer at" , readable_time()]))
                else:
                    std_flush( " ".join(["Unstructured downloader is creating files normally at",readable_time()]))
            

                    
        while not errorQueue.empty():
            
            _type, _details, _error = errorQueue.get()
            if _type == "unstructured":
                std_flush("UnstructuredStreamer Crash: %s crashed with error %s at %s"%(_details[0], _error, readable_time()))

                APIKeys = keyServer.refresh_key(APIKeys[0])
                try:
                    tweetStreamer.terminate()
                except:
                    pass
                tweetStreamer = TweetProcess(keywords,APIKeys[1],errorQueue, messageQueue)
                tweetStreamer.start()
                std_flush( " ".join(["Restarted", _type, "at" , readable_time()]))
            elif _type == "structured":
                std_flush("StructuredStreamer Crash: %s crashed with error %s at %s"%(_details[0], _error, readable_time()))
            
        while not messageQueue.empty():
            std_flush( messageQueue.get())
        #time.sleep(5)






