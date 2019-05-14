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
    #setup_pid(pid_name)
    #Set up configOriginal dict
    configOriginal = load_config(CONSTANTS.TOPIC_CONFIG_PATH)
    StreamerManager = {}
    for _streamer_ in configOriginal["SocialStreamers"]:
        StreamerManager[_streamer_] = {}
        StreamerManager[_streamer_]["name"] = configOriginal["SocialStreamers"][_streamer_]["name"]
        StreamerManager[_streamer_]["type"] = configOriginal["SocialStreamers"][_streamer_]["type"]
        StreamerManager[_streamer_]["apikey_name"] = configOriginal["SocialStreamers"][_streamer_]["apikey"]
        StreamerManager[_streamer_]["apimax"] = configOriginal["SocialStreamers"][_streamer_]["apimax"]
        _scriptname = configOriginal["SocialStreamers"][_streamer_]["script"]
        moduleImport = __import__("SocialStreamerSrc.%s"%_scriptname, fromlist=[_scriptname])
        StreamerManager[_streamer_]["executor"] = getattr(moduleImport, _scriptname)
        StreamerManager[_streamer_]["keyserver"] = KeyServer(load_config(CONSTANTS.ASSED_CONFIG), key_mode=StreamerManager[_streamer_]["apikey_name"], key_max=StreamerManager[_streamer_]["apimax"])
        # Streamer specific instances
        if StreamerManager[_streamer_]["type"] == "unstructured":
            # Only single instance, with all keywords
            StreamerManager[_streamer_]["instance"] = None
            StreamerManager[_streamer_]["keywords"] = []
            StreamerManager[_streamer_]["apikey"] = None
        elif StreamerManager[_streamer_]["type"] == "structured":
            StreamerManager[_streamer_]["instances"] = {}
        else:
            raise ValueError("Invalid streamer type %s for SocialStreamer. Must be one of: unstructured | structured"%StreamerManager[_streamer_]["type"])

    
    pdb.set_trace()

    '''Error queue - This is the queue for errors; Each time process crashes, it will inform this queue'''
    errorQueue = multiprocessing.Queue()
    messageQueue = multiprocessing.Queue()
    '''streamerConfig - streamerConfig'''
    streamerConfig = {}

    
    '''Launch the Streamer with all keywords'''
    keywords = []
    for physicalEvent in configOriginal['topic_names'].keys():
        for language in configOriginal['topic_names'][physicalEvent]["languages"]:
            eventLangTuple = (physicalEvent,language)
            streamerConfig[eventLangTuple] = {}
            streamerConfig[eventLangTuple]['name'] = physicalEvent
            streamerConfig[eventLangTuple]['keywords'] = configOriginal['topic_names'][physicalEvent]["languages"][language]
            streamerConfig[eventLangTuple]['lang'] = language

            keywords += streamerConfig[eventLangTuple]['keywords']

        # for each allowed streamer, add the keywords
        for _allowed_streamer in configOriginal['topic_names'][physicalEvent]["social_source"]:
            # _allowed_streamer -- >twitter, facebook
            if not configOriginal['topic_names'][physicalEvent]["social_source"][_allowed_streamer]:
                continue
            # Streamer is valid...
            if StreamerManager[_allowed_streamer]["type"] == "unstructured":
                StreamerManager[_allowed_streamer]["keywords"] += keywords
            elif StreamerManager[_allowed_streamer]["type"] == "structured":
                #add an instance for each event lang tuple pair (but for our purposes, just english is enough for now...)
                for language in configOriginal['topic_names'][physicalEvent]["languages"]:
                    if language == "en":
                        eventLangTuple = (physicalEvent, language)
                        StreamerManager[_allowed_streamer]["instances"][eventLangTuple] = {}
                        StreamerManager[_allowed_streamer]["instances"][eventLangTuple]["keywords"] = streamerConfig[eventLangTuple]["keywords"]
                        StreamerManager[_allowed_streamer]["instances"][eventLangTuple]["instance"] = None
                        StreamerManager[_allowed_streamer]["instances"][eventLangTuple]["apikey"] = None
                    else:
                        pass
            else:
                raise ValueError("Invalid streamer type %s for SocialStreamer. Must be one of: unstructured | structured"%StreamerManager[_allowed_streamer]["type"])

    # Now we have a StreamerManager with empty instances for each streamer we are going to launch.
    # We will launch all of them, and go on from there...
    pdb.set_trace()
    for _streamer_ in StreamerManager:
        if StreamerManager[_streamer_]["type"] == "unstructured":
            #launch single unstructured streamer...
            StreamerManager[_streamer_]["apikey"] = StreamerManager[_streamer_]["keyserver"].get_key()
            StreamerManager[_streamer_]["instance"] = StreamerManager[_streamer_]["executor"](StreamerManager[_streamer_]["keywords"], StreamerManager[_streamer_]["apikey"][1], errorQueue, messageQueue)
            StreamerManager[_streamer_]["instance"].start()
            std_flush("Deployed unstructured streamer : %s\tat %s\twith key %s"%(StreamerManager[_streamer_]["name"], readable_time(), StreamerManager[_streamer_]["apikey"][0]))
        elif StreamerManager[_streamer_]["type"] == "structured":
            # Launch each instance (eventlangtuple)...
            for _instance_ in StreamerManager[_streamer_]["instances"]:
                StreamerManager[_streamer_]["instances"][_instance_]["apikey"] = StreamerManager[_streamer_]["keyserver"].get_key()
                StreamerManager[_streamer_]["instances"][_instance_]["instance"] = StreamerManager[_streamer_]["executor"](
                    _instance_[0],
                    _instance_[1],
                    StreamerManager[_streamer_]["instances"][_instance_]["keywords"],
                    StreamerManager[_streamer_]["instances"][_instance_]["apikey"][1], 
                    errorQueue, 
                    messageQueue)
                StreamerManager[_streamer_]["instances"][_instance_]["instance"].start()
                std_flush("Deployed structured streamer : %s for %s-%s\tat %s\twith key %s"%(StreamerManager[_streamer_]["name"], _instance_[0], _instance_[1], readable_time(), StreamerManager[_streamer_]["instances"][_instance_]["apikey"][0]))
        else:
            raise ValueError("Invalid streamer type %s for SocialStreamer. Must be one of: unstructured | structured"%StreamerManager[_streamer_]["type"])

    configCheckTimer = time.time()
    fileCheckTimer = time.time()
    crashCheckInfoDumpTimer = time.time()
    while True:
        if time.time() - configCheckTimer > CONSTANTS.SOCIAL_STREAMER_CONFIG_TIME_CHECK:

            configCheckTimer = time.time()
            std_flush( " ".join(["Checking configuration at", readable_time()]))
            configReload = load_config(CONSTANTS.TOPIC_CONFIG_PATH)
            
            configChangeFlag = False
            # Update all keyservers...
            for _streamer_ in StreamerManager:
                StreamerManager[_streamer_]["keyserver"].update(load_config(CONSTANTS.ASSED_CONFIG))

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
                        
                        # TODO add instance to StreamerManager() structured
                        if language == "en":
                            for _all_streamers_ in StreamerManager:
                                if StreamerManager[_all_streamers_]["type"] == "structured":
                                    # Pass because I don't want an extra level here for tasks. It's going on below in a 
                                    # couple lines after the continue
                                    pass
                                else:
                                    continue
                                # all_streamer_ is a structured streamer. we will add the instances...
                                if eventLangTuple not in StreamerManager[_all_streamers_]["instances"]:    
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple] = {}
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["keywords"] = streamerConfig[eventLangTuple]["keywords"]
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["instance"] = None
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"] = None

                                    # Now start the process here.
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"] = StreamerManager[_all_streamers_]["keyserver"].get_key()
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["instance"] = StreamerManager[_all_streamers_]["executor"](
                                        eventLangTuple[0],
                                        eventLangTuple[1],
                                        StreamerManager[_all_streamers_]["instances"][eventLangTuple]["keywords"],
                                        StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"][1], 
                                        errorQueue, 
                                        messageQueue)
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["instance"].start()
                                    std_flush("Deployed structured streamer : %s for %s-%s\tat %s\twith key %s"%(StreamerManager[_streamer_]["name"], eventLangTuple[0], eventLangTuple[1], readable_time(), StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"][0]))
                                else:
                                    # TODO handle weird situation if a new eventlangtuple is added but already exists in StreamerManager. 
                                    # Need to shut it down cleanly.
                                    pass

                    else:
                        if streamerConfig[eventLangTuple]['keywords'] != configReload['topic_names'][physicalEvent]["languages"][language]:
                            if not configChangeFlag:
                                std_flush( "Changes have been made to Multiprocessing config file")
                                configChangeFlag = True
                            std_flush( "Keyword changes made to event-language pair: ", str(eventLangTuple))
                            std_flush( "    Old keywords: ", str(streamerConfig[eventLangTuple]['keywords']))
                            streamerConfig[eventLangTuple]['keywords'] = configReload['topic_names'][physicalEvent]["languages"][language]
                            std_flush( "    New keywords: ", str(streamerConfig[eventLangTuple]['keywords']))
                            # TODO TODO ChangeStreamerManagerInstance()... structured

                            if language == "en":
                                for _all_streamers_ in StreamerManager:
                                    if StreamerManager[_all_streamers_]["type"] == "structured":
                                        # Pass because I don't want an extra level here for tasks. It's going on below in a 
                                        # couple lines after the continue
                                        pass
                                    else:
                                        continue
                                    # all_streamer_ is a structured streamer. we will add the instances...
                                    # First delete the eventlangtuple if it exists

                                    if eventLangTuple in StreamerManager[_all_streamers_]["instances"]:    
                                        try:
                                            StreamerManager[_all_streamers_]["instances"][eventLangTuple].terminate()
                                            std_flush("Shutdown structured streamer %s for deleted event-language pair %s-%s "%(_all_streamers_, eventLangTuple[0], eventLangTuple[1]))
                                        except:
                                            std_flush("Structured streamer %s for deleted event-language pair %s-%s is already shut down"%(_all_streamers_, eventLangTuple[0], eventLangTuple[1]))
                                        # TODO TODO TODO reemove keyserver
                                        StreamerManager[_all_streamers_]["keyserver"].abandon_key(StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"][0])
                                        del StreamerManager[_all_streamers_]["instances"][eventLangTuple]
                                        std_flush( "Removed structured streamer configuration for deleted event-language pair %s-%s "%(eventLangTuple[0], eventLangTuple[1]))
                                        # Not it no longer is in the StreamerManager...
                                    
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple] = {}
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["keywords"] = streamerConfig[eventLangTuple]["keywords"]
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["instance"] = None
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"] = None

                                    # Now start the process here.
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"] = StreamerManager[_all_streamers_]["keyserver"].get_key()
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["instance"] = StreamerManager[_all_streamers_]["executor"](
                                        eventLangTuple[0],
                                        eventLangTuple[1],
                                        StreamerManager[_all_streamers_]["instances"][eventLangTuple]["keywords"],
                                        StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"][1], 
                                        errorQueue, 
                                        messageQueue)
                                    StreamerManager[_all_streamers_]["instances"][eventLangTuple]["instance"].start()
                                    std_flush("Deployed structured streamer : %s for %s-%s\tat %s\twith key %s"%(StreamerManager[_streamer_]["name"], eventLangTuple[0], eventLangTuple[1], readable_time(), StreamerManager[_all_streamers_]["instances"][eventLangTuple]["apikey"][0]))
                                    
                                    
                            # Terminate the existing process and restart...
                            # Hopefully this won't cause issues with the key access per day...

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

                # Shutdown structured streamer that matched this eventLangTuple
                for _streamer_ in StreamerManager:
                    if StreamerManager[_streamer_]["type"] == "structured":
                        if eventLangTuple in StreamerManager[_streamer_]["instances"]:
                            try:
                                StreamerManager[_streamer_]["instances"][eventLangTuple].terminate()
                                std_flush("Shutdown structured streamer %s for deleted event-language pair %s-%s "%(_streamer_, eventLangTuple[0], eventLangTuple[1]))
                            except:
                                std_flush("Structured streamer %s for deleted event-language pair %s-%s is already shut down"%(_streamer_, eventLangTuple[0], eventLangTuple[1]))
                            # TODO TODO TODO reemove keyserver
                            StreamerManager[_streamer_]["keyserver"].abandon_key(StreamerManager[_streamer_]["instances"][eventLangTuple]["apikey"][0])
                            del StreamerManager[_streamer_]["instances"][eventLangTuple]
                            std_flush( "Removed structured streamer configuration for deleted event-language pair %s-%s "%(eventLangTuple[0], eventLangTuple[1]))
                            


            if configChangeFlag:
                # reset keywords for unstructured streamer
                for _allowed_streamer_ in StreamerManager:
                    if StreamerManager[_allowed_streamer_]["type"] == "unstructured":
                        StreamerManager[_allowed_streamer_]["keywords"] = []
                        for eventLangTuple in streamerConfig:
                            StreamerManager[_allowed_streamer_]["keywords"] += streamerConfig[eventLangTuple]['keywords']

                        #relaunch the unstructured streamer...
                        try:
                            StreamerManager[_allowed_streamer_]["instance"].terminate()
                        except:
                            pass
                        #APIKeys = keyServer.refresh_key(APIKeys[0])
                        StreamerManager[_allowed_streamer_]["instance"] = StreamerManager[_allowed_streamer_]["executor"](StreamerManager[_allowed_streamer_]["keywords"], StreamerManager[_allowed_streamer_]["apikey"][1], errorQueue, messageQueue)
                        StreamerManager[_allowed_streamer_]["instance"].start()
                        std_flush("Deployed unstructured streamer : %s\tat %s\twith key %s"%(StreamerManager[_allowed_streamer_]["name"], readable_time(), StreamerManager[_allowed_streamer_]["apikey"][0]))
            else:
                std_flush( "No changes have been made to Multiprocessing config file")

        #Crash checks        
        if time.time() - crashCheckInfoDumpTimer > CONSTANTS.SOCIAL_STREAMER_CRASH_TIME_CHECK:
            crashCheckInfoDumpTimer = time.time()
            std_flush( " ".join(["No crashes at", readable_time()]))

        # TODO TODO TODO move file checker inside the Streamer itself
        """
        #File write checks for unstructured streamer...
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
        """

                    
        while not errorQueue.empty():
            
            _type, _details, _error = errorQueue.get()
            if _type == "unstructured":
                std_flush("UnstructuredStreamer Crash: %s crashed with error %s at %s"%(_details[0], _error, readable_time()))                
                # Releaunch... 
                try:
                    StreamerManager[_details[0]]["instance"].terminate()
                    std_flush("Terminated possible zombie unstructured streamer : %s\tat %s"%(StreamerManager[_details[0]]["name"], readable_time()))
                except:
                    std_flush("Termination of possible zombie unstructured streamer : %s\t FAILED at %s. Possibly already dead."%(StreamerManager[_details[0]]["name"], readable_time()))
                #APIKeys = keyServer.refresh_key(APIKeys[0])
                StreamerManager[_details[0]]["instance"] = StreamerManager[_details[0]]["executor"](StreamerManager[_details[0]]["keywords"], StreamerManager[_details[0]]["apikey"][1], errorQueue, messageQueue)
                StreamerManager[_details[0]]["instance"].start()
                std_flush("Restarted unstructured streamer : %s\tat %s\twith key %s"%(StreamerManager[_details[0]]["name"], readable_time(), StreamerManager[_details[0]]["apikey"][0]))
                


            elif _type == "structured":
                eventLangTuple = (_details[1], _details[2])
                std_flush("Structured Streamer Crash: %s streamer for %s-%s crashed with error %s at %s"%(_details[0], _details[1], _details[2], _error, readable_time()))
                # Releaunch... 
                try:
                    StreamerManager[_details[0]]["instances"][eventLangTuple]["instance"].terminate()
                    std_flush("Terminated possible zombie structured streamer : %s for %s-%s \tat %s"%(_details[0],_details[1], _details[2], readable_time()))
                except:
                    std_flush("Termination of possible zombie structured streamer : %s for %s-%s FAILED \tat %s"%(_details[0],_details[1], _details[2], readable_time()))

                #APIKeys = keyServer.refresh_key(APIKeys[0])
                StreamerManager[_details[0]]["instances"][eventLangTuple]["instance"] = StreamerManager[_details[0]]["executor"](
                                        _details[1],
                                        _details[2],
                                        StreamerManager[_details[0]]["instances"][eventLangTuple]["keywords"],
                                        StreamerManager[_details[0]]["instances"][eventLangTuple]["apikey"][1], 
                                        errorQueue, 
                                        messageQueue)
                StreamerManager[_details[0]]["instances"][eventLangTuple]["instance"].start()
                std_flush("Restarted structured streamer : %s for %s-%s\tat %s\twith key %s"%(StreamerManager[_details[0]]["name"], _details[1], _details[2], readable_time(), StreamerManager[_details[0]]["instances"][eventLangTuple]["apikey"][0]))
            
        while not messageQueue.empty():
            std_flush( messageQueue.get())
        #time.sleep(5)






