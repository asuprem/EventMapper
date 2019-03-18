# Genral imports
import sys, time, pdb, os, json, codecs
from datetime import datetime
from copy import deepcopy

# Multiprocessing import
import multiprocessing
from master_twitter_src.MasterProcess import MasterProcess
from master_twitter_src.KeyServer import KeyServer

# Utils import
from utils.file_utils import load_config
from utils.helper_utils import dict_equal

CONFIG_PATH = 'config/multiprocess.json'
CONFIG_TIME_CHECK = 60*60

if __name__ == '__main__':

    #Set up configOriginal dict
    configOriginal = load_config(CONFIG_PATH)
    physicalEventTypes = configOriginal['keyws_twitter'].keys()

    '''Error queue - This is the queue for errors; Each time process crashes, it will inform this queue'''
    errorQueue = multiprocessing.Queue()
    '''streamerConfig - streamerConfig'''
    streamerConfig = {}
    '''keyServer - determines which keys are assigned'''
    keyServer = KeyServer(configOriginal)

    


    for physicalEvent in physicalEventTypes:
        for language in configOriginal['keyws_twitter'][physicalEvent]:
            eventLangTuple = (physicalEvent,language)
            streamerConfig[eventLangTuple] = {}
            streamerConfig[eventLangTuple]['name'] = physicalEvent
            streamerConfig[eventLangTuple]['keywords'] = configOriginal['keyws_twitter'][physicalEvent][language]
            streamerConfig[eventLangTuple]['lang'] = language
            #keys is a tupe (keyStr, key)
            streamerConfig[eventLangTuple]['keys'] = keyServer.get_key()
            streamerConfig[eventLangTuple]['process'] = MasterProcess(configOriginal, physicalEvent, language, streamerConfig[eventLangTuple]['keywords'],streamerConfig[eventLangTuple]['keys'][1],errorQueue)
            streamerConfig[eventLangTuple]['process'].start()

    timer = time.time()
    while True:
        if time.time() - timer > CONFIG_TIME_CHECK:

            timer = time.time()
            print "Checking configOriginal at ", timer
            configReload = load_config(CONFIG_PATH)

            keyServer.update(configReload)

            if not (dict_equal(configOriginal, configReload)):
                print "Changes have been made to Multiprocessing config file"
                configDifference = {i: configReload[i] for i in set(configReload) - set(configOriginal)}
                print (configDifference)
                deltaPhysicalEvents = configDifference['keyws_twitter'].keys()
                for deltaEvent in deltaPhysicalEvents:
                    for language in configReload['keyws_twitter'][deltaEvent]:
                        deltaEventLangTuple = (deltaEvent,language)
                        print "changing configOriginal files"
                        streamerConfig[deltaEventLangTuple] = {}
                        streamerConfig[deltaEventLangTuple]['name'] = deltaEvent
                        streamerConfig[deltaEventLangTuple]['keywords'] = configReload['keyws_twitter'][deltaEvent][language]
                        streamerConfig[deltaEventLangTuple]['lang'] = language
                        streamerConfig[deltaEventLangTuple]['keys'] = keyServer.refresh_key(streamerConfig[deltaEventLangTuple]['keys'][0])
                        streamerConfig[deltaEventLangTuple]['process'].terminate()
                        streamerConfig[deltaEventLangTuple]['process'] = MasterProcess(configReload, deltaEvent, language,
                                                                              streamerConfig[deltaEventLangTuple]['keywords'],streamerConfig[deltaEventLangTuple]['keys'][1], errorQueue)
                        streamerConfig[deltaEventLangTuple]['process'].start()

                #TODO optimize this
                configOriginal = deepcopy(configReload)

            print "Finished with TIMER!"

        while not errorQueue.empty():
            eventName, lang, error_ = errorQueue.get()
            print (eventName,lang), " crashed with error ", error_
            #TODO TODO TODO have a try catch in case terminate causes problem
            streamerConfig[(eventName, lang)]['process'].terminate()
            print "Terminating ",eventName, " ", lang 
            streamerConfig[(eventName, lang)]['keys'] = keyServer.refresh_key(streamerConfig[(eventName, lang)]['keys'][0])
            streamerConfig[(eventName, lang)]['process'] = MasterProcess(configOriginal, eventName, lang, streamerConfig[(eventName,lang)]['keywords'], errorQueue)
            streamerConfig[(eventName, lang)]['process'].start()
            print "Restarted ", eventName, " ", lang






