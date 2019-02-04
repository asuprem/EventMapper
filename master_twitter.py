from twitter_download import streamStopper
import threading
import sys
import time
import os, json, codecs
from datetime import datetime

class masterThread(streamStopper):

    def __init__(self, config):
        threading.Thread.__init__(self)
        self.disaster_config = config
        #self.disaster_type = disaster_type


    def start_worker(self, config, disaster_type):

        threading.Thread.__init__(self)
        self.disaster_config = config
        self.disaster_type = disaster_type
        super(masterThread,self).streamer(disaster_config,disaster_type)

def load_config(config='config.js'):
    cf = json.load(codecs.open(config, encoding='utf-8'))
    return cf


config = load_config('../config.js')
landslide_thread = masterThread(config)
thread1 = threading.Thread(target=landslide_thread.start_worker, args=(config, "landslide"))
#thread2 = threading.Thread(target=masterThread.start_worker, args=(config, "wildfire"))

thread1.start()
#thread2.start()