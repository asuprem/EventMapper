# -*- coding: utf-8 -*-
"""FacebookProcess  script

This will launch a twitter streamer for all of twitter
"""

import multiprocessing
import time, os, sys, json
from datetime import datetime
import redis
# General tweepy imports
from apiclient.discovery import build
from utils.helper_utils import readable_time

class TweetProcess(multiprocessing.Process):
    """A Class for a process for a multiprocessing application.

    Args:
        config (dict): A dictionary containing config details. Returned by load_config in file_utils. Config file contains streamer details.
        name (str): Name of this process. Usually a key field from config file.
        lang (str): Streamer language. Used for creating output directory structure.
        keywords (list[str]): List of keywords for  streamer. For facebook, we only take the first keyword because we are poor :)
        physical_event -   Name of disaster. Extensible to event_type.
    """

    def __init__(self, event, lang, keywords, apiKeys, errorQueue, messageQueue):
        """ Initializes the Facebookprocess class.
        
        """
        multiprocessing.Process.__init__(self)

        self.local_time = time.time()
        self.date = datetime.now()
        self.PREPEND = './downloads/'
        self.event = event
        self.lang = lang

         #Creates flush path
        self.dir = os.path.join(self.PREPEND + '%s_%s_%s_%s' % ('facebook', event, lang, self.date.year), '%02d' % self.date.month,
                                '%02d' % self.date.day, '%02d' % self.date.hour)
        self.path = os.path.join(self.dir, '%02d.json' % self.date.minute)
        #Error checking for directory create
        try:
            os.makedirs(self.dir)
        except:
            pass
        #Create write path
        self.output = open(self.path, 'w')

        self.TIMER = 60
        self.timer = time.time()
        self.keywords = keywords[0]
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        self.r = redis.Redis(connection_pool=pool)
        
        
        self.apikey = apiKeys[0]
        self.cx = apiKeys[1]

        self.service = build("customsearch","v1",developerKey=self.apikey)
    def path_setup(self):
            """path_setup()

            Function to set up a path.
            """
            self.local_time = time.time()
            self.date = datetime.now()
            self.dir = os.path.join(self.PREPEND + '%s_%s_%s_%s' % ('facebook', self.event, self.lang, self.date.year), '%02d' % self.date.month,
                                    '%02d' % self.date.month,
                                    '%02d' % self.date.day, '%02d' % self.date.hour)
            self.path = os.path.join(self.dir, '%02d.json' % self.date.minute)
            try:
                os.makedirs(self.dir)
            except:
                pass
            self.output.close()
            self.output = open(self.path, 'w')

    def run(self):
        """Run - Launches the sreamer itself.

        """
        # Perform work ONLY if it hasn't been performed today...
        previousTimestamp = self.r.get("social:streamer:facebook:timestamp")
        previousApiAccesses = self.r.get("social:streamer:facebook:count")
        if previousApiAccesses is None:
            previousApiAccesses = 0
        if previousTimestamp is not None:
            previousTimestamp = int(previousTimestamp)
            if datetime.fromtimestamp(previousTimestamp).day != datetime.fromtimestamp(time.time()):
                self.messageQueue.put("Initiating facebook download of %s-%s at %s"%(self.event, self.lang, readable_time()))
                max_results = 10
                for page_get in range(9):
                    start_param = page_get*10 + 1
                    if start_param > max_results:
                        continue
                    results = self.service.cse().list(q=self.keywords,cx=self.cx,dateRestrict='d1',siteSearch='www.facebook.com',siteSearchFilter='i', start=start_param).execute()
                    max_results = results["searchInformation"]["totalResults"]

                    previousApiAccesses+=1
                    self.r.set("social:streamer:facebook:count", previousApiAccesses)

                    try:
                        if time.time() - self.local_time > self.TIMER:
                            self.path_setup()
                        
                        for _item_ in results["items"]:
                            _data_ = {}
                            #-20 for the varchar limit...
                            _data_["id_str"] = _item_["link"][-20:]
                            _data_["text"] = _item_["snippet"]
                            _data_["location"] = ""
                            _data_["latitude"] = None
                            _data_["longitude"] = None
                            _data_["streamtype"] = "facebook"
                            _data_["timestamp"] = time.time()*1000
                            _data_["link"] = _item_["link"]
                            self.output.write(json.dumps(_data_)+"\n")

                    except Exception as e:
                        self.errorQueue.put(("structured",("facebook", self.event, self.lang), str(e)))

                    self.messageQueue.put("Completed facebook download of %s-%s part %i of 10 at %s"%(self.event, self.lang, page_get+1, readable_time()))


            else:
                # we already done...
                self.messageQueue.put("facebook download of %s-%s at is already complete for day %s"%(self.event, self.lang, str(datetime.fromtimestamp(time.time()).day)))
            self.messageQueue.put("Completed facebook download of %s-%s at time %s"%(self.event, self.lang, readable_time()))


