# -*- coding: utf-8 -*-
"""MasterProcess multiprocessor script

This class is a Process class for a Multiprocessing application. Each process runs a streamer application that connects to a social media network and downloads data.

"""

import multiprocessing
import time, os, sys, json
from datetime import datetime

# General tweepy imports
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from utils.CONSTANTS import *
from utils.helper_utils import readable_time

class MasterProcess(multiprocessing.Process):
    """A Class for a process for a multiprocessing application.

    Args:
        config (dict): A dictionary containing config details. Returned by load_config in file_utils. Config file contains streamer details.
        name (str): Name of this process. Usually a key field from config file.
        lang (str): Streamer language. Used for creating output directory structure.
        keywords (list[str]): List of keywords for generic streamer. Used in Twitter streaming. Planned extensibility to other streamers

    """

    def __init__(self, config, physical_event, lang, keywords, apiKeys, errorQueue):
        #Initializes this class as a child of Multiprocessing.Process
        multiprocessing.Process.__init__(self)

        """Variable Details

            self.config -           Entire config dict
            self.timer -            Used to create schedule flushing. Every N seconds, all intermediate data is flushed to disk.
            self.physical_event -    Name of disaster. Extensible to event_type.
            self.lang -             Language of streamer downloads
            self.keywords -         List of streaming keywords for physical event type    
        
        """
        self.config = config
        self.timer = time.time()
        self.physical_event = physical_event
        self.lang = lang
        self.keywords = keywords
        self.errorQueue = errorQueue


        """ The next four are API keys.
            TODO --> Change to streamer specific

            self.auth are the OAuth Handlers for API keys for Tweepy; TODO --> Modify to be streamer specific
            self.stream is the Stream function. TODO --> Modify to be streamer specific

            self.time - current time
        """            

        self.access_token_secret = apiKeys[0]
        self.access_token = apiKeys[1]
        self.consumer_key = apiKeys[2]
        self.consumer_secret = apiKeys[3]

        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.stream = Stream(self.auth, self.streamStopper(self.physical_event, self.lang))

    def run(self):
        """Run - Launches the sreamer itself.

        """
        # TODO change print to logging; use logging function/API
        try:
            #print "entered" + " " + self.physical_event + " "+ self.lang
            #TODO have changed from json_array to self.keywords
            #print json_array
            self.stream.filter(track=self.keywords)
            print(" ".join(["Running", self.physical_event,self.lang, "with PID", str(os.getpid()), "at", readable_time()]))
            pid = str(os.getpid())
        except Exception as e:
            print('------------')
            print(" ".join(["Crashed", self.physical_event,self.lang, "at", readable_time()]))
            print(e)
            print('------------')
            #TODO TODO TODO CHECK THIS
            self.errorQueue.put((self.physical_event, self.lang, e))



    class streamStopper(StreamListener):
        """A Class for a Streamer for a multiprocess application.

        Args:
            physical_event (str):    A string with the name of the physical_event. Used for .
            lang (str): Language for this physical event social streamer

        """
        def __init__(self, physical_event, lang):
            """Variables

                    TIMER (const int)       This is a scheduler. It is the number of seconds between scheduled file flushes to disk.
                    local_time (time)       Time for the streamer. Use to schedule disk flushes
                    date (time)             Current date. Used for flush directory (each download is sent to a day/hour/minute folder)
                    physical_event (str)     The name of physical event. used to create top level name for flush folder
                    lang (str)              Language of this physical event streamer. Used to create top level nanme for flush folder
                    output (PATH)           This is the flush path. Each data item is written to this file buffer.

            """
            self.TIMER = STREAMING_GRANULARITY_SECONDS
            self.local_time = time.time()
            self.date = datetime.now()
            self.physical_event = physical_event
            self.lang = lang
            self.PREPEND = './downloads/'

            #Creates flush path
            self.dir = os.path.join(self.PREPEND + '%s_%s_%s_%s' % ('tweets', self.physical_event, self.lang, self.date.year), '%02d' % self.date.month,
                                    '%02d' % self.date.day, '%02d' % self.date.hour)
            self.path = os.path.join(self.dir, '%02d.json' % self.date.minute)
            
            #Error checking for directory create
            try:
                os.makedirs(self.dir)
            except:
                pass

            #Create write path
            self.output = open(self.path, 'w')


        def on_data(self, data):
            """on_data - This function is an interface in Tweepy's streamers. It is activated on each data item"""

            """ Operation

            Each time it is activated, on_data checks if TIMER has been surpassed. If it has, on_data creates a new file with the current time (because on_data flushes every TIMER seconds). This new file is what on_data operates on.

            If TIMER has not been surpassed, on_data writes data to self.output, which is the flush path
            """
            try:
                #check TIMER
                if time.time() - self.local_time > self.TIMER:
                    self.path_setup()
                sample = json.loads(data)
                #get rid of retweets and keep only source tweets
                if sample['in_reply_to_user_id'] is not None or 'retweeted_status' in sample:
                    pass
                else:
                    self.output.write(data)
            except:
                pass

        def path_setup(self):
            """path_setup()

            Function to set up a path.
            """
            self.local_time = time.time()
            self.date = datetime.utcnow()
            self.dir = os.path.join(self.PREPEND+'%s_%s_%s_%s' % ('tweets', self.physical_event, self.lang, self.date.year),
                                    '%02d' % self.date.month,
                                    '%02d' % self.date.day, '%02d' % self.date.hour)
            self.path = os.path.join(self.dir, '%02d.json' % self.date.minute)
            try:
                os.makedirs(self.dir)
            except:
                pass
            self.output.close()
            self.output = open(self.path, 'w')
