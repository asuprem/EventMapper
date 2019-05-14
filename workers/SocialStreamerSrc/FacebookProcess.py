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

class FacebookProcess(multiprocessing.Process):
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
        while True:
            # Perform work ONLY if it hasn't been performed today...
            previousTimestamp = self.r.get("social:streamer:facebook:%s:%s:timestamp"%(self.event, self.lang))
            previousApiAccesses = self.r.get("social:streamer:facebook:%s:%s:count"%(self.event, self.lang))
            if previousApiAccesses is None:
                previousApiAccesses = 0
            if previousTimestamp is not None:
                previousTimestamp = int(previousTimestamp)
            else:
                previousTimestamp = time.time()
            if datetime.fromtimestamp(previousTimestamp).day != datetime.fromtimestamp(time.time()).day:
                self.messageQueue.put("Initiating facebook download of %s-%s at %s"%(self.event, self.lang, readable_time()))
                max_results = 10
                for page_get in range(9):
                    start_param = page_get*10 + 1
                    if start_param > max_results:
                        continue
                    #results = self.service.cse().list(q=self.keywords,cx=self.cx,dateRestrict='d1',siteSearch='www.facebook.com',siteSearchFilter='i', start=start_param).execute()
                    results = {'searchInformation': {'searchTime': 0.438406, 'formattedSearchTime': '0.44', 'totalResults': '359', 'formattedTotalResults': '359'}, 'items': [{'kind': 'customsearch#result', 'title': 'Dost_pagasa - 24-HOUR PUBLIC WEATHER FORECAST Issued at ...', 'htmlTitle': 'Dost_pagasa - 24-HOUR PUBLIC WEATHER FORECAST Issued at ...', 'link': 'https://www.facebook.com/PAGASA.DOST.GOV.PH/photos/a.302759263167323/2109827015793863/?type=3', 'displayLink': 'www.facebook.com', 'snippet': 'Caused by: Southwesterlies Impacts: Possible flash floods or landslides due to \nscattered light to moderate rains. Place: Metro Manila and the rest of the country', 'htmlSnippet': 'Caused by: Southwesterlies Impacts: Possible flash floods or <b>landslides</b> due to <br>\nscattered light to moderate rains. Place: Metro Manila and the rest of the country', 'cacheId': 'U61GxJQyR4QJ', 'formattedUrl': 'https://www.facebook.com/PAGASA.../a.../2109827015793863/?...', 'htmlFormattedUrl': 'https://www.facebook.com/PAGASA.../a.../2109827015793863/?...', 'pagemap': {'cse_thumbnail': [{'width': '255', 'height': '197', 'src': 'https://encrypted-tbn1.gstatic.com/images?q=tbn:ANd9GcSmvRenMdLrIaVlcJZQhA9lPWht0i9M50eSKoYjUCgjlN5yEKdHrPENUM2m'}], 'metatags': [{'referrer': 'default', 'og:title': 'Dost_pagasa', 'og:description': '24-HOUR PUBLIC WEATHER FORECAST\nIssued at 4:00 PM Monday, 13 May 2019 \n\nSynopsis: Ridge of High Pressure Area affecting the eastern sections of Northern and Central Luzon, and Southern Luzon....', 'og:image': 'https://lookaside.fbsbx.com/lookaside/crawler/media/?media_id=2109827015793863', 'og:url': 'https://www.facebook.com/PAGASA.DOST.GOV.PH/posts/2109827262460505'}], 'cse_image': [{'src': 'https://lookaside.fbsbx.com/lookaside/crawler/media/?media_id=2109827015793863'}]}}, {'kind': 'customsearch#result', 'title': 'Persistent rainfall + floods +... - Severe Weather Europe | Facebook', 'htmlTitle': 'Persistent rainfall + floods +... - Severe Weather Europe | Facebook', 'link': 'https://www.facebook.com/severeweatherEU/posts/persistent-rainfall-floods-landslides-for-parts-of-the-western-balkans-next-week/2515360518687034/', 'displayLink': 'www.facebook.com', 'snippet': 'Persistent rainfall + floods + landslides for parts of the western Balkans next week\n! #Croatia and #Bosnia and Herzegovina will see the most problems....', 'htmlSnippet': 'Persistent rainfall + floods + <b>landslides</b> for parts of the western Balkans next week<br>\n! #Croatia and #Bosnia and Herzegovina will see the most problems....', 'cacheId': 'axcWNRhPvZ8J', 'formattedUrl': 'https://www.facebook.com/...landslides.../2515360518687034/', 'htmlFormattedUrl': 'https://www.facebook.com/...<b>landslides</b>.../2515360518687034/', 'pagemap': {'cse_thumbnail': [{'width': '225', 'height': '225', 'src': 'https://encrypted-tbn1.gstatic.com/images?q=tbn:ANd9GcQMzGKR0k2IELVUTyc9UD443A5nzGE2PDqO1rSLG6xCajL8xpYI26zXvm4'}], 'metatags': [{'referrer': 'default', 'og:title': 'Severe Weather Europe', 'og:description': 'Persistent rainfall + floods + landslides for parts of the western Balkans next week! #Croatia and #Bosnia and Herzegovina will see the most problems....', 'og:image': 'https://external-atl3-1.xx.fbcdn.net/safe_image.php?d=AQAQHIR0RhEZxrkq&w=400&h=400&url=http%3A%2F%2Fwww.severe-weather.eu%2Fwp-content%2Fuploads%2F2019%2F05%2F500h_anom.eu_Tuesday.png&cfs=1&_nc_hash=AQD2C7pYBAE_Wgq_', 'og:url': 'https://www.facebook.com/severeweatherEU/posts/2515360518687034'}], 'cse_image': [{'src': 'https://lookaside.fbsbx.com/lookaside/crawler/media/?media_id=157048324896449'}]}}]}
                    currentTimeStamp = time.time()
                    self.r.set("social:streamer:facebook:%s:%s:timestamp"%(self.event, self.lang), currentTimeStamp)
                    previousApiAccesses+=1
                    self.r.set("social:streamer:facebook:%s:%s:count"%(self.event, self.lang), previousApiAccesses)

                    max_results = results["searchInformation"]["totalResults"]

                    

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
                self.messageQueue.put("Completed facebook download of %s-%s at time %s"%(self.event, self.lang, readable_time()))

            else:
                # we already done...
                self.messageQueue.put("Facebook download of %s-%s at is already complete for day %s"%(self.event, self.lang, str(datetime.fromtimestamp(time.time()).day)))
                # Perform sleep for four hours until next check.
                time.sleep(14400)
        
        


