# -*- coding: utf-8 -*-
"""News HCS  script

This will launch a scheduled News downloader -- download news from NewsApi using NewsApiClient at https://github.com/mattlisiv/newsapi-python

"""
import pdb
import multiprocessing
import requests, time, redis, base64, nltk, traceback
from newsapi import NewsApiClient
from datetime import datetime, timedelta
import dateutil.parser
from dateutil import tz
from sner import Ner
from utils.helper_utils import location_standardize, high_confidence_streamer_key, lookup_address_only, location_normalize, generate_cell, sublocation_key, readable_time
from utils.db_utils import get_db_connection
from importlib import reload

class News(multiprocessing.Process):
    def __init__(self,assed_config,root_name, errorQueue, messageQueue, **kwargs):
        multiprocessing.Process.__init__(self)
        # set up DB connections
        self.DB_CONN = get_db_connection(assed_config)
        self.client = NewsApiClient(api_key="f715251d799140f793e63a1aec194920")
        self.root_name = root_name
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue
        # No cached list because we are getting new stuff every day...
        self.config = kwargs["config"]
        self.NER = Ner(host='localhost', port=9199)
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        self.r=redis.Redis(connection_pool = pool) 
        pass

    def run(self,):
        
        try:
            for event_topic in self.config["topic_names"]:
                if not self.config["topic_names"][event_topic]["high_confidence"]["valid"]:
                    continue
                self.messageQueue.put("News downloader - working on %s"%event_topic)
                event_topic_key = str(self.config["topic_names"][event_topic]["index"])
                self.cached_list = self.getCachedList(event_topic_key)
                stopwords = self.config["topic_names"][event_topic]["stopwords"]
                keyword_set = self.config["topic_names"][event_topic]["high_confidence"]["keywords"]
                articles = []
                for keyword in keyword_set:
                    try:
                        response = self.client.get_everything(q=keyword,page_size=100)
                        articles += response["articles"]
                    except Exception as e:
                        self.messageQueue.put("NewsAPI for %s-%s failed with error: %s" % (event_topic,keyword, repr(e)))
                
                article_content, article_location = self.getArticleDetails(articles, stopwords)

                self.insertNews(article_content, event_topic_key)
                self.updateRedisLocations(article_location)
            
            self.DB_CONN.close()
            self.messageQueue.put("Completed News download successfully at %s."%readable_time())

        except Exception as e:
            traceback.print_exc()
            self.errorQueue.put((self.root_name, str(e)))            
            
    def getArticleDetails(self,articles, stopwords):
        article_content = []
        article_location = []
        exist_skip, stop_skip, location_skip, coordinate_skip = 0, 0, 0, 0
        for article in articles:
            item = {}
            item["id"] = base64.b64encode(str.encode(article["url"])).decode()
            if item["id"] in self.cached_list:
                exist_skip+=1
                continue
            item["source"] = article["source"]["name"]
            item["url"] = article["url"]
            item["time"] = dateutil.parser.parse(article["publishedAt"]).replace(tzinfo=tz.gettz('UTC')).astimezone(tz=tz.gettz('EDT')).strftime("%Y-%m-%d %H:%M:%S")
            item["title"] = article["title"]
            item["text"] = article["description"]

            #We are doing an extremely basic lookup <-- if it has landslide keyword, we accept. 
            #Since this is alreadya landslide feed, google/whatever has better classifiers. We exploit those to create a super simple keyword filter.
            search_flag = False
            search_counter = 0
            rText = item["text"]
            if "content" in article and article["content"] is not None and len(article["content"]) > 0:
                rText += article["content"]
            while not search_flag and search_counter < len(stopwords):
                if stopwords[search_counter] in rText:
                    search_flag = True
                search_counter+=1
            if search_flag:
                stop_skip+=1
                continue

            # Description based location
            temp_loc_tags = self.NER.get_entities(item["text"])
            desc_locations = self.extractLocations(temp_loc_tags)
            content_locations = []
            try:
                temp_loc_tags = self.NER.get_entities(" ".join(nltk.tokenize.word_tokenize(article["content"])))
                content_locations = self.extractLocations(temp_loc_tags)
            except (TypeError, IndexError):
                # TypeError -- if content is empty in article. IndexError -- if content is not None, but still empty
                pass

            # create location set - take unique from both desc and content_location, after normalization...
            item["description_location"] = [location_normalize(item) for item in desc_locations]
            item["content_location"] = [location_normalize(item) for item in content_locations]

            final_locations = list(set(item["description_location"] + item["content_location"]))
            if len(final_locations) == 0:
                location_skip+=1
                continue
            item["locations"] = final_locations
            
            lat,lng = lookup_address_only(desc_locations, self.config["APIKEYS"]["googlemaps"], self.r)
            if lat == False:
                raise ValueError("Ran out of GoogleMaps daily keys")
            if lat is None or lng is None:
                coordinate_skip+=1
                continue
            item["latitude"] = lat
            item["longitude"] = lng
            item["cell"] = generate_cell(lat,lng)

            article_content.append(item)
            article_location.append({"name":final_locations, "lat":lat, "lng":lng})

        self.messageQueue.put("Obtained News with: %i items and skipped \n\texisting %i items\n\tstopword %i items, \n\tmissing location %i items \n\tmissing coordinates %i items"%(len(article_content), exist_skip, stop_skip, location_skip, coordinate_skip))
        return article_content, article_location
        
    def extractLocations(self,temp_loc_tags):
        locations = []
        temp_loc=[]
        if temp_loc_tags[0][1] == 'LOCATION':
            temp_loc.append(temp_loc_tags[0][0])
        for entry in temp_loc_tags[1:]:
            if entry[1] == 'LOCATION':
                temp_loc.append(entry[0])
            else:
                if temp_loc:
                    locations.append(' '.join(temp_loc))
                    temp_loc=[]
        if temp_loc:
            locations.append(' '.join(temp_loc))
        return locations

    def convertDateFromTime(self, tm):
        '''
        Convert datetime to MySQL's datetime from time structure.
        '''
        return time.strftime("%Y-%m-%d %H:%M:%S", tm)

    def getCachedList(self,event_topic):
        cachedlist = set()
        cursor = self.DB_CONN.cursor()
        select = "SELECT item_id FROM HCS_News where timestamp > %s and topic_name = %s"%((datetime.now()-timedelta(days=5)).strftime("%Y-%m-%d"), event_topic)
        cursor.execute(select)
        results = cursor.fetchall()
        cursor.close()
        for row in results:
            cachedlist.add(row[0])
        self.messageQueue.put("News cachedlist has  %i items in last 5 days"%(len(cachedlist)))
        return cachedlist

    def insertNews(self,article_items, event_topic_key):
        event_topic_key = int(event_topic_key)
        cursor = self.DB_CONN.cursor()
        for item in article_items:

            insert = 'INSERT INTO HCS_News ( \
                        item_id, link, \
                        cell, latitude, longitude, timestamp, location, news_src, text, topic_name) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s,%s)'
            params = (item['id'], item['url'], item['cell'], \
                    item['latitude'], item['longitude'], item['time'], ",".join(item["locations"]), item['source'], item['text'], event_topic_key)
            try:
                cursor.execute(insert, params)
                self.DB_CONN.commit()
            except Exception as e:
                traceback.print_exc()
                self.messageQueue.put('Failed to insert %s with error %s' % (item["id"], repr(e)))
        cursor.close()
            

    def updateRedisLocations(self,article_location):
        # get REDIS connection
        
        totalLocations = len(article_location)
        sublocations = 0
        for location in article_location:
            converted_location = " ".join(location["name"])
            location_std = location_standardize(converted_location)
            location_key = high_confidence_streamer_key("news:location:"+location_std)
            self.r.set(location_key,converted_location,ex=259200)

            point_str = str(location["lat"]) + "," + str(location["lng"])
            for sublocation in location_std.split(":"):
                sublocationkey = sublocation_key(sublocation)
                self.r.set(sublocationkey, point_str, ex=259200)
                sublocations+=1
        self.messageQueue.put("Completed News with: %i locations and %i sublocations"%(totalLocations, sublocations))

