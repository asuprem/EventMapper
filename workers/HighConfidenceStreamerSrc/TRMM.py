# -*- coding: utf-8 -*-
"""TRMM HCS  script

This will launch a scheduled TRMM downloader
"""

import multiprocessing
import requests, time, redis, datetime, traceback
from utils.helper_utils import location_standardize, high_confidence_streamer_key, sublocation_key, readable_time, generate_cell
from utils.db_utils import get_db_connection
from importlib import reload

class TRMM(multiprocessing.Process):
    def __init__(self,assed_config,root_name, errorQueue, messageQueue, **kwargs):
        multiprocessing.Process.__init__(self)
        # set up DB connections
        self.DB_CONN = get_db_connection(assed_config)

        self.root_name = root_name
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue
        self.cached_list = self.getCachedList()
        pass

    def run(self,):
        try:
            url_list = ['https://trmm.gsfc.nasa.gov/trmm_rain/Events/latest_1_day_landslide.html',
                        'https://trmm.gsfc.nasa.gov/trmm_rain/Events/latest_3_day_landslide.html',
                        'https://trmm.gsfc.nasa.gov/trmm_rain/Events/latest_7_day_landslide.html']
            
                
            for trmm_url in url_list:
                self.messageQueue.put("Obtained TRMM url: %s"%trmm_url)
                try:
                    response = requests.get(trmm_url)
                except Exception as e:
                    self.messageQueue.put("TRMM URL %s failed with error: %s" % (trmm_url, repr(e)))
                    continue
                html = response.text
                trmm_items, trmm_locations = self.getTRMMItems(html)

                self.insertTRMM(trmm_items)
                self.updateRedisLocations(trmm_locations)
            
            self.DB_CONN.close()
            self.messageQueue.put("Completed TRMM successfully at %s."%readable_time())
        except Exception as e:
            traceback.print_exc()
            self.errorQueue.put((self.root_name, str(e)))
                
            
    def getTRMMItems(self,html):
        trmm_locations = []
        trmm_items = []
        skipCounter = 0
        for line in html.split('\n'):
            words = line.split()
            n = len(words)
            if (n>3 and words[3]=='landslides'):
                item = {}
                date_str = words[0][:8]
                item['date'] = self.convertDateFromTime(time.strptime(date_str, '%Y%m%d'))
                latitude = words[n-2]
                longitude = words[n-1]
                if words[4]=='LIKELY':
                    item['very_likely'] = 0
                else:
                    item['very_likely'] = 1
                trmm_id = date_str+'_'+longitude+'_'+latitude

                if trmm_id in self.cached_list: 
                    skipCounter += 1
                    continue
                self.cached_list.add(trmm_id)

                item['trmm_id'] = trmm_id
                item['line'] = line
                item['latitude'] = float(latitude)
                item['longitude'] = float(longitude)
                item["cell"] = generate_cell(item["latitude"], item["longitude"])
                country = words[n-3]
                item['country'] = country.lower()
                words1 = line.split('km from ')
                words1 = words1[1].split(country)
                place = words1[0].strip()
                item['place'] = place.lower()
                trmm_items.append(item)
                trmm_locations.append({"name":item['place'], "lat":item["latitude"], "lng":item["longitude"]})
        self.messageQueue.put("Obtained TRMM with: %i items and skipped existing %i items"%(len(trmm_items), skipCounter))
        return trmm_items, trmm_locations
        
    def convertDateFromTime(self, tm):
        '''
        Convert datetime to MySQL's datetime from time structure.
        '''
        return time.strftime("%Y-%m-%d %H:%M:%S", tm)

    def getCachedList(self,):
        cachedlist = set()
        cursor = self.DB_CONN.cursor()
        select = "SELECT trmm_id FROM HCS_TRMM where date > %s"%(datetime.datetime.now()-datetime.timedelta(days=5)).strftime("%Y-%m-%d")
        cursor.execute(select)
        results = cursor.fetchall()
        cursor.close()
        for row in results:
            cachedlist.add(row[0])
        self.messageQueue.put("TRMM cachedlist has  %i items in last 5 days"%(len(cachedlist)))
        return cachedlist

    def insertTRMM(self,trmm_items):
        cursor = self.DB_CONN.cursor()
        for item in trmm_items:
            
            insert = 'INSERT INTO HCS_TRMM ( \
                        trmm_id, line, \
                        date, latitude, longitude, very_likely, place, country, cell) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s)'
            params = (item['trmm_id'], item['line'], item['date'], \
                    item['latitude'], item['longitude'], item['very_likely'], item['place'], item['country'], item["cell"])
            try:
                cursor.execute(insert, params)
                self.DB_CONN.commit()
            except Exception as e:
                traceback.print_exc()
                self.messageQueue.put('Failed to insert %s with error %s' % (item["trmm_id"], repr(e)))
        cursor.close()
            

    def updateRedisLocations(self,trmm_locations):
        # get REDIS connection
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        r=redis.Redis(connection_pool = pool) 
        totalLocations = len(trmm_locations)
        sublocations = 0
        for location in trmm_locations:
            location_std = location_standardize(location["name"])
            location_key = high_confidence_streamer_key("trmm:location:"+location_std)
            r.set(location_key,location["name"],ex=259200)

            # set location data
            point_str = str(location["lat"]) + "," + str(location["lng"])
            for sublocation in location_std.split(":"):
                sublocationkey = sublocation_key(sublocation)
                r.set(sublocationkey, point_str, ex=259200)
                sublocations+=1
        self.messageQueue.put("Completed TRMM with: %i locations and %i sublocations"%(totalLocations, sublocations))