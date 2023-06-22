# -*- coding: utf-8 -*-
"""USGS HCS  script

This will launch a scheduled USGS downloader
"""

import multiprocessing
import requests, re, time, time, redis, datetime, traceback

from utils.helper_utils import location_standardize, high_confidence_streamer_key, sublocation_key, readable_time, generate_cell
from utils.db_utils import get_db_connection

class USGS(multiprocessing.Process):
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
            url_list = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson']
            
            for usgs_url in url_list:
                self.messageQueue.put("Obtaining USGS url: %s"%usgs_url)
                try:
                    response = requests.get(usgs_url)
                except Exception as e:
                    self.messageQueue.put("USGS URL %s failed with error: %s" % (usgs_url, repr(e)))
                    continue
                jsonData = response.json()
                usgs_items, usgs_locations = self.getUSGSItems(jsonData)
                self.insertUSGS(usgs_items)
                self.updateRedisLocations(usgs_locations)
            
            self.DB_CONN.close()
            self.messageQueue.put("Completed USGS successfully at %s."%readable_time())
        except Exception as e:
            traceback.print_exc()
            self.errorQueue.put((self.root_name, str(e)))
            
            
    def getUSGSItems(self,jsonData):
        usgs_locations = []
        usgs_items = []
        skipCounter = 0
        for feature in jsonData['features']:
            usgs_id = feature['id']
            if usgs_id in self.cached_list: 
                skipCounter+=1
                continue
            self.cached_list.add(usgs_id)
            item = {}
            item['usgs_id'] = usgs_id
            coords = feature['geometry']['coordinates']
            item['longitude'] = float(str(coords[0]))
            item['latitude'] = float(str(coords[1]))
            item["cell"] = generate_cell(item["latitude"],item["longitude"])
            item['mag'] = feature['properties']['mag']
            item['time'] = datetime.datetime.fromtimestamp(feature['properties']['time']/1000.0)
            place_raw = feature['properties']['place'].lower()
            if ' of ' in place_raw:
                parts = place_raw.split(' of ')
                place_detailed = ' '.join(map(lambda w: w, re.findall(r'(?u)\w+', parts[1])))
            else:
                place_detailed = place_raw
            item['place_detailed'] = place_detailed.encode(encoding="utf-8", errors="ignore")

            place_general = None
            if ', ' in place_raw:
                parts = place_raw.split(', ')
                place_general = parts[1]
            item['place_general'] = place_general
            usgs_items.append(item)
            usgs_locations.append({"name":place_detailed, "lat":item["latitude"], "lng":item["longitude"]})
        
        self.messageQueue.put("Obtained USGS with: %i items and skipped existing %i items"%(len(usgs_items), skipCounter))
        return usgs_items, usgs_locations
        
    def convertDateFromTime(self, tm):
        '''
        Convert datetime to MySQL's datetime from time structure.
        '''
        return time.strftime("%Y-%m-%d %H:%M:%S", tm)

    def getCachedList(self,):
        cachedlist = set()
        cursor = self.DB_CONN.cursor()
        # get things from last 5 days
        select = "SELECT usgs_id FROM HCS_USGS where time > %s"%(datetime.datetime.now()-datetime.timedelta(days=5)).strftime("%Y-%m-%d")
        cursor.execute(select)
        results = cursor.fetchall()
        cursor.close()
        for row in results:
            cachedlist.add(row[0])
        self.messageQueue.put("USGS cachedlist has  %i items in last 5 days"%(len(cachedlist)))
        return cachedlist

    def insertUSGS(self,usgs_items):
        cursor = self.DB_CONN.cursor()
        for item in usgs_items:

            insert = 'INSERT INTO HCS_USGS ( \
                        usgs_id, \
                        time, latitude, longitude, mag, place_detailed, place_general, cell) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s, %s)'
            params = (item['usgs_id'], item['time'], item['latitude'], \
                    item['longitude'], item['mag'], item['place_detailed'], item['place_general'], item["cell"])
            try:
                cursor.execute(insert, params)
                self.DB_CONN.commit()
            except Exception as e:
                self.messageQueue.put('Failed to insert %s with error %s' % (item["usgs_id"], repr(e)))
        cursor.close()
            

    def updateRedisLocations(self,usgs_locations):
        # get REDIS connection
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        r=redis.Redis(connection_pool = pool) 
        totalLocations = len(usgs_locations)
        sublocations = 0
        for location in usgs_locations:
            # loc standardize cleans as well...
            location_std = location_standardize(location["name"])
            location_key = high_confidence_streamer_key("usgs:location:"+location_std)
            r.set(location_key,location["name"],ex=259200)

            # set location data
            point_str = str(location["lat"]) + "," + str(location["lng"])
            for sublocation in location_std.split(":"):
                sublocationkey = sublocation_key(sublocation)
                r.set(sublocationkey, point_str, ex=259200)
                sublocations+=1
        self.messageQueue.put("Completed USGS with: %i locations and %i sublocations"%(totalLocations, sublocations))

