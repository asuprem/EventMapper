# -*- coding: utf-8 -*-
"""USGS HCS  script

This will launch a scheduled USGS downloader
"""

import multiprocessing
import requests, re, time, time, redis, datetime, traceback, xmltodict

from utils.helper_utils import location_standardize, high_confidence_streamer_key, sublocation_key, readable_time, \
    generate_cell
from utils.db_utils import get_db_connection, run_sql_file
from utils.file_utils import load_config


class USGS_wildfire(multiprocessing.Process):
    def __init__(self, assed_config, root_name, errorQueue, messageQueue, **kwargs):
        multiprocessing.Process.__init__(self)
        # set up DB connections
        self.DB_CONN = get_db_connection(assed_config)

        self.root_name = root_name
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue
        self.cached_list = self.getCachedList()
        pass

    def run(self, ):
        try:
            url_list = ['https://www.geomac.gov/DynContent/georss/nifc_large_firesW3C.xml']

            for usgs_url in url_list:
                self.messageQueue.put("Obtaining USGS url: %s" % usgs_url)
                try:
                    response = requests.get(usgs_url)
                except Exception as e:
                    self.messageQueue.put("USGS URL %s failed with error: %s" % (usgs_url, repr(e)))
                    continue
                responseDict = xmltodict.parse(response.text)['rss']['channel']
                usgs_items, usgs_locations = self.getUSGSItems(responseDict)
                self.insertUSGS(usgs_items)
                self.updateRedisLocations(usgs_locations)

            self.DB_CONN.close()
            self.messageQueue.put("Completed USGS successfully at %s." % readable_time())
        except Exception as e:
            traceback.print_exc()
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))

    def getUSGSItems(self, dictData):
        usgs_locations = []
        usgs_items = []
        skipCounter = 0
        for feature in dictData['item']:
            usgs_id = feature['title'].split(',')[1].replace(' ', '')
            if usgs_id in self.cached_list:
                skipCounter += 1
                continue
            self.cached_list.add(usgs_id)
            item = {}
            item['usgs_id'] = usgs_id
            item['longitude'] = float(feature['geo:long'])
            item['latitude'] = float(feature['geo:lat'])
            item["cell"] = generate_cell(item["latitude"], item["longitude"])
            item['acres'] = float(feature['description'].split(',')[1].replace(' acres', ''))
            item['time'] = self.convertDateFromTime(time.strptime(feature['description'].split(',')[0],
                                                                  "%m/%d/%Y %I:%M:%S %p"))
            item['place'] = feature['title'].split(',')[0] + ', ' + item['usgs_id'].split('-')[1][:2] + ', United States'
            usgs_items.append(item)
            usgs_locations.append({"name": item['place'], "lat": item["latitude"], "lng": item["longitude"]})

        self.messageQueue.put(
            "Obtained USGS with: %i items and skipped existing %i items" % (len(usgs_items), skipCounter))
        return usgs_items, usgs_locations

    def convertDateFromTime(self, tm):
        '''
        Convert datetime to MySQL's datetime from time structure.
        '''
        return time.strftime("%Y-%m-%d %H:%M:%S", tm)

    def getCachedList(self, ):
        cachedlist = set()
        try:
            cursor = self.DB_CONN.cursor()
            # get things from last 5 days
            select = "SELECT usgs_id FROM HCS_USGS_WILDFIRE where time > %s" % (
                    datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
            cursor.execute(select)
            results = cursor.fetchall()
            cursor.close()
            for row in results:
                cachedlist.add(row[0])
            self.messageQueue.put("USGS cachedlist has  %i items in last 5 days" % (len(cachedlist)))
        except Exception as e:
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))
        return cachedlist

    def insertUSGS(self, usgs_items):
        cursor = self.DB_CONN.cursor()
        for item in usgs_items:

            insert = 'INSERT INTO HCS_USGS_WILDFIRE ( \
                        usgs_id, \
                        time, latitude, longitude, acres, place, cell) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s)'
            params = (item['usgs_id'], item['time'], item['latitude'], item['longitude'], item['acres'],
                      item['place'], item["cell"])
            try:
                cursor.execute(insert, params)
                self.DB_CONN.commit()
            except Exception as e:
                self.messageQueue.put('Failed to insert %s with error %s' % (item["usgs_id"], repr(e)))
        cursor.close()

    def updateRedisLocations(self, usgs_locations):
        # get REDIS connection
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=pool)
        totalLocations = len(usgs_locations)
        sublocations = 0
        for location in usgs_locations:
            # loc standardize cleans as well...
            location_std = location_standardize(location["name"])
            location_key = high_confidence_streamer_key("usgs-wildfire:location:" + location_std)
            r.set(location_key, location["name"], ex=259200)

            # set location data
            point_str = str(location["lat"]) + "," + str(location["lng"])
            for sublocation in location_std.split(":"):
                sublocationkey = sublocation_key(sublocation)
                r.set(sublocationkey, point_str, ex=259200)
                sublocations += 1
        self.messageQueue.put("Completed USGS with: %i locations and %i sublocations" % (totalLocations, sublocations))


if __name__ == '__main__':
    assed_config = load_config('../../config/assed_config.json')
    proc = USGS(assed_config, 'USGS_wildfire', multiprocessing.Queue(), multiprocessing.Queue())
    run_sql_file('../../initialization/mysql/usgs_wildfire_sql.SQL', proc.DB_CONN)
    proc.run()
