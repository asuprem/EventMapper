# -*- coding: utf-8 -*-
"""MODIS HCS  script

This will launch a scheduled MODIS downloader
"""

import multiprocessing
import requests, re, time, time, redis, datetime, traceback, xmltodict
import reverse_geocoder as rg

from utils.helper_utils import location_standardize, high_confidence_streamer_key, sublocation_key, readable_time, \
    generate_cell
from utils.db_utils import get_db_connection, run_sql_file
from utils.file_utils import load_config


class MODIS_wildfire(multiprocessing.Process):
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
            url_list = [
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Canada_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Alaska_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_USA_contiguous_and_Hawaii_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Central_America_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_South_America_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Europe_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Northern_and_Central_Africa_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Southern_Africa_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Russia_and_Asia_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_South_Asia_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_SouthEast_Asia_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Australia_and_New_Zealand_24h.csv',
            ]

            for modis_url in url_list:
                self.messageQueue.put("Obtaining MODIS url: %s" % modis_url)
                print("Obtaining MODIS url: %s" % modis_url)
                try:
                    response = requests.get(modis_url)
                except Exception as e:
                    self.messageQueue.put("MODIS URL %s failed with error: %s" % (modis_url, repr(e)))
                    print("MODIS URL %s failed with error: %s" % (modis_url, repr(e)))
                    continue
                modis_items, modis_locations = self.getMODISItems(response.text.split('\n')[1:])
                self.insertMODIS(modis_items)
                self.updateRedisLocations(modis_locations, modis_url)

            self.DB_CONN.close()
            self.messageQueue.put("Completed MODIS successfully at %s." % readable_time())
            print("Completed MODIS successfully at %s." % readable_time())
        except Exception as e:
            traceback.print_exc()
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))

    def getMODISItems(self, csvData):
        modis_locations = []
        modis_items = []
        skipCounter = 0

        for fid, feature in enumerate(csvData):
            if len(feature) == 0:
                continue
            feature = feature.split(',')
            latitude = float(feature[0])
            longitude = float(feature[1])
            brightness = float(feature[2])
            scan = float(feature[3])
            track = float(feature[4])
            acq_datetime = feature[5] + ' ' + feature[6]
            satellite = feature[7]
            confidence = float(feature[8])
            bright_t31 = float(feature[10])
            frp = float(feature[11])
            daynight = feature[12]

            modis_cached_check = "%f_%f" % (round(latitude,1), round(longitude,1))
            modis_id = "%s_%f_%f" % (acq_datetime.replace(' ', '_'), round(latitude,1), round(longitude,1))
            if modis_cached_check in self.cached_list:
                skipCounter += 1
                continue
            self.cached_list.add(modis_cached_check)
            item = {}
            item['modis_id'] = modis_id
            item['latitude'] = float(feature[0])
            item['longitude'] = float(feature[1])
            item["cell"] = generate_cell(item["latitude"], item["longitude"])
            item['brightness'] = brightness
            item['acq_time'] = self.convertDateFromTime(time.strptime(acq_datetime, "%Y-%m-%d %H%M"))
            item['scan'] = scan
            item['track'] = track
            item['satellite'] = satellite
            item['confidence'] = confidence
            item['bright_t31'] = bright_t31
            item['frp'] = frp
            item['daynight'] = daynight
            reverse_geocode = rg.search((item['latitude'], item['longitude']))[0]
            item['place'] = reverse_geocode['name'] + ', ' + reverse_geocode['admin1'] + ', ' + reverse_geocode['cc']
            modis_items.append(item)
            modis_locations.append({"name": item['place'], "lat": item["latitude"], "lng": item["longitude"]})

        self.messageQueue.put(
            "Obtained MODIS with: %i items and skipped existing %i items" % (len(modis_items), skipCounter))
        print("Obtained MODIS with: %i items and skipped existing %i items" % (len(modis_items), skipCounter))
        return modis_items, modis_locations

    def convertDateFromTime(self, tm):
        '''
        Convert datetime to MySQL's datetime from time structure.
        '''
        return time.strftime("%Y-%m-%d %H:%M:%S", tm)

    def getCachedList(self, ):
        cachedlist = set()
        try:
            cursor = self.DB_CONN.cursor()
            # get things from last 2 days
            select = "SELECT modis_id FROM HCS_MODIS_WILDFIRE where acq_time > %s" % (
                    datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
            cursor.execute(select)
            results = cursor.fetchall()
            cursor.close()
            for row in results:
                cachedlist.add("_".join(row[0].split("_")[-2:]))
            self.messageQueue.put("MODIS cachedlist has  %i items in last 2 days" % (len(cachedlist)))
            print("MODIS cachedlist has  %i items in last 2 days" % (len(cachedlist)))
        except Exception as e:
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))
        return cachedlist

    def insertMODIS(self, modis_items):
        cursor = self.DB_CONN.cursor()
        for item in modis_items:

            insert = 'INSERT INTO HCS_MODIS_WILDFIRE ( \
                        modis_id, \
                        longitude, latitude, brightness, bright_t31, acq_time, place, \
                        track, satellite, confidence, frp, daynight, cell) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            params = (item['modis_id'], item['longitude'], item['latitude'], item['brightness'], item['bright_t31'],
                      item['acq_time'], item['place'], item['track'], item['satellite'],
                      item['confidence'], item['frp'], item['daynight'], item['cell'])
            try:
                cursor.execute(insert, params)
                self.DB_CONN.commit()
            except Exception as e:
                self.messageQueue.put('Failed to insert %s with error %s' % (item["modis_id"], repr(e)))
                print('Failed to insert %s with error %s' % (item["modis_id"], repr(e)))
        cursor.close()

    def updateRedisLocations(self, modis_locations, modis_url):
        # get REDIS connection
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=pool)
        totalLocations = len(modis_locations)
        sublocations = 0
        for location in modis_locations:
            # loc standardize cleans as well...
            location_std = location_standardize(location["name"])
            location_key = high_confidence_streamer_key("modis-wildfire:location:" + location_std)
            r.set(location_key, location["name"], ex=259200)

            # set location data
            point_str = str(location["lat"]) + "," + str(location["lng"])
            for sublocation in location_std.split(":"):
                sublocationkey = sublocation_key(sublocation)
                r.set(sublocationkey, point_str, ex=259200)
                sublocations += 1
        self.messageQueue.put("Completed MODIS with: %i locations and %i sublocations for %s" % (totalLocations, sublocations, modis_url))
        #print("Completed MODIS with: %i locations and %i sublocations" % (totalLocations, sublocations))


if __name__ == '__main__':
    assed_config = load_config('../../config/assed_config.json')
    proc = MODIS(assed_config, 'MODIS_wildfire', multiprocessing.Queue(), multiprocessing.Queue())
    run_sql_file('../../initialization/mysql/modis_wildfire_sql.SQL', proc.DB_CONN)
    proc.run()
