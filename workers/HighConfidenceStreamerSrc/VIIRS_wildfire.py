# -*- coding: utf-8 -*-
"""VIIRS HCS  script

This will launch a scheduled VIIRS downloader
"""

import multiprocessing
import requests, re, time, time, redis, datetime, traceback, xmltodict
import reverse_geocoder as rg

from utils.helper_utils import location_standardize, high_confidence_streamer_key, sublocation_key, readable_time, \
    generate_cell
from utils.db_utils import get_db_connection, run_sql_file
from utils.file_utils import load_config


class VIIRS_wildfire(multiprocessing.Process):
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
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Canada_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Alaska_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_USA_contiguous_and_Hawaii_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Central_America_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_South_America_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Europe_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Northern_and_Central_Africa_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Southern_Africa_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Russia_and_Asia_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_South_Asia_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_SouthEast_Asia_24h.csv',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_Australia_and_New_Zealand_24h.csv'
            ]

            for viirs_url in url_list:
                self.messageQueue.put("Obtaining VIIRS url: %s" % viirs_url)
                try:
                    response = requests.get(viirs_url)
                except Exception as e:
                    self.messageQueue.put("VIIRS URL %s failed with error: %s" % (viirs_url, repr(e)))
                    continue
                viirs_items, viirs_locations = self.getVIIRSItems(response.text.split('\n')[1:])
                self.insertVIIRS(viirs_items)
                self.updateRedisLocations(viirs_locations, viirs_url)

            self.DB_CONN.close()
            self.messageQueue.put("Completed VIIRS successfully at %s." % readable_time())
        except Exception as e:
            traceback.print_exc()
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))

    def getVIIRSItems(self, csvData):
        viirs_locations = []
        viirs_items = []
        skipCounter = 0

        for fid, feature in enumerate(csvData):
            if len(feature) == 0:
                continue
            feature = feature.split(',')
            latitude = float(feature[0])
            longitude = float(feature[1])
            bright_ti4 = float(feature[2])
            scan = float(feature[3])
            track = float(feature[4])
            acq_datetime = feature[5] + ' ' + feature[6]
            satellite = feature[7]
            confidence = feature[8]
            bright_ti5 = float(feature[10])
            frp = float(feature[11])
            daynight = feature[12]


            viirs_cached_check = "%f_%f" % (round(latitude,1), round(longitude,1))
            viirs_id = "%s_%f_%f" % (acq_datetime.replace(' ', '_'), round(latitude,1), round(longitude,1))
            if viirs_cached_check in self.cached_list:
                skipCounter += 1
                continue
            self.cached_list.add(viirs_cached_check)

            item = {}
            item['viirs_id'] = viirs_id
            item['latitude'] = float(feature[0])
            item['longitude'] = float(feature[1])
            item["cell"] = generate_cell(item["latitude"], item["longitude"])
            item['bright_ti4'] = bright_ti4
            item['acq_time'] = self.convertDateFromTime(time.strptime(acq_datetime, "%Y-%m-%d %H%M"))
            item['scan'] = scan
            item['track'] = track
            item['satellite'] = satellite
            item['confidence'] = confidence
            item['bright_ti5'] = bright_ti5
            item['frp'] = frp
            item['daynight'] = daynight
            reverse_geocode = rg.search((item['latitude'], item['longitude']))[0]
            item['place'] = reverse_geocode['name'] + ', ' + reverse_geocode['admin1'] + ', ' + reverse_geocode['cc']
            viirs_items.append(item)
            viirs_locations.append({"name": item['place'], "lat": item["latitude"], "lng": item["longitude"]})

        self.messageQueue.put(
            "Obtained VIIRS with: %i items and skipped existing %i items" % (len(viirs_items), skipCounter))
        return viirs_items, viirs_locations

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
            select = "SELECT viirs_id FROM HCS_VIIRS_WILDFIRE where acq_time > %s" % (
                    datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
            cursor.execute(select)
            results = cursor.fetchall()
            cursor.close()
            for row in results:
                cachedlist.add("_".join(row[0].split("_")[-2:]))
            self.messageQueue.put("VIIRS cachedlist has  %i items in last 2 days" % (len(cachedlist)))
        except Exception as e:
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))
        return cachedlist

    def insertVIIRS(self, viirs_items):
        cursor = self.DB_CONN.cursor()
        for item in viirs_items:

            insert = 'INSERT INTO HCS_VIIRS_WILDFIRE ( \
                        viirs_id, \
                        longitude, latitude, bright_ti4, bright_ti5, acq_time, place, \
                        track, satellite, confidence, frp, daynight, cell) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            params = (item['viirs_id'], item['longitude'], item['latitude'], item['bright_ti4'], item['bright_ti5'],
                      item['acq_time'], item['place'], item['track'], item['satellite'],
                      item['confidence'], item['frp'], item['daynight'], item['cell'])
            try:
                cursor.execute(insert, params)
                self.DB_CONN.commit()
            except Exception as e:
                self.messageQueue.put('Failed to insert %s with error %s' % (item["viirs_id"], repr(e)))
        cursor.close()

    def updateRedisLocations(self, viirs_locations, viirs_url):
        # get REDIS connection
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=pool)
        totalLocations = len(viirs_locations)
        sublocations = 0
        for location in viirs_locations:
            # loc standardize cleans as well...
            location_std = location_standardize(location["name"])
            location_key = high_confidence_streamer_key("viirs-wildfire:location:" + location_std)
            r.set(location_key, location["name"], ex=259200)

            # set location data
            point_str = str(location["lat"]) + "," + str(location["lng"])
            for sublocation in location_std.split(":"):
                sublocationkey = sublocation_key(sublocation)
                r.set(sublocationkey, point_str, ex=259200)
                sublocations += 1
        self.messageQueue.put("Completed VIIRS with: %i locations and %i sublocations for %s" % (totalLocations, sublocations, viirs_url))


if __name__ == '__main__':
    assed_config = load_config('../../config/assed_config.json')
    proc = VIIRS(assed_config, 'VIIRS_wildfire', multiprocessing.Queue(), multiprocessing.Queue())
    run_sql_file('../../initialization/mysql/viirs_wildfire_sql.SQL', proc.DB_CONN)
    proc.run()
