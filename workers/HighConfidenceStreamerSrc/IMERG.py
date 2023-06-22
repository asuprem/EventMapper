# -*- coding: utf-8 -*-
"""VIIRS HCS  script

This will launch a scheduled VIIRS downloader
"""

import glob, os
import multiprocessing
import requests, re, time, time, redis, datetime, traceback, h5py
import reverse_geocoder as rg
import numpy as np

from utils.helper_utils import location_standardize, high_confidence_streamer_key, sublocation_key, readable_time, \
    generate_cell
from utils.db_utils import get_db_connection, run_sql_file
from utils.file_utils import load_config
from bs4 import BeautifulSoup


class IMERG(multiprocessing.Process):
    def __init__(self, assed_config, root_name, errorQueue, messageQueue, **kwargs):
        multiprocessing.Process.__init__(self)
        # set up DB connections
        self.DB_CONN = get_db_connection(assed_config)

        self.root_name = root_name
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue
        self.config = kwargs["config"]
        self.cached_list = self.getCachedList()
        pass

    def run(self, ):
        try:
            
            # 0. Check for IMERG_arrays in the last 6 hours in downloads
            #       a. do a glob search with pattern, and extract the time component
            #       b. Check which time components are from more than six hours ago
            #       c. Delete those files, keep the rest, load them and sum them in memory
            # 1. Download most recent data from remote.
            # 2. Save this in downloads, open in h5py, extract precipitationCal field, add to in memory sum
            # 3. For each cell where sum > 7.5 mm (based on 0.3 inches of rain), save into IMERG database
            # 4. Close the hdf5 dataset, delete the file
            # 5. save the precipitationCal into a file in downloads, following pattern.
            # FIN

            # 0
            name_pattern = "IMERGLate_temp_downloads"
            name_ending = "array"

            imerg_list = glob.glob("./downloads/{name_pattern}_*.{extension}".format(name_pattern=name_pattern,extension=name_ending))
            keep_dict = {}
            for imerg_file in imerg_list:
                imerg_time = imerg_file[len(name_pattern)+1:-len(name_ending)]
                if self.is_within_timeframe(imerg_time, timeframe=6):
                    keep_dict[imerg_file] = True
                else:
                    keep_dict[imerg_file] = False

            delete_list = [item for item in keep_dict if keep_dict[item] is False]
            [os.remove(item) for item in delete_list]

            load_list = [item for item in keep_dict if keep_dict[item] is True]
            summed_array = np.zeros((3600, 1800))
            for imerg_load in load_list:
                n_array = np.load(imerg_load)
                summed_array += n_array

            # 1, 2
            base_url = "https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/"
            username = self.config["credentials"]["imerg"]["username"]
            password = self.config["credentials"]["imerg"]["password"]
            imerg_items, imerg_locations = self.getIMERGitems(name_pattern, name_ending, base_url, username, password, summed_array)

            self.insertIMERG(imerg_items)
            self.updateRedisLocations(imerg_locations, base_url)

            self.DB_CONN.close()
            self.messageQueue.put("[imerg] Completed IMERG successfully at %s." % readable_time())
        except Exception as e:
            traceback.print_exc()
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))




    def unix_time_to_string(self, unix_time):
        # Convert Unix timestamp to datetime object
        dt = datetime.datetime.fromtimestamp(unix_time)
        # Format the datetime object into the desired string format
        formatted_string = dt.strftime("%Y_%m_%d_%H")
        return formatted_string


    def is_within_timeframe(self, time_string, timeframe = 6):
        # Get the current time
        current_time = datetime.datetime.now()
        
        # Convert the time string into a datetime object
        dt = datetime.datetime.strptime(time_string, "%Y_%m_%d_%H")
        
        # Calculate the time difference between the current time and the specified time
        time_difference = current_time - dt
        # Check if the time difference is within six hours
        if abs(time_difference.total_seconds()) <= timeframe * 60 * 60:
            return True
        else:
            return False

    def get_imerg_files(self, base_url, username, password, year_month):
        # Construct the URL for the specified YearMonth directory
        url = base_url + year_month + "/"

        # Send a GET request to the URL with authentication
        response = requests.get(url, auth=(username, password))

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the HTML content to extract the file URLs
            file_urls = []
            soup = BeautifulSoup(response.content, 'html.parser')
            rows = soup.find_all('tr')
            for row in rows:
                link = row.find('a')
                if link is not None:
                    file_url = base_url + year_month + "/" + link.get('href')
                    file_urls.append(file_url)

            return file_urls
        else:
            print("Request failed. Status code:", response.status_code)
            return None


    def download_file(self, url, username, password):
        # Send a GET request to download the file with authentication
        response = requests.get(url, auth=(username, password))

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the file name from the URL
            file_name = url.rsplit('/', 1)[-1]

            # Save the file
            with open(file_name, 'wb') as f:
                f.write(response.content)

            print("Download complete:", file_name)
            return True
        else:
            print("Download failed. Status code:", response.status_code)
            return False



    def getIMERGitems(self, name_pattern, name_ending, base_url, username, password, summed_array):
        imerg_locations = []
        imerg_items = []
        skip_counter = 0
        currentyearmonth = datetime.datetime.now().strftime("%Y%m")
        files = self.get_imerg_files(base_url, username, password, currentyearmonth)
        if files:
            if not os.path.exists(files[-1].rsplit('/', 1)[-1]):

                download_success = self.download_file(files[-1], username, password)
                if download_success:
                    fname = files[-1].rsplit('/', 1)[-1]
                    fobj = h5py.File(fname, "r")
                    precipArray = np.array(fobj["Grid"]["precipitationCal"])[0,:,:] # shape: 3600x1800
                    # 5
                    np.save("./downloads/{name_pattern}_{date_pattern}.{extension}".format(
                        name_pattern=name_pattern,
                        date_pattern = self.unix_time_to_string(int(time.time())),
                        extension = name_ending
                    ), precipArray)
                    summed_array += precipArray

                    # 4
                    fobj.close()
                    os.remove(fname)

                    # 3
                    # TODO
                    # So, identify the values > 7.5 in summed
                    # Then, back calculate the longitude and latitude of these
                    # Then, calculate the cell value
                    # Then push into the IMERG database...
                    matchings = np.where(summed_array>40) # magic number, 7.5*6 = 45 --> 40
                    matching_longitudes = matchings[0].tolist()
                    matching_latitudes = matchings[1].tolist()
                    longitude_array = np.linspace(-179.95, 179.95, 3600)
                    latitude_array = np.linspace(-89.95, 89.95, 1800)

                    # round it down. Then we will take the max values in each lng-lat cell
                    final_longitude = longitude_array[matching_longitudes].astype(int)
                    final_latitude = latitude_array[matching_latitudes].astype(int)

                    rainfall_dict = {}
                    for idx, item in enumerate(zip(final_latitude, final_longitude)):
                        # generate string of lat_lng
                        latlng = "%i_%i"%(item[0], item[1])
                        # since we cast to int, multiple lat-lngas have the same value, with different rainfall amounts.
                        # we collect the max value for the int-cast cell
                        if latlng in rainfall_dict:
                            if summed_array[matching_longitudes[idx], matching_latitudes[idx]] > rainfall_dict[latlng]:
                                rainfall_dict[latlng] = summed_array[matching_longitudes[idx], matching_latitudes[idx]]
                        else:
                            rainfall_dict[latlng] = summed_array[matching_longitudes[idx], matching_latitudes[idx]]

                    acq_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    for latlng in rainfall_dict:
                        # cache ckeck -- if we already have an entry in the last 6 hours, we skip
                        # Note that it is still in the summed array, so we do not have to worry about it
                        if latlng in self.cached_list:
                            skip_counter += 1
                            continue
                        self.cached_list.add(latlng)

                        lat_lng = latlng.split("_")
                        item = {}
                        item['imerg_id'] = fname
                        item['date'] = acq_time
                        item["latitude"] = float(lat_lng[0])
                        item["longitude"] = float(lat_lng[1])
                        item["cell"] = generate_cell(item["latitude"], item["longitude"])
                        item["precipitation"] = rainfall_dict[latlng]
                        reverse_geocode = rg.search((item['latitude'], item['longitude']))[0]
                        item['place'] = reverse_geocode['name'] + ', ' + reverse_geocode['admin1'] + ', ' + reverse_geocode['cc']
                        item['country'] = reverse_geocode["cc"]
                        imerg_items.append(item)
                        imerg_locations.append(
                            {"name": item['place'], "lat": item["latitude"], "lng": item["longitude"]}
                        )
            else:
                self.messageQueue.put("[imerg] Not downloading. %s already exists"%files[-1].rsplit('/', 1)[-1])
        self.messageQueue.put(
            "[imerg] Obtained IMERG with: %i items and skipped existing %i items for %s" % (len(imerg_items), skip_counter, fname))
        return imerg_items, imerg_locations

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
            select = "SELECT imerg_id FROM HCS_IMERG_LATE where date > %s" % (
                    datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
            cursor.execute(select)
            results = cursor.fetchall()
            cursor.close()
            for row in results:
                cachedlist.add("_".join(row[0].split("_")[-2:]))
            self.messageQueue.put("[imerg] IMERG cachedlist has  %i items in last 2 days" % (len(cachedlist)))
        except Exception as e:
            self.errorQueue.put((self.root_name, str(e)))
            print((self.root_name, str(e)))
        return cachedlist

    def insertIMERG(self, imerg_items):
        cursor = self.DB_CONN.cursor()
        for item in imerg_items:

            insert = 'INSERT INTO HCS_IMERG_LATE ( \
                        imerg_id, \
                        longitude, latitude, date, place, \
                        cell, precipitation, country) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
            params = (item['imerg_id'], item['longitude'], item['latitude'], item['date'],
                      item['place'], item['cell'], item['precipitation'], item['country'])
            try:
                cursor.execute(insert, params)
                self.DB_CONN.commit()
            except Exception as e:
                self.messageQueue.put('[imerg] Failed to insert %s with error %s' % (item["imerg_id"], repr(e)))
        cursor.close()

    def updateRedisLocations(self, imerg_locations, base_url):
        # get REDIS connection
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=pool)
        totalLocations = len(imerg_locations)
        sublocations = 0
        for location in imerg_locations:
            # loc standardize cleans as well...
            location_std = location_standardize(location["name"])
            location_key = high_confidence_streamer_key("imerg-late:location:" + location_std)
            r.set(location_key, location["name"], ex=259200)

            # set location data
            point_str = str(location["lat"]) + "," + str(location["lng"])
            for sublocation in location_std.split(":"):
                sublocationkey = sublocation_key(sublocation)
                r.set(sublocationkey, point_str, ex=259200)
                sublocations += 1
        self.messageQueue.put("[imerg] Completed IMERG redis update with: %i locations and %i sublocations for %s" % (totalLocations, sublocations, base_url))


if __name__ == '__main__':
    assed_config = load_config('../../config/assed_config.json')
    proc = IMERG(assed_config, 'IMERG', multiprocessing.Queue(), multiprocessing.Queue())
    run_sql_file('../../initialization/mysql/imerg_late_sql.SQL', proc.DB_CONN)
    proc.run()
