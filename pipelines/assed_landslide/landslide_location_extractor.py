import utils.AssedMessageProcessor
import time, redis
import pdb
from sner import Ner
import nltk
import utils.helper_utils

class landslide_location_extractor(utils.AssedMessageProcessor.AssedMessageProcessor):
    def __init__(self):
        self.time = time.time()
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        self.r=redis.Redis(connection_pool = pool)
        self.timecheck = 7200
        self.locations = {}
        self.update_location_store()
        self.NER =  Ner(host="localhost", port=9199)
        self.counter = 0
        self.memory={}

    def process(self,message):
        if time.time() - self.time > self.timecheck:
            self.update_location_store()
        
        # First location tagging to get locations...
        cleaned_message = str(message["text"].encode("utf-8"))[2:-2]
        cleaned_message = " ".join(nltk.tokenize.word_tokenize(cleaned_message))
        loc_tags = self.NER.get_entities(cleaned_message)
        desc_locations = self.extractLocations(loc_tags)
        locations = " ".join(desc_locations) if len(desc_locations) > 0 else None
        latitude = None
        longitude = None

        if locations is None:
            # Attempt match...
            for sublocations in self.locations:
                if sublocations in cleaned_message:
                    locations = sublocations
                    latitude = self.locations[sublocations][0]
                    longitude = self.locations[sublocations][1]
                    break
        else:
            # This is number of location items...
            pass

            #utils.helper_utils.std_flush(self.counter)
                    
        if locations is None:
            return (False, message)

        # location is there, we will attempt geocoding right here... right now... right on this ship
        # With sublocations...
        if latitude is None or longitude is None:
            standardized_location = utils.helper_utils.location_standardize(locations)

            for sublocation in standardized_location.split(":"):
                if sublocation in self.locations:
                    latitude = self.locations[sublocation][0]
                    longitude = self.locations[sublocation][1]
            
        
        message["location"] = locations
        if latitude is not None and longitude is not None:
            message["latitude"] = str(latitude)
            message["longitude"] = str(longitude)
        else:
            #pass
            #
            #
            # Attempt 
            
            if message["location"] in self.memory:
                pass
            else:
                utils.helper_utils.std_flush(utils.helper_utils.location_standardize(message["location"]), message["location"])
                #self.counter+=1
                #utils.helper_utils.std_flush(self.counter)
                self.memory[message["location"]] = 1





        return (True, message)
        


    def update_location_store(self,):
        self.locations = {}
        for _key in self.r.scan_iter(match="assed:sublocation:*", count=500):
            # keep only the first key location
            key_location = _key.decode("utf-8").split("assed:sublocation:")[1]
            if key_location.strip():
                key_coords = self.r.get(_key).decode("utf-8").split(",")
                latitude = float(key_coords[0])
                longitude = float(key_coords[1])
                self.locations[key_location] = (latitude, longitude)


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