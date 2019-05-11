import utils.AssedMessageProcessor
import time, redis
import pdb
class landslide_location_extractor(utils.AssedMessageProcessor.AssedMessageProcessor):
    def __init__(self):
        self.time = time.time()
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        self.r=redis.Redis(connection_pool = pool)
        self.timecheck = 7200
        self.locations = {}
        self.update_location_store()

    def process(self,message):
        
        pdb.set_trace()


    def update_location_store(self,):
        self.locations = {}
        for _key in self.r.scan_iter(match="assed:sublocation:*", count=500):
            # keep only the first key location
            key_location = _key.decode("utf-8").split("assed:sublocation:")[1]
            key_coords = self.r.get(_key).decode("utf-8").split(",")
            latitude = float(key_coords[0])
            longitude = float(key_coords[1])
            self.locations[key_location] = (latitude, longitude)
        pdb.set_trace()