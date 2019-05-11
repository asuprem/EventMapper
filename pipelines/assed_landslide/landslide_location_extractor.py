import utils.AssedMessageProcessor
import time, redis
import pdb
class landslide_location_extractor(utils.AssedMessageProcessor.AssedMessageProcessor):
    def __init__(self):
        self.time = time.time()
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        self.r=redis.Redis(connection_pool = pool)
        self.timecheck = 7200
        self.locations = []
        self.update_location_store()

    def process(self,message):
        
        pdb.set_trace()


    def update_location_store(self,):
        pdb.set_trace()