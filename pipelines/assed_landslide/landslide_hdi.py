import utils.AssedMessageProcessor
import pdb, time, datetime

import utils.helper_utils as helper_utils
from utils.file_utils import load_config
from utils.db_utils import get_db_connection

class landslide_hdi(utils.AssedMessageProcessor.AssedMessageProcessor):

    def __init__(self):
        self.config = load_config("./config/assed_config.json")
        self.DB_CONN = get_db_connection(self.config)
        self.cursor = self.DB_CONN.cursor()
        pass

        self.cursor_timer = time.time()

        self.cursor_refresh = 300
        self.MS_IN_DAYS = 86400000


    def process(self,message):
        if time.time() - self.cursor_timer > self.cursor_refresh:
            self.cursor.close()
            self.cursor = self.DB_CONN.cursor()
        # Check 

        # Check item
        apikey = self.config["APIKEYS"]["googlemaps"]
        helper_utils.lookup_address_only_DEBUG(message["location"], apikey)
        message["cell"] = utils.helper_utils.generate_cell(float(message["latitude"]), float(message["longitude"]))
        _time_ = int(int(message["timestamp"])/1000)
        _time_minus = self.time_convert(_time_ -  6*self.MS_IN_DAYS)
        _time_plus = self.time_convert(_time_ +  3*self.MS_IN_DAYS)
        select_s = 'SELECT location from HCS_News where cell = %s and timestamp > %s and timestamp < %s'
        params = (message["cell"], _time_minus, _time_plus)
        self.cursor.execute(select_s, params)
        results = self.cursor.fetchall()
        if len(results) > 0:
            helper_utils.std_flush("True Event found for %s"%str(message["text"].encode("utf-8"))[2:-2])


        return (False,message)

    def time_convert(self,timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")