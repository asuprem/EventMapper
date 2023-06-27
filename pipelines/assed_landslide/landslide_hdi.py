import utils.AssedMessageProcessor
import pdb, time, datetime

import utils.helper_utils as helper_utils
from utils.file_utils import load_config, topic_indexer
from utils.db_utils import get_db_connection

import traceback
import MySQLdb as mdb

class landslide_hdi(utils.AssedMessageProcessor.AssedMessageProcessor):

    def __init__(self,debug=False):
        self.debug = debug
        self.config = load_config("./config/assed_config.json")
        self.topic_index = topic_indexer(self.config, "landslide")
        self.DB_CONN = get_db_connection(self.config)
        self.cursor = self.DB_CONN.cursor()
        pass

        self.cursor_timer = time.time()

        self.cursor_refresh = 300
        self.MS_IN_DAYS = 86400
        self.true_counter = 0
        self.unk = 0
        self.stream_tracker = {}

    def process(self,message):
        if message["streamtype"] not in self.stream_tracker:
            self.stream_tracker[message["streamtype"]] = {}
            self.stream_tracker[message["streamtype"]]["hdi"] = 0
            self.stream_tracker[message["streamtype"]]["non_hdi"] = 0
            self.stream_tracker[message["streamtype"]]["totalcounter"] = 0
        self.stream_tracker[message["streamtype"]]["totalcounter"] += 1


        if time.time() - self.cursor_timer > self.cursor_refresh:
            self.cursor.close()
            self.cursor = self.DB_CONN.cursor()
            self.cursor_timer = time.time()
            for _streamtype in self.stream_tracker:
                utils.helper_utils.std_flush("[%s] -- Processed %i elements from %s with %i HDI  and %i NONHDI"%(helper_utils.readable_time(), self.stream_tracker[_streamtype]["totalcounter"],_streamtype, self.stream_tracker[_streamtype]["hdi"], self.stream_tracker[_streamtype]["non_hdi"]))
                self.stream_tracker[_streamtype]["totalcounter"] = 0
                self.stream_tracker[_streamtype]["non_hdi"] = 0
                self.stream_tracker[_streamtype]["hdi"] = 0
        if self.debug:
            utils.helper_utils.std_flush("Processed %i elements from %s with %i HDI and %i NONHDI"%(self.stream_tracker[message["streamtype"]]["totalcounter"],message["streamtype"], self.stream_tracker[message["streamtype"]]["hdi"], self.stream_tracker[message["streamtype"]]["non_hdi"]))
        # Check 

        # Check item
        self.verify_message(message)
        message["cell"] = utils.helper_utils.generate_cell(float(message["latitude"]), float(message["longitude"]))
        _time_ = int(int(message["timestamp"])/1000)
        _time_minus = self.time_convert(_time_ -  6*self.MS_IN_DAYS)
        _time_plus = self.time_convert(_time_ +  3*self.MS_IN_DAYS)
        select_s = 'SELECT location from HCS_News where cell = %s and timestamp > %s and timestamp < %s and topic_name=%s'
        params = (message["cell"], _time_minus, _time_plus, str(self.topic_index))
        utils.helper_utils.std_flush("Performing query: %s"%(select_s%params))
        self.cursor.execute(select_s, params)
        results = self.cursor.fetchall()
        if len(results) > 0:
            #helper_utils.std_flush("True Event found for %s"%str(message["text"].encode("utf-8"))[2:-2])
            self.true_counter+=1
            # Push into landslide events...
            insert = 'INSERT INTO ASSED_Social_Events ( \
                        social_id, cell, \
                        latitude, longitude, timestamp, link, text, location, topic_name, source, valid, streamtype) \
                        VALUES (%s,%s,%s,%s,%s,%s, %s, %s,%s, %s, %s, %s)'
            params = (str(message["id_str"]), message["cell"], str(message['latitude']), \
                    str(message['longitude']), self.ms_time_convert(message['timestamp']), message["link"], str(message["text"].encode("utf-8"))[2:-2], message["location"], "landslide", "hdi", "1", message["streamtype"])

            #helper_utils.std_flush(insert%params)
            
            try:
                if not self.debug:
                    self.cursor.execute(insert, params)
                    self.DB_CONN.commit()
                else:
                    #helper_utils.std_flush(insert%params)
                    pass
                helper_utils.std_flush("[%s] -- Possible landslide event at %s detected at time %s using HDI (current time: %s)"%(helper_utils.readable_time(), message["location"], self.ms_time_convert(message["timestamp"]), self.time_convert(time.time())))
                self.stream_tracker[message["streamtype"]]["hdi"] += 1
                return (False, message)
            except mdb._exceptions.Error as mdb_error:
                traceback.print_exc()
                true_mdb_error = eval(str(mdb_error))
                if true_mdb_error[0] == 2013 or true_mdb_error[0] == 2006:   # This is database connection error
                    raise RuntimeError("[%s] -- ERROR -- Cannot connect to MySQL Database. Shutting down."%helper_utils.readable_time())    
                helper_utils.std_flush('[%s] -- ERROR -- Failed to insert %s with error %s' % (helper_utils.readable_time(), message["id_str"], repr(mdb_error)))
        else:
            # No matching HDI
            pass
            
        """
        tODO
        also perform event detection on other data (just news data (already exists), combination of earthquake AND TRMM (???))

        """
        
        if self.debug:
            #helper_utils.std_flush("No HDI detected for %s - %s - %s"%(str(message["id_str"]),str(message["text"].encode("utf-8"))[2:-2], message["cell"] ))
            pass
        self.stream_tracker[message["streamtype"]]["non_hdi"] += 1
        return (True,message)

    def time_convert(self,timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    def ms_time_convert(self,timestamp):
        return datetime.datetime.fromtimestamp(int(int(timestamp)/1000)).strftime("%Y-%m-%d %H:%M:%S")

    def verify_message(self,msg):
        if "timestamp" not in msg:
            msg["timestamp"] = time.time()*1000
        if "streamtype" not in msg:
            msg["streamtype"] = "twitter"
        if "link" not in msg:
            if msg["streamtype"] == "twitter":
                msg["link"] = "https://twitter.com/"+msg['user']["screen_name"]+"/status/"+msg["id_str"]
        
