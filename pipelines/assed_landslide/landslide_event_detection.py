import utils.AssedMessageProcessor
import pdb, time, datetime

from gensim.models import KeyedVectors
from gensim.utils import tokenize
import nltk
import keras

from numpy import zeros
import numpy as np


import utils.helper_utils as helper_utils
from utils.file_utils import load_config
from utils.db_utils import get_db_connection

import traceback

class landslide_event_detection(utils.AssedMessageProcessor.AssedMessageProcessor):

    def __init__(self):
        self.config = load_config("./config/assed_config.json")

        self._encoder = KeyedVectors.load_word2vec_format('./pipelines/assed_landslide/ml/encoders/GoogleNews-vectors-negative300.bin', binary=True, unicode_errors='ignore', limit=100000)
        self.zero_v = zeros(shape=(300,))
        self.model = keras.models.load_model("./pipelines/assed_landslide/ml/models/tf_model.h5")

        self.DB_CONN = get_db_connection(self.config)
        self.cursor = self.DB_CONN.cursor()
        pass

        self.cursor_timer = time.time()

        self.cursor_refresh = 300
        self.true_counter = 0
        self.false_counter = 0
        self.tseliot = open("positive.txt","w")
        self.poe = open("negative.txt","w")

    def process(self,message):
        if time.time() - self.cursor_timer > self.cursor_refresh:
            self.cursor.close()
            self.cursor = self.DB_CONN.cursor()
            self.cursor_timer = time.time()
        
        # Get message text
        cleaned_message = str(message["text"].encode("utf-8"))[2:-2]
        encoded_message = self.encode(cleaned_message)

        #pdb.set_trace()
        
        prediction = np.argmax(self.model.predict(np.array([encoded_message]))[0])

        if prediction == 1:
            # push to db
            #self.true_counter+=1
            self.tseliot.write(cleaned_message+"\n")

        elif prediction == 0:
            # push to db, with false? push to different db?
            self.poe.write(cleaned_message+"\n")
            #self.false_counter+=1
        
        helper_utils.std_flush("TRUE: %i\t\tFALSE: %i"%(self.true_counter, self.false_counter))
        
        results = []
        if len(results) > 0:
            #helper_utils.std_flush("True Event found for %s"%str(message["text"].encode("utf-8"))[2:-2])
            self.true_counter+=1
            # Push into landslide events...
            insert = 'INSERT INTO ASSED_Social_Events ( \
                        social_id, cell, \
                        latitude, longitude, timestamp, link, text, location, topic_name) \
                        VALUES (%s,%s,%s,%s,%s,%s, %s, %s,%s)'
            params = (message["id_str"], message["cell"], str(message['latitude']), \
                    str(message['longitude']), self.ms_time_convert(message['timestamp']), message["link"], str(message["text"].encode("utf-8"))[2:-2], message["location"], "landslide")

            #helper_utils.std_flush(insert%params)
            
            try:
                self.cursor.execute(insert, params)
                self.DB_CONN.commit()
                helper_utils.std_flush("Possible landslide event at %s detected at time %s using HDI (current time: %s)"%(message["location"], self.ms_time_convert(message["timestamp"]), self.time_convert(time.time())))
            except Exception as e:
                traceback.print_exc()
                helper_utils.std_flush('Failed to insert %s with error %s' % (message["id_str"], repr(e)))
                return (False, message)
        else:
            pass


        return (False,message)

    def time_convert(self,timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    def ms_time_convert(self,timestamp):
        return datetime.datetime.fromtimestamp(int(int(timestamp)/1000)).strftime("%Y-%m-%d %H:%M:%S")

    def encode(self, data):
        """ data MUST be a string """
        tokens = list(nltk.tokenize.word_tokenize(data))
        # this is for possibly empty tokens
        transformed_data = zeros(shape=(300,))
        if not tokens:
            pass
        else:
            for word in tokens:
                transformed_data += self._encoder[word] if word in self._encoder else self.zero_v
            transformed_data/=len(tokens)
        return transformed_data
