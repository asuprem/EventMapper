import utils.AssedMessageProcessor
import pdb, time, datetime

from gensim.models import KeyedVectors
from gensim.utils import tokenize
import nltk
import keras

from numpy import zeros
import numpy as np

import warnings

import utils.helper_utils as helper_utils
from utils.file_utils import load_config
from utils.db_utils import get_db_connection

import traceback

class landslide_event_detection(utils.AssedMessageProcessor.AssedMessageProcessor):

    def __init__(self,debug=False):
        self.debug = debug
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
        self.total_counter = 0

        self.db_insert = 'INSERT INTO ASSED_Social_Events ( \
        social_id, cell, \
        latitude, longitude, timestamp, link, text, location, topic_name, source, valid, stream_type) \
        VALUES (%s,%s,%s,%s,%s,%s, %s, %s,%s, %s, %s, %s)'

    def process(self,message):
        if time.time() - self.cursor_timer > self.cursor_refresh:
            self.cursor.close()
            self.cursor = self.DB_CONN.cursor()
            self.cursor_timer = time.time()
            helper_utils.std_flush("TRUE: %i\t\tFALSE: %i out of total of %i"%(self.true_counter, self.false_counter, self.total_counter))
            self.total_counter, self.true_counter, self.false_counter = 0, 0, 0
        
        # Get message text
        cleaned_message = str(message["text"].encode("utf-8"))[2:-2]
        encoded_message = self.encode(cleaned_message)

        
        prediction = np.argmax(self.model.predict(np.array([encoded_message]))[0])
        params = None
        if prediction == 1:
            # push to db
            self.true_counter+=1
            params = (message["id_str"], message["cell"], str(message['latitude']), \
                    str(message['longitude']), self.ms_time_convert(message['timestamp']), message["link"], str(message["text"].encode("utf-8"))[2:-2], message["location"], "landslide", "ml", "1", message["streamtype"])

        elif prediction == 0:
            # push to db, with false? push to different db?
            self.false_counter+=1
            params = (message["id_str"], message["cell"], str(message['latitude']), \
                    str(message['longitude']), self.ms_time_convert(message['timestamp']), message["link"], str(message["text"].encode("utf-8"))[2:-2], message["location"], "landslide", "ml", "0", message["streamtype"])
        else:
            warnings.warn("WARNING -- Prediction value of %i is not one of valid predictions [0, 1]"%prediction)
        try:
            if not self.debug:
                self.cursor.execute(self.db_insert, params)
                self.DB_CONN.commit()
            else:
                helper_utils.std_flush(self.db_insert%params)
        except Exception as e:
            traceback.print_exc()
            helper_utils.std_flush('Failed to insert %s with error %s' % (message["id_str"], repr(e)))
            return (False, message)
        
        self.total_counter += 1
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
