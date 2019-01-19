import feedparser
import pdb
import time
import MySQLdb as mysql
import json, codecs
from nltk import word_tokenize

class NewsDownloader(object):
    def __init__(self,config):
        self.config=config

    def get_max_timestamp(self, row_name):

        try:
            str_ = row_name+'.txt'
            with open(str,"r") as f:
                timestamp = f.read()
        except:
            timestamp = 0
            str_ = row_name+'.txt'
            with open(str_,"w") as f:
                f.write("%s" %timestamp)
        return int(timestamp)

        #UPDATE TO  THIS ON SERVER
        db = self.get_db_connection()    
        cursor = db.cursor()
        select_s = 'select max_ts from news_timestamps where source=%s'
        parameter=row_name
        cursor.execute(select_s,[parameter])
        timestamp = cursor.fetchall()
        if len(timestamp) == 0:
            timestamp = 0
            insert_s = 'insert into news_timestamps(source,max_ts) values(%s,%s)'
            parameter = (row_name,timestamp)
            cursor.execute(insert_s,parameter)
        cursor.close()
        return int(timestamp[0][0])
            
    def get_db_connection(self,db=None):
        if db==None:
            return mysql.connect(   host=self.config['db_host'], 
                                    port=self.config['db_port'], 
                                    user=self.config['db_user'], 
                                    passwd=self.config['db_passwd'], 
                                    db=self.config['db_db'], 
                                    charset='utf8')
        else:
            return mysql.connect(   host=self.config['db_host'], 
                                    port=self.config['db_port'], 
                                    user=self.config['db_user'], 
                                    passwd=self.config['db_passwd'], 
                                    db=db, 
                                    charset='utf8')


    

    def update_timestamp(self,max_current_ts,row_name):
        str = row_name+'.txt'
        with open(str,"w") as fp:
            fp.write("%s" %max_current_ts)
        return
        
        db = self.get_db_connection()
        cursor = db.cursor()
        update_s = 'update news_timestamps set max_ts=%s where source=%s'
        parameter = (max_current_ts,row_name)
        cursor.execute(update_s,parameter)
        cursor.close()
        


    
    def get(self,url,params=['Africa','AfricaBBC','landslide','en']):
        '''
            url -> This is the RSS feed url as a string
            params -> ['DEF_L','ROW_N','D_TYPE','LANG']
                DEF_L --> default location


        '''
        default_location = params[0]
        row_name = params[1]
        disaster_type = params[2]
        lang = params[3]

        max_ts = self.get_max_timestamp(row_name)
        max_current_ts = 0
        try:
            data = feedparser.parse(url)
        except:
            return {}
        data_struct={}
        #iterate through retrieved results
        for entry in data['entries']:
            this_timestamp = self.get_published_timestamp(entry)
            if this_timestamp <= max_ts or this_timestamp is None:
                continue
            title = self.get_title(entry)
            title_tokens = word_tokenize(title)
            
            search_counter = 0
            search_flag = False
            #check if sarch terms exist in title <-- simpe checker
            #iterate throught list of keywords w/ search_counter, flag positives with search_flag
            while not search_flag and search_counter < len(self.config['keyws'][disaster_type]['langs']['en']):
                if self.config['keyws'][disaster_type]['langs']['en'][search_counter] in title_tokens:
                    search_flag = True
                search_counter+=1
            if not search_flag:
                continue
            current_id = self.get_current_id(entry)
            data_struct[current_id] = self.build_struct(this_timestamp, current_id, title, default_location,disaster_type,entry)
            
            if len(data_struct[current_id]['location']) <= 1 or len(data_struct[current_id]['location']) >= 150:
                data_struct[current_id]['location'] = default_location
            if this_timestamp > max_current_ts:
                max_current_ts = this_timestamp

        if max_current_ts > 0:
            self.update_timestamp(max_current_ts, row_name)

        return data_struct

    '''
    Edit these in subclasses
    '''

    def get_current_id(self,entry):
        return entry['link']
    def build_struct(self,this_timestamp, current_id, title, default_location,disaster_type,entry):
        struct = {}
        struct['timestamp'] = this_timestamp
        struct['id'] = current_id
        struct['title'] = title
        struct['link'] = entry['link']
        struct['short'] = entry['summary'].split('<div')[0]
        struct['location'] = default_location
        struct['disasterType'] = disaster_type
        return struct
    def get_title(self,entry):
        return entry['title']
    def get_published_timestamp(self,entry):
        if entry['published_parsed'] == "" or entry['published_parsed'] is None:
            return None
        return int(time.mktime(entry['published_parsed'])*1000)
        
        


