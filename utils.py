# -*- coding: utf-8 -*-
import json, MySQLdb as mysql, re, sys
from datetime import datetime
import codecs, logging, os.path

fileName = 'config.json'
config = json.load(codecs.open(fileName, encoding='utf-8'))

def load_config(config_file='config.json'):
    return json.load(codecs.open(config_file, encoding='utf-8'))
    

def get_db_connection(config,db=None):
    if db==None:
        return mysql.connect(host=config['db_host'], port=config['db_port'], user=config['db_user'], passwd=config['db_passwd'], db=config['db_db'], charset='utf8')
    else:
        return mysql.connect(host=config['db_host'], port=config['db_port'], user=config['db_user'], passwd=config['db_passwd'], db=db, charset='utf8')
def run_sql_file(filename, connection):
    '''
    The function takes a filename and a connection as input
    and will run the SQL query on the given connection  
    '''    
    file = open(filename, 'r')
    sql = s = " ".join(file.readlines())
    cursor = connection.cursor()
    cursor.execute(sql)    
    connection.commit()