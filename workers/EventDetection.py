#import click
import os, sys, pdb, redis, json

import utils.db_utils as db_utils
import utils.helper_utils as helper_utils
import utils.file_utils as file_utils

from datetime import datetime, timedelta


def generate_social_query(_streamer_, _topic_):
    time_start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

    query = """select twitter_cell, hdi_twitter_cell_count, ml_twitter_cell_count

from

(select coalesce(hdi_twitter_cell, ml_twitter_cell) as twitter_cell, hdi_twitter_cell_count, ifnull(ml_twitter_cell_count, 0) as ml_twitter_cell_count from

(select cell as hdi_twitter_cell, count(*) as hdi_twitter_cell_count
from ASSED_Social_Events where streamtype='{streamer}' and source = 'hdi' and topic_name='{topic}' and timestamp >= {timestamp}
group by cell having hdi_twitter_cell_count > 5)  twitter_hdi 

left join 

(select cell as ml_twitter_cell, count(*)*0.34 as ml_twitter_cell_count
from ASSED_Social_Events where streamtype='{streamer}' and source = 'ml' and topic_name='{topic}' and timestamp >= {timestamp}
group by cell having ml_twitter_cell_count > 1)  twitter_ml

on twitter_hdi.hdi_twitter_cell = twitter_ml.ml_twitter_cell) left_join

union

select coalesce(hdi_twitter_cell, ml_twitter_cell) as twitter_cell, ifnull(hdi_twitter_cell_count, 0) as hdi_twitter_cell_count, ml_twitter_cell_count from

(select cell as hdi_twitter_cell, count(*) as hdi_twitter_cell_count
from ASSED_Social_Events where streamtype='{streamer}' and source = 'hdi' and topic_name='{topic}' and timestamp >= {timestamp}
group by cell having hdi_twitter_cell_count > 5)  twitter_hdi 

right join 

(select cell as ml_twitter_cell, count(*)*0.34 as ml_twitter_cell_count
from ASSED_Social_Events where streamtype='{streamer}' and source = 'ml' and topic_name='{topic}' and timestamp >= {timestamp}
group by cell having ml_twitter_cell_count > 1)  twitter_ml

on twitter_hdi.hdi_twitter_cell = twitter_ml.ml_twitter_cell;""".format(streamer=_streamer_, topic= _topic_, timestamp=time_start)
    
    return query

def generate_trmm_query():
    time_start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    return """select cell, count(*) from HCS_TRMM where date >= {timestamp} group by cell""".format(timestamp=time_start)

def generate_usgs_query():
    time_start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    return """select cell, count(*) from HCS_USGS where time >= {timestamp} and mag >= 5 group by cell""".format(timestamp=time_start)

def generate_news_query():
    time_start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    return """select cell, count(*) from HCS_News where timestamp >= {timestamp} group by cell""".format(timestamp=time_start)





def main():

    helper_utils.std_flush("Initializing EventDetection -- %s"%helper_utils.readable_time())
    cell_cache = {}

    assed_config = file_utils.load_config("./config/assed_config.json")

    helper_utils.std_flush("Obtained DB Connection")
    DB_CONN = db_utils.get_db_connection(assed_config)
    cursor = DB_CONN.cursor()
    
    available_streamers = [item for item in assed_config["SocialStreamers"]]
    streamer_results = {}
    helper_utils.std_flush("Available streamers: %s"%str(available_streamers))

    for _streamer_ in available_streamers:
        helper_utils.std_flush("Generating query for: %s"%_streamer_)
        _query_ = generate_social_query(_streamer_=_streamer_, _topic_="landslide")
        cursor.execute(_query_)
        streamer_results[_streamer_] = cursor.fetchall()
        helper_utils.std_flush("Obtained results for : %s"%_streamer_)

    helper_utils.std_flush("Generating query for: %s"%"TRMM")
    _query_ = generate_trmm_query()
    cursor.execute(_query_)
    trmm_results = cursor.fetchall()
    helper_utils.std_flush("Obtained resuts for: %s"%"TRMM")

    helper_utils.std_flush("Generating query for: %s"%"USGS")
    _query_ = generate_usgs_query()
    cursor.execute(_query_)
    usgs_results = cursor.fetchall()
    helper_utils.std_flush("Obtained resuts for: %s"%"USGS")
    
    helper_utils.std_flush("Generating query for: %s"%"News")
    _query_ = generate_news_query()
    cursor.execute(_query_)
    news_results = cursor.fetchall()
    helper_utils.std_flush("Obtained resuts for: %s"%"News")
    cursor.close()

    helper_utils.std_flush("Generating local cache with scoring:\tSocial-ML - 0.3\tSocial-HDI - 1\tNews - 3\tUSGS - 5\tTRMM - 1")
    # Scoring -- Twitter-Social: 0.3    Twitter-HDI - 1     News:       3       USGS:   5       TRMM:   1
    for _streamer_ in streamer_results:
        helper_utils.std_flush("Local caching for %s"%_streamer_)
        for tuple_cell_ in streamer_results[_streamer_]:
            _cell_ = tuple_cell_[0]
            if _cell_ not in cell_cache:
                cell_cache[_cell_] = {}
            cell_cache[_cell_][_streamer_+"-hdi"]=(float(tuple_cell_[1]), float(tuple_cell_[1]))
            cell_cache[_cell_][_streamer_+"-ml"]=(float(tuple_cell_[2]), float(tuple_cell_[2]))

    helper_utils.std_flush("Local caching for %s"%"TRMM")
    for tuple_cell_ in trmm_results:
        _cell_ = tuple_cell_[0]
        if _cell_ not in cell_cache:
            cell_cache[_cell_] = {}
        cell_cache[_cell_]["TRMM"] = (float(tuple_cell_[1]), float(tuple_cell_[1]*1))   # 1 <-- TRMM score
    
    helper_utils.std_flush("Local caching for %s"%"USGS")
    for tuple_cell_ in usgs_results:
        _cell_ = tuple_cell_[0]
        if _cell_ not in cell_cache:
            cell_cache[_cell_] = {}
        cell_cache[_cell_]["USGS"] = (float(tuple_cell_[1]), float(tuple_cell_[1]*5))

    helper_utils.std_flush("Local caching for %s"%"News")
    for tuple_cell_ in news_results:
        _cell_ = tuple_cell_[0]
        if _cell_ not in cell_cache:
            cell_cache[_cell_] = {}
        cell_cache[_cell_]["News"] = (float(tuple_cell_[1]), float(tuple_cell_[1]*3))

    helper_utils.std_flush("Local cache score total generation")
    for _cell_ in cell_cache:
        cell_cache[_cell_]["total"] = sum([cell_cache[_cell_][item][1] for item in cell_cache[_cell_]])

    
    
    pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
    r=redis.Redis(connection_pool = pool)
    helper_utils.std_flush("Connected to Redis")

    # Correct-key -- v1 or v2
    # Key Push
    # Actual keys...
    list_tracker_key = "assed:event:detection:multisource:listkey"
    list_push_key = "assed:event:detection:multisource:list"
    list_info_key = "assed:event:detection:multisource:info"

    key_version = r.get(list_tracker_key)
    if key_version is None:
        key_version = "v2"
    else:
        key_version = key_version.decode()
    push_key = 'v1'
    if key_version == 'v1':
        helper_utils.std_flush("v1 key already in effect. Pushing to v2")
        push_key = 'v2'
    else:
        helper_utils.std_flush("v2 key already in effect. Pushing to v1")


    cell_list = [item for item in cell_cache]
    true_list_push_key = list_push_key + ":" + push_key
    helper_utils.std_flush("Deleting existing %s, if any"%true_list_push_key)
    r.delete(true_list_push_key)

    r.lpush(true_list_push_key, *cell_list)
    helper_utils.std_flush("Pushed cell list to %s"%true_list_push_key)

    helper_utils.std_flush("Pushing individual cell results")
    cell_counter = 0
    for _cell_ in cell_cache:
        cell_push_contents = json.dumps(cell_cache[_cell_])
        cell_specific_suffix = ":".join(_cell_.split("_"))
        cell_push_key = ":".join([list_info_key, cell_specific_suffix, push_key])
        r.set(cell_push_key, cell_push_contents)
        if cell_counter == 0:
            helper_utils.std_flush("First push: %s --- %s"%(cell_push_key, cell_push_contents))
        cell_counter += 1

    helper_utils.std_flush("Completed individual cell pushes with %s cells"%str(cell_counter))

    r.set(list_tracker_key, push_key)
    helper_utils.std_flush("Setting versioning in %s to %s"%(list_tracker_key, push_key))

    helper_utils.std_flush("--------   COMPLETE AT  %s ----------"%helper_utils.readable_time())

if __name__ == "__main__":
    main()