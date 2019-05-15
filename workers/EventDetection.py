import click
import os, sys, pdb

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
    return """select cell, count(*) from HCS_USGS where time >= {timestamp} and mag > 4 group by cell""".format(timestamp=time_start)

def generate_news_query():
    time_start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    return """select cell, count(*) from HCS_News where timestamp >= {timestamp} group by cell""".format(timestamp=time_start)





def main():

    assed_config = file_utils.load_config("./config/assed_config.json")
    DB_CONN = db_utils.get_db_connection(assed_config)
    cursor = DB_CONN.cursor()
    available_streamers = [item for item in assed_config["SocialStreamers"]]
    streamer_results = {}
    for _streamer_ in available_streamers:
        _query_ = generate_social_query(_streamer_=_streamer_, _topic_="landslide")
        t_result = cursor.execute(_query_)
        streamer_results[_streamer_] = t_result.fetchall()

    _query_ = generate_trmm_query()
    trmm_results = cursor.execute(_query_)

    _query_ = generate_usgs_query()
    usgs_results = cursor.execute(_query_)
    
    _query_ = generate_news_query()
    news_results = cursor.execute(_query_)

    pdb.set_trace()


    

        


    # For event detection, we will check databases, and do stuff with it...

    # Like what?????????
    pass


    # So when feed msa is requested, we just make a call to the database.

    # But we have to replace the 





if __name__ == "__main__":
    main()