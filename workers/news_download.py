import sys
import time
import os, json, codecs
from datetime import datetime
import pdb
# do the PID check from download_twitter.py here if needed


def load_config(config='config.js'):
    cf = json.load(codecs.open(config, encoding='utf-8'))
    return cf
        

if __name__ == '__main__':

    start_time = time.time()
    config = load_config('../config.json')

    #begin disaster loop:
    for keyword in config['keyws']:

        if config['keyws'][keyword]['news_valid'] == 0:
            continue
        #extract source_file for current diasters
        source_data = {}
        source_list = config['keyws'][keyword]['news_source']
        #extract sources from source list
        with open ('../'+source_list, 'r') as sources:
            for line in sources:
                lines_json = json.loads(line)
                source_data[lines_json['name']] = lines_json
        
        # now we have a list of sources
        #for eaach source, we'll import the neccesary library and use the getFeeds to get the titles...?
        write_data = {}
        for source in source_data:
            #source is the name field
            #if source_data[source]['library'] == "landslide":
            #so this does 'from news_downloaders.google import google_downloader <-- google_downloader is the class'
            print "Retrieving: ", source, "       using library: ", source_data[source]['library'], " on ", datetime.now()
            feed_reader = __import__('news_downloaders.' + source_data[source]['library'],fromlist=[source_data[source]['library']+'_downloader'])
            feed_reader = getattr(feed_reader,source_data[source]['library']+'_downloader')
            feed_read = feed_reader(config)
            data = feed_read.get(source_data[source]['url'], params=source_data[source]['parameters'])
            #here we clear save the data:
            #no need for time check I think

            #create directory
            date = datetime.utcnow()
            dir_ = os.path.join('%s_%s_%s' % ('../downloads/news', keyword, date.year), '%02d' % date.month, '%02d' %date.day, '%02d' % date.hour)
            path = os.path.join(dir_, '%02d.json' % date.minute)
            #create directory if it doesn't exist yet
            try:
                os.makedirs(dir_)
            except:
                pass
            #bugfix got rid of empty files yay!
            output = open(path, 'a')
            data_write = '\n'.join([json.dumps(data[item]) for item in data])
            output.write(data_write)
            output.close()


