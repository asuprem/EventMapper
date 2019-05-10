from downloader import NewsDownloader
import pdb

"""
Implement NewsDownloader from downloader.py with custom functions.
Make sure to name the class [file_name]_downloader --> bbc.py contains class bbc_downloader(NewsDownloader):
"""

class bing_downloader(NewsDownloader):
    def __init__(self,config):
        super(bing_downloader,self).__init__(config)
    '''
    def get_published_timestamp(self,entry):
        pdb.set_trace()
        if entry['published_parsed'] == "" or entry['published_parsed'] is None:
            return None
        return int(time.mktime(entry['published_parsed'])*1000)
    '''