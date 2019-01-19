from downloader import NewsDownloader
import pdb
"""
Implement NewsDownloader from downloader.py with custom functions.
Make sure to name the class [file_name]_downloader --> bbc.py contains class bbc_downloader(NewsDownloader):
"""

class google_downloader(NewsDownloader):
    def __init__(self,config):
        super(google_downloader,self).__init__(config)
    
    def get_current_id(self,entry):
        return entry['id'].split('feed:')[-1]
     
       

