from downloader import NewsDownloader

"""
Implement NewsDownloader from downloader.py with custom functions.
Make sure to name the class [file_name]_downloader --> bbc.py contains class bbc_downloader(NewsDownloader):
"""

class nbc_downloader(NewsDownloader):
    def __init__(self,config):
        super(nbc_downloader,self).__init__(config)

