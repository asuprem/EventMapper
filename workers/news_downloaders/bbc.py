from downloader import NewsDownloader

"""
Implement NewsDownloader from downloader.py with custom functions.
Make sure to name the class [file_name]_downloader --> bbc.py contains class bbc_downloader(NewsDownloader):
"""

class bbc_downloader(NewsDownloader):
    def __init__(self,config):
        super(bbc_downloader,self).__init__(config)
