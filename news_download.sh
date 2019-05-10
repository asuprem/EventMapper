#!/bin/bash
cd /expansion2/LITMUS/workers
#touch logfiles/news_downloader_status.log
nohup ../venv/bin/python news_download.py >> ../logfiles/news_downloader_status.log &
