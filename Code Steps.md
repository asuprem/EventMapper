# Introduction
This details steps about various components in the LITMUS pipeline

# Workers
LITMUS has several worker scripts that perform download, location extraction, NER, and a variety of tasks. These are all stored in workers. The cron jobs target these workers on a regular basis to populated the LITMUS database.

# Workers - Downloaders
LITMUS is extensible with downloaders. These downloaders collect raw data streams into several folders. There is a consistent naming convention to these folders.

For now we have downloaders separately. The goall is to create an overarching LITMUS program that uses `config.json` to control the entire infrastructure/system flow.

(Not yet) NOTE - workers-downloaders MUST contain a symbolic link to `utils.py` in the parent directory

NOTE - All downloads are stored in the 'downloads' folder in top-level directory

## News Downloaders
`config.json` provides a list of disaster keywords we are targeting (i.e. landslides, flooding, etc). This is extensible and can be modified over time.

`news_download.py` uses list of news sources fromm `config.json` (actually, pointers to files containing such lists) to perform downloads. The list of sources for each disaster defined in `config.json` should be in `config\News\[d_type].json`, where **d_type** is the disaster type. Note that this *json* file just contains a list of sources, and no other configuration details as yet.

### CRON Job
The cron line for news downloader is as follows:

    */20 * * * * sh /expansion2/LITMUS/news_download.sh

This indicates the downloader is run every 20 minutes. The bash script `download_news.sh` performs top level tasks for news download.


