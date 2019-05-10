# Introduction
This details steps about various components in the LITMUS pipeline

# Workers
LITMUS has several worker scripts that perform download, location extraction, NER, and a variety of tasks. These are all stored in workers. The cron jobs target these workers on a regular basis to populated the LITMUS database.

# Workers - Downloaders
LITMUS is extensible with downloaders. These downloaders collect raw data streams into several folders. There is a consistent naming convention to these folders.

Since this performs Physical Event detection, we have several topics, for which we have several keywords for basic social streamer search. For example, for the topic `landslide`, have provided keywords for various languages below:

    "landslide": {
            "en": ["landslide", "mudslide", "landslides", "mudslides", "rockslide", "rockslides", "rockfall", "landslip", "flooding"],
            "pt": ["deslizamento", "desmoronamento", "desabamento"],
            "ja": ["土砂崩れ", "山崩れ", "土石流", "地滑り"],
            "zh": ["滑坡", "塌方", "泥石流", "坍塌"],
            "hi": ["भूस्खलन"],
            "ru": ["сель", "селевой", "селевые", "селевого", "селевых", "оползень", "оползня", "оползнем", "оползни", "оползневый", "оползневые", "оползней"],
            "it": ["frana", "fango"]
        },

NOTE - workers-downloaders MUST contain a symbolic link to `utils.py` in the parent directory

NOTE - All downloads are stored in the 'downloads' folder in top-level directory

## High Confidence Streamers (News Downloaders)
`config.json` provides a list of disaster keywords we are targeting (i.e. landslides, flooding, etc). This is extensible and can be modified over time.

`news_download.py` uses list of news sources fromm `config.json` (actually, pointers to files containing such lists) to perform downloads. The list of sources for each disaster defined in `config.json` should be in `config\News\[d_type].json`, where **d_type** is the disaster type. Note that this *json* file just contains a list of sources, and no other configuration details as yet.

## Social Source Streamers (Twitter Downloader)
    $ sh twitter_download.sh


# File Processors
Since most social streams only allow a single endoint for streaming in non-enterprise grade access, and we work within the free tier, our social streamer performs 'unstructured' download. That is to say, the streamer downloads posts for all keywords for all topics, but the file processor actually bins them to their respective locations



### CRON Job
The cron line for workers is as follows:

    */10 * * * * sh /expansion2/LITMUS/news_download.sh
    */120 * * * * sh /expansion2/LITMUS/twitter_download.sh
    */120 * * * * sh /expansion2/LITMUS/twitter_processor.sh

This indicates the downloader is run every 20 minutes. The bash script `download_news.sh` performs top level tasks for news download.

The cron is located in /var/spool/cron/afnu6


