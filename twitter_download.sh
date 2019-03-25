#!/bin/sh
cd /expansion2/LITMUS/
if ps up `cat ./logfiles/master_twitter.pid ` > /dev/null
then
    printf "master_twitter.py is aleady running\n" >> ./logfiles/download_twitter_sh.out
else
    printf "master_twitter is no longer running.\n    Deleting PID file.\n" >> ./logfiles/download_twitter_sh.out
    rm  ./logfiles/master_twitter.pid >> ./logfiles/download_twitter_sh.out
    printf "    Deleted file\n" >> ./logfiles/download_twitter_sh.out
    printf "Starting master_twitter.py\n" >> ./logfiles/download_twitter_sh.out
    nohup ./venv/bin/python workers/master_twitter.py >> ./logfiles/streamers.log 2>&1 &
fi