#!/bin/sh
#cd /expansion2/LITMUS/
cd "/mnt/d/OneDrive - Georgia Institute of Technology/Projects/gatech/LITMUS/aibek/Final/LITMUS"
if ps up `cat ./logfiles/streamCollector.pid ` > /dev/null
then
    printf "streamCollector.py is aleady running\n" >> ./logfiles/twitterProcessor_sh.out
else
    printf "streamCollector is no longer running.\n    Deleting PID file.\n" >> ./logfiles/twitterProcessor_sh.out
    rm  ./logfiles/streamCollector.pid >> ./logfiles/twitterProcessor_sh.out
    printf "    Deleted file\n" >> ./logfiles/twitterProcessor_sh.out
    printf "Starting streamCollector.py\n" >> ./logfiles/twitterProcessor_sh.out
    #nohup ./venv/bin/python workers/streamCollector.py >> ./logfiles/streamerProcessor.log 2>&1 &
    nohup python workers/streamCollector.py >> ./logfiles/streamerProcessor.log 2>&1 &
fi