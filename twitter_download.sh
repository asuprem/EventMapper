#!/bin/bash
cd /expansion2/LITMUS/
if ps up `cat ./logfiles/master_twitter.pid ` > /dev/null
then
   echo "Already is running" >> ./logfiles/download_twitter_sh.out
   # Do something knowing the pid exists, i.e. the process with $PID is running
else
   rm  ./logfiles/master_twitter.pid >> ./logfiles/download_twitter_sh.out
   echo "uh oh. not running started running" >> ./logfiles/download_twitter_sh.out
   nohup ./venv/bin/python workers/master_twitter.py >> ./logfiles/streamers.log &
fi
