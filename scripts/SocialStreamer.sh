#!/bin/sh
cd /expansion1/LITMUS/
if ps up `cat ./logfiles/SocialStreamer.pid ` > /dev/null
then
    printf "SocialStreamer.py is aleady running\n" >> ./logfiles/SocialStreamer.out
else
    printf "SocialStreamer is no longer running.\n    Deleting PID file.\n" >> ./logfiles/SocialStreamer.out
    rm  ./logfiles/SocialStreamer.pid >> ./logfiles/SocialStreamer.out
    printf "    Deleted file\n" >> ./logfiles/SocialStreamer.out
    printf "Starting SocialStreamer.py\n" >> ./logfiles/SocialStreamer.out
    nohup ./assed_env/bin/python workers/SocialStreamer.py >> ./logfiles/SocialStreamer.log 2>&1 &
fi