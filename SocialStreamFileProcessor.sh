#!/bin/sh
#cd /expansion2/LITMUS/
cd "/mnt/d/OneDrive - Georgia Institute of Technology/Projects/gatech/LITMUS/aibek/Final/LITMUS"
if ps up `cat ./logfiles/SocialStreamFileProcessor.pid ` > /dev/null
then
    printf "SocialStreamFileProcessor.py is aleady running\n" >> ./logfiles/SocialStreamFileProcessor.out
else
    printf "SocialStreamFileProcessor is no longer running.\n    Deleting PID file.\n" >> ./logfiles/SocialStreamFileProcessor.out
    rm  ./logfiles/SocialStreamFileProcessor.pid >> ./logfiles/SocialStreamFileProcessor.out
    printf "    Deleted file\n" >> ./logfiles/SocialStreamFileProcessor.out
    printf "Starting SocialStreamFileProcessor.py\n" >> ./logfiles/SocialStreamFileProcessor.out
    #nohup ./venv/bin/python workers/SocialStreamFileProcessor.py >> ./logfiles/SocialStreamFileProcessor.log 2>&1 &
    nohup python workers/SocialStreamFileProcessor.py >> ./logfiles/SocialStreamFileProcessor.log 2>&1 &
fi