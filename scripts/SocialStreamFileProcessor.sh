#!/bin/sh
cd /expansion1/LITMUS/
if ps up `cat ./logfiles/SocialStreamFileProcessor.pid ` > /dev/null
then
    printf "SocialStreamFileProcessor.py is aleady running\n" >> ./logfiles/SocialStreamFileProcessor.out
else
    printf "SocialStreamFileProcessor is no longer running.\n    Deleting PID file.\n" >> ./logfiles/SocialStreamFileProcessor.out
    rm  ./logfiles/SocialStreamFileProcessor.pid >> ./logfiles/SocialStreamFileProcessor.out
    printf "    Deleted file\n" >> ./logfiles/SocialStreamFileProcessor.out
    printf "Starting SocialStreamFileProcessor.py\n" >> ./logfiles/SocialStreamFileProcessor.out
    nohup ./assed_env/bin/python workers/SocialStreamFileProcessor.py >> ./logfiles/SocialStreamFileProcessor.log 2>&1 &
fi