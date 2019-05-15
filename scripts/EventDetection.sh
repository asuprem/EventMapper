#!/bin/sh
cd /expansion1/LITMUS/
if ps up `cat ./logfiles/EventDetection.pid ` > /dev/null
then
    printf "EventDetection.py is aleady running\n" >> ./logfiles/EventDetection.out
else
    printf "EventDetection is no longer running.\n    Deleting PID file.\n" >> ./logfiles/EventDetection.out
    rm  ./logfiles/EventDetection.pid >> ./logfiles/EventDetection.out
    printf "    Deleted file\n" >> ./logfiles/EventDetection.out
    printf "Starting EventDetection.py\n" >> ./logfiles/EventDetection.out
    nohup ./assed_env/bin/python workers/EventDetection.py >> ./logfiles/EventDetection.log 2>&1 &
fi