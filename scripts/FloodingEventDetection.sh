#!/bin/sh
cd /expansion1/LITMUS/
if ps up `cat ./logfiles/FloodingEventDetection.pid ` > /dev/null
then
    printf "FloodingEventDetection.py is aleady running\n" >> ./logfiles/FloodingEventDetection.out
else
    printf "FloodingEventDetection is no longer running.\n    Deleting PID file.\n" >> ./logfiles/FloodingEventDetection.out
    rm  ./logfiles/FloodingEventDetection.pid >> ./logfiles/FloodingEventDetection.out
    printf "    Deleted file\n" >> ./logfiles/FloodingEventDetection.out
    printf "Starting FloodingEventDetection.py\n" >> ./logfiles/FloodingEventDetection.out
    nohup ./assed_env/bin/python workers/FloodingEventDetection.py >> ./logfiles/FloodingEventDetection.log 2>&1 &
fi