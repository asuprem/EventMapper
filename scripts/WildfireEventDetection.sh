#!/bin/sh
cd /expansion1/LITMUS/
if ps up `cat ./logfiles/WildfireEventDetection.pid ` > /dev/null
then
    printf "WildfireEventDetection.py is aleady running\n" >> ./logfiles/WildfireEventDetection.out
else
    printf "WildfireEventDetection is no longer running.\n    Deleting PID file.\n" >> ./logfiles/WildfireEventDetection.out
    rm  ./logfiles/WildfireEventDetection.pid >> ./logfiles/WildfireEventDetection.out
    printf "    Deleted file\n" >> ./logfiles/WildfireEventDetection.out
    printf "Starting WildfireEventDetection.py\n" >> ./logfiles/WildfireEventDetection.out
    nohup ./assed_env/bin/python workers/WildfireEventDetection.py >> ./logfiles/WildfireEventDetection.log 2>&1 &
fi