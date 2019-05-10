#!/bin/sh
cd /expansion1/LITMUS/
if ps up `cat ./logfiles/HighConfidenceStreamer.pid ` > /dev/null
then
    printf "HighConfidenceStreamer.py is aleady running\n" >> ./logfiles/HighConfidenceStreamer.out
else
    printf "HighConfidenceStreamer is no longer running.\n    Deleting PID file.\n" >> ./logfiles/HighConfidenceStreamer.out
    rm  ./logfiles/HighConfidenceStreamer.pid >> ./logfiles/HighConfidenceStreamer.out
    printf "    Deleted file\n" >> ./logfiles/HighConfidenceStreamer.out
    printf "Starting HighConfidenceStreamer.py\n" >> ./logfiles/HighConfidenceStreamer.out
    nohup ./assed_env/bin/python workers/HighConfidenceStreamer.py >> ./logfiles/HighConfidenceStreamer.log 2>&1 &
fi