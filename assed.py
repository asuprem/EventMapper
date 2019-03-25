''' ASSED - Adaptive Social Sensor Event Detection 

This is the server for ASSED. Once it begins running, it should begin taking requests from a user.
'''

# General Imports
import os, json

'''

So what do we want ASSED to be?

Is it just a server front end, and the pub/sub is somewhere else?
Or is it also the pub/sub manager?

Assume it is separated - server and pub/sub. How would that work?

Let's begin with architecture

ASSED initializes the Social Streamer application - workers/master_twitter.py --> eventually workers/socialStreamers.py
    master_twitter.py writes stuff to downloads/[STREAMTYPE]_[YEAR]/../../../MINUTE.json

ASSED also initializes the High-Confidence Streamer application
    This needs to be refactored into thhe socialStreamer formar (maybe take a look at how DataSift does their stuff)

'''





def main():


    







if __name__ == "__main__""
    main()

