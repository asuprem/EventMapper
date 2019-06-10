""" This file containts constants used throughout ASSED. """

ASSED_CONFIG = "config/assed_config.json"

# Path to topics file which contains all topics and keywords for each topic
TOPIC_CONFIG_PATH = 'config/assed_config.json'


# ------------------------------------------------------
# CHECK IF CONFIG FILE HAS CHANGED

# Time in seconds when a process checks if its configuration file has changed
CONFIG_TIME_CHECK = 60*2

# CONFIG_TIME_CHECK for the SocialStreamer (master_twitter)
SOCIAL_STREAMER_CONFIG_TIME_CHECK = CONFIG_TIME_CHECK
# CONFIG_TIME_CHECK for the StreamFilesProcessor (streamCollector)
STREAM_COLLECTOR_CONFIG_TIME_CHECK = CONFIG_TIME_CHECK
# CONFIG_TIME_CHECK for the High Confidence Streamer (streamCollector)
HCS_CONFIG_TIME_CHECK = 60*60*6

# ------------------------------------------------------
# CHECK IF FILES ARE BEING CREATED

# Seconds between checking if files are being created for a processor
FILE_TIME_CHECK = 60*15
# Flag for file check

# FILE_TIME_CHECK and FIRST_FILE_CHECK for social streamer
SOCIAL_STREAMER_FILE_TIME_CHECK = 60*5

# Number of files to check before declaring Streamer is no longer working
SOCIAL_STREAMER_FILE_CHECK_COUNT = 3

# Maximum delay StreamFilesProcessor lags behind SocialStreamer (master_twitter)
STREAM_FILES_PROCESSOR_MAX_SECOND_DELAY = 60*4

# ------------------------------------------------------
# CHECK IF CRASHED

# Delay in seconds before a master checks if a child has died
CRASH_TIME_CHECK = 60*10

# Delay in seconds before a SocialStreamer master checks if a child (TweetProcess) has died
SOCIAL_STREAMER_CRASH_TIME_CHECK = 60*10

# Time in seconds StreamProcessor delays relaunching a StreamFilesProcessor when files are not available
STREAM_PROCESSOR_POSTPONE_SECONDS = 60*60

# Granularity in seconds for StreamFilesProcessor timeDelta
STREAMING_GRANULARITY_SECONDS = 60

# ------------------------------------------------------
# HIGH CONFIDENCE STREAMER

HIGH_CONFIDENCE_CONFIG_PATH = "config/high_confidence_topics.json"