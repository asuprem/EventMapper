# ASSED - Adaptive Social Sensor Event Detection

ASSED's processes include the following:
 - Social-sensor streamers
 - High-confidence streamers
 - Event Identification
 - Metadata Extraction
 - Heterogenous Data Integration
 - Event Detection (ML Section)
    - Filter Updates
    - Filter Generation

# ASSED Dependencies

ASSED requires the following

- **Apache Kafka**: This is the pub-sub system ASSED uses to manage intermediate data
    - **Zookeeper** - Kafka requires Apache Zookeeper. 
- **Redis**: Used for M_STORE (but maybe just use Kafka???)

## ASSED Initializations

ASSED initializes the following services during startup

1. Start up Zookeeper
2. Start up Kafka
3. Start up a REDIS database (unsure - maybe use Kafka for M_STORE as well?)


# ASSED Process initializations

ASSED goes through the following steps, detailed for each process.

## General Process Configuration

A process's configuration is stored as a JSON file. A process can have multiple export keys if it has several sub-applications that do things

    {
        "NAME": [Process name],
        "TYPE": [Scheduled | Continuous],
        "SCHEDULE": [SECOND-based DELAY]
        "EXPORT-KEY(s)": ["KEY1","KEY2",...,],
        "IMPORT-KEY(s)": ["KEY1","KEY2","KEY3",...],
        "EXECUTE":["python", "envName"],
    }

## a. Social Sensor Streamers: 

1. Launch streamer
    - Streamer loads config file
    - Streamer launches process for each source (Twitter, Facebook, etc)
    - Streamer checks for crashes in streams, and restarts when they fail
    - Streamer checks for config changes, and restarts streamers
    - Streamer checks if new files are not being created, and restarts
    - Streamer registers child processes in log file

2. Check for Streamer status (avoid lagged flush - immediate flush)
3. If Streamer crashes, shutdown child processes, if any
4. Restart Streamer

## b. High Confidence Streamer

1. Launch Streamer
    - Same as social sensor streamers

2. Check for Streamer status
3. If Streamer crashes, shutdown child processes, if any
4. Restart Streamer

## c. Event Identification

1. Launch Identifier
    - Identifier waits for new deliveries from ASSED with import key
    - Look through EP config files for list of rules, and apply them
    - For events, send to PE_RDB
    - Publish metadata to M_Store

2. Perform process checks (as above)

TODO - how to set up rules for ASSED?
Rules need to be pure CEP AND ML-CEP hybrids
Integrating StreamingCEPs?

## d. Metadata Extraction

Types of metadata - geographic location, time of post, post credibility, 

1. Launch main metadata extraction process
    - config file contains list of metadata extractors?
    - Each metadata extractor config contains:

            {"NAME": Name of extractor
             "APPLICATION":path to application
             "EXEC": How to execute application (for now, basic - env in python)
             "IMPORT-KEY" - social streamer import (wildcard...)
             "EXPORT-KEY" - for output (focus on single meta extractor for now)}
    - Execute metadata-extractor code
        - This further requires M_STORE
        - For locs identified, publish to M_Store

    - Continue processing data as it comes using ASSED API (?)
    - Processed data is sent to kafka


## e. Heterogenous Data Integration
This is an important part of ASSED. Combines info from HCEP and SSEP. This labels SSEP Data, and is dependent on HCEP data

1. Launch HDI Process (continuous)
    - Two consumers - SSEP and HCEP consumer
    - SSEP Consumer:
        - For each new social post from MetadataExtraction
        - find any event in PE_RDB matching metadata with fuzzy join (longitude w/in 50km and time w/in 3 days)
        - THIS NEEDS TO BE FAST WITHOUT OVERLOADING...
        - schema matching (????) - not done YET
        - ADD to social sensor Data Lake (with flag for LABELED, UNLABELED)
        - Published unlabeled, labeled to R_STORE
    - HCEP Consumer
        - For each physical event from CEP, 
        - Future data will be matched correctly using SSEP consumer
        - backlogged data matched here
        - query Data Lake for UNLABELED social sensor within 50km, within the past three days

2. Maintain HDI Process
3. If crashes, identify last key taken, and go from there

## f. ML-based EP (MLEP)

This is another important part of ASSED. MLEP performs filter generation, filter updates, and filtering.

1. Launch MLEP process
    - Get Unlabeled, Labeled from HDI 
    - getTopKFilters(Unlabeled/Labeled)
    - classifyUnlabeled(Unlabaled, [topK], "weighting")
        - Unweighted, weighted, model-weighted
        - driftDetectUpdate([topK])
    - scheduledFilterGenerate()
        - requestData(time)
        - createFilter()
    - scheduledFilterUpdate()
        - requestData(time)
        - data.forEach(getFilters(_topK) -> _topK.forEach(updateWithData(data)))
    - DriftFilterUpdate(topK)
        - requestData(sinceLastDriftUpdate)
        - 
    - updateFilters(Labeled)
    - 




