

## 1. Identify the source

The source is the NASA GPM (Global Precipitation Measurement) satellite collection that measure rainfall across the globe. For rainfall data, we use IMERG (Integrated Multi-satellitE Retrievals for GPM), with details [found at this link](https://gpm.nasa.gov/data/imerg)


## 2. Identify the precedure to download data and extract cell-related content. The following attributes are mandatory: (i) unique identifier, (ii) timestamp, (iii) latitude, (iv) longitude, (v) the event magnitude itself.

The [GPM Data Directory](https://gpm.nasa.gov/data/directory) has a list of links to access IMERG and other products. We are most interested in the IMERG Late Run product. Per the website, the IMERG Late Run is:

> This algorithm is intended to intercalibrate, merge, and interpolate “all” satellite microwave precipitation estimates, together with microwave-calibrated infrared (IR) satellite estimates, precipitation gauge analyses, and potentially other precipitation estimators at fine time and space scales for the TRMM and GPM eras over the entire globe. The system is run several times for each observation time, first giving a quick estimate (IMERG Early Run) and successively providing better estimates as more data arrive (IMERG Late Run). The final step uses monthly gauge data to create research-level products (IMERG Final Run). 

Within the Late Run product, we want to access the PPS Real-Time feed that is updated every 30 minutes with 0.1 degree granularity rainfall conditions across the globe (with the exception of the North and South Poles).

This is available at [this https directory](https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/). One may register for access at [this PPS Registration Link](http://registration.pps.eosdis.nasa.gov/registration/). 


### a. Accessing the directory.

Programmatic access instructions are provided in [this access instruction file](https://gpm.nasa.gov/sites/default/files/2021-01/jsimpsonhttps_retrieval.pdf). 

Basically, with the username and password and URL provided: `https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/`, we navigate to the YYYYMM directory (e.g. `202306` for June), and download the most recent file (by parsing the list of files there). Each file as the following format:

`https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/202306/3B-HHR-L.MS.MRG.3IMERG.20230616-S033000-E035959.0210.V06C.RT-H5`

Here, the `S033000` indicates the time, i.e. 3.30AM, and the 20230616 indicates the date, i.e. June 16, 2023. So a file from 4.00PM from June 4th is named:

`https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/202306/3B-HHR-L.MS.MRG.3IMERG.20230604-S160000-E162959.0960.V06C.RT-H5`

Note that the exact time (E162959) is also listed, which changes the auto naming retrieval.

### b. Downloading the required files

Once we access the directory, we can search for the most recent uploaded file and download it. Can also perform pattern search given the first part of the file name (see above).

Download the file, open with `h5py.File()` in python, and extract the rainfall table from the `precipitationCal` field in the HDF5 file. May convert to np array if needed. The table is a 3600x1800 matrix (3600 longitude values, 1800 latitude values, each spaced every 0.1 degree from -180 to 180, and -90 to 90, respectively). This yields about 6.5M values.

### c. Inserting into database

We already have longitude and latitude of the `precipitationCal` field. We identify the high rainfall regions. We can do this as follows:

1. Run the retrieval program every hour or every thirty minutes. Hour is better given it is in mm/hr
2. Download the most recent rainfall data, and keep it in memory
3. Sum it with the previous data, rolling 6 hour sum
4. In the sum, claculate the highest rainfall cells and push it into DB



## 3. Create a mysql INSERT TABLE statement, using the following template that sets up a table for IMERG rainfall data:

/* This sets up stuff for IMERG  */
CREATE TABLE IF NOT EXISTS `HCS_IMERG_LATE` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `imerg_id` mediumtext NOT NULL COMMENT 'IMERG file name 3B-HHR-L.MS.MRG.3IMERG.20230614-S210000-E212959.1260.V06C.h5',
  `date` datetime NOT NULL,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `precipitation` tinyint(1) DEFAULT '0' COMMENT 'In units of mm per hr over the last 6 hours',
  `place` mediumtext,
  `country` mediumtext NOT NULL,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);

NOTE: add where this should go
NOTE: add what to do (e.g. execute initialize.py mysql thingamazig)

## 4. Next steps -- setting up the profiles

We will need to ensure these changes are propagated properly.

1. How to update High confidence sources to use the new source
2. How to make sure this new information is used in HDI
3. How to make sure this new information is used in ML section
4. How to harmonize this across pipelines (future, maybe.)
   1. For example, rainfall+earthquake is for landslide
   2. Rainfall alone is for flooding...


Additional note: assed_config or high confidence config SHOULD include the password needed to access the source, or some sort of args for input.....



## 5. Additional Notes

To ensure our code runs successfully, we add the following snippet to `config/high_confidence_topics.json` to set up the IMERG downloads:

```
"IMERG":{
        "name": "IMERG",  # Name of the retriever
        "db_name": "IMERG", # Name of DB table
        "source_file":"IMERG", # Name of the source file in workers/HighConfidenceSrc
        "type":"scheduled", # Whether it is a scheduled source or continuous (always running)
        "schedule":1800, # How often, in seconds, should it be executed
        "config":"config/assed_config.json" # Configuration file containig additional parameters
    }
```

Then, in `assed_config`, we add the following to the credentials field to ensure we can download from the IMERG online directory:

```
"credentials":
    {
        "imerg":{
            "username": "user@name.com",
            "password": "user@name.com"
        }
    },
```



