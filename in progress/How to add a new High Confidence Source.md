

1. Identify the source
2. Identify the precedure to download data and extract cell-related content. The following attributes are mandatory:
	a. a unique identifier
	b. a timestamp
	c. a latitude
	d. longitude
	e. the event magnitude itself. may be a number, text, classification. Make a note of it.

3. Create a mysql INSERT TABLE statement, using the following template that sets up a table for IMERG rainfall data:

/* This sets up stuff for IMERG  */
CREATE TABLE IF NOT EXISTS `HCS_IMERG_LATE` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `imerg_id` mediumtext NOT NULL COMMENT 'IMERG file name 3B-HHR-L.MS.MRG.3IMERG.20230614-S210000-E212959.1260.V06C.h5',
  `date` datetime NOT NULL,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `precipitation` tinyint(1) DEFAULT '0' COMMENT 'In units of mm per hr',
  `place` mediumtext,
  `country` mediumtext NOT NULL,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);


4. ---
5. Currently, need to restart the HighConfidenceStreamer.py using the .sh activator (scripts/HighConfidenceStreamer.sh) to propagate config changes...