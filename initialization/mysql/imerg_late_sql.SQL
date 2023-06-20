/* This sets up stuff for IMERG  */
CREATE TABLE IF NOT EXISTS `HCS_IMERG_LATE` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `imerg_id` mediumtext NOT NULL COMMENT 'IMERG file name 3B-HHR-L.MS.MRG.3IMERG.20230614-S210000-E212959.1260.V06C.h5',
  `date` datetime NOT NULL,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `precipitation` float DEFAULT '0' COMMENT 'In units of mm per hr',
  `place` mediumtext,
  `country` mediumtext NOT NULL,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);