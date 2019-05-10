/* This sets up stuff for USGS  */
CREATE TABLE IF NOT EXISTS `HCS_USGS` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `usgs_id` mediumtext NOT NULL,
  `longitude` double NOT NULL,
  `latitude` double NOT NULL,
  `mag` double NOT NULL,
  `time` datetime NOT NULL,
  `place_detailed` mediumtext COMMENT 'geotag - usually city and country',
  `place_general` mediumtext COMMENT 'geotag - usually country, but in case of US it is a state name',
  `latitude_detailed` double DEFAULT NULL,
  `longitude_detailed` double DEFAULT NULL,
  `latitude_general` double DEFAULT NULL,
  `longitude_general` double DEFAULT NULL,
  `geo` tinyint(1) DEFAULT NULL,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);