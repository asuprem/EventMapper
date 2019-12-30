/* This sets up stuff for USGS  */
CREATE TABLE IF NOT EXISTS `HCS_USGS_WILDFIRE` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `usgs_id` varchar(17) NOT NULL,
  `longitude` double NOT NULL,
  `latitude` double NOT NULL,
  `acres` double NOT NULL,
  `time` datetime NOT NULL,
  `place` mediumtext,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);