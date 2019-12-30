/* This sets up stuff for MODIS  */
CREATE TABLE IF NOT EXISTS `HCS_MODIS_WILDFIRE` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `modis_id` varchar(40) NOT NULL,
  `longitude` double NOT NULL,
  `latitude` double NOT NULL,
  `brightness` double NOT NULL,
  `bright_t31` double NOT NULL,
  `acq_time` datetime NOT NULL,
  `place` mediumtext NOT NULL,
  `track` double NOT NULL,
  `satellite` varchar(1) NOT NULL,
  `confidence` int(3) NOT NULL,
  `frp` double NOT NULL,
  `daynight` varchar(1) NOT NULL,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);