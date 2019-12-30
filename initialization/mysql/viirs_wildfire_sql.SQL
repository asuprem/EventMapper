/* This sets up stuff for VIIRS  */
CREATE TABLE IF NOT EXISTS `HCS_VIIRS_WILDFIRE` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `viirs_id` varchar(40) NOT NULL,
  `longitude` double NOT NULL,
  `latitude` double NOT NULL,
  `bright_ti4` double NOT NULL,
  `bright_ti5` double NOT NULL,
  `acq_time` datetime NOT NULL,
  `place` mediumtext NOT NULL,
  `track` double NOT NULL,
  `satellite` varchar(1) NOT NULL,
  `confidence` varchar(7) NOT NULL,
  `frp` double NOT NULL,
  `daynight` varchar(1) NOT NULL,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);