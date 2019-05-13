/* This sets up stuff for USGS  */
CREATE TABLE IF NOT EXISTS `ASSED_Social_Events` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `cell` mediumtext NOT NULL,
  `longitude` double NOT NULL,
  `latitude` double NOT NULL,
  `link` mediumtext NOT NULL,
  `text` mediumtext NOT NULL,
  `timestamp` datetime NOT NULL
  PRIMARY KEY (`db_id`)
);