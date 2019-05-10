/* This sets up stuff for TRMM  */
CREATE TABLE IF NOT EXISTS `HCS_TRMM` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `trmm_id` mediumtext NOT NULL COMMENT 'composed field: date_E_N',
  `line` mediumtext NOT NULL COMMENT 'full line from the report',
  `date` datetime NOT NULL,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `very_likely` tinyint(1) DEFAULT '0',
  `place` mediumtext,
  `country` mediumtext NOT NULL,
  `cell` varchar(9) NOT NULL,
  PRIMARY KEY (`db_id`)
);