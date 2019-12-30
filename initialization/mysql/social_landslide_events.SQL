/* This sets up stuff for USGS  */
CREATE TABLE IF NOT EXISTS `ASSED_Social_Events` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `social_id` varchar(22) NOT NULL,
  `cell` mediumtext NOT NULL,
  `longitude` double NOT NULL,
  `latitude` double NOT NULL,
  `location` mediumtext NOT NULL,
  `link` mediumtext NOT NULL,
  `text` mediumtext NOT NULL,
  `timestamp` datetime NOT NULL,
  `topic_name` varchar(20) NOT NULL,
  `source` varchar(10) Not NULL default 'hdi',
  `valid` boolean not null default 1,
  `streamtype` varchar(15) not null default 'twitter',
  PRIMARY KEY (`db_id`)
);