/* This sets up stuff for USGS  */
CREATE TABLE IF NOT EXISTS `tweets` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `social_id` int(22),
  `text` mediumtext NOT NULL,
  `link` mediumtext NOT NULL,
  `timestamp` datetime NOT NULL,
  PRIMARY KEY (`db_id`)
);