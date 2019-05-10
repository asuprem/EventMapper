CREATE TABLE IF NOT EXISTS `HCS_News` (
  `db_id` int(11) NOT NULL AUTO_INCREMENT,
  `item_id` mediumtext,
  `topic_name` int not null,
  `link` mediumtext,
  `cell` varchar(9) NOT NULL,
  `timestamp` datetime NOT NULL,
  `text` mediumtext,
  `news_src` mediumtext,
  `location` mediumtext,
  `latitude` double not Null, 
  `longitude` double not null,
  PRIMARY KEY (`db_id`)
)
