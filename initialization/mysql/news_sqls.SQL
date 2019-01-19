/* This sets up stuff for news_downloaders, specifically the timestamp table */
/*NOTE that this does not at all touch litmus tables, i.e. news_litmus */
/* Those have to be created separately */
CREATE TABLE IF NOT EXISTS `news_timestamps` (
  `news_id_local` int(11) NOT NULL AUTO_INCREMENT,
  `source` text NOT NULL,
  `max_ts` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`news_id_local`)
);