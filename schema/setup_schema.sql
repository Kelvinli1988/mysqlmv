create database mysqlmv;
use mysqlmv;
DROP TABLE IF EXISTS `mview`;
CREATE TABLE `mview` (
  `mview_id` int(11) NOT NULL AUTO_INCREMENT,
  `mview_name` varchar(50) default NULL,
  `mview_schema` varchar(50) default NULL,
  `mview_setup_finished` tinyint default 0,
  `mview_enabled` tinyint(1) default 0,
  `mview_last_refresh` datetime default NULL,
  /*`mview_refresh_period` int(11) default '86400',*/
  `is_complete` tinyint default false,
  `mview_engine` enum('MyISAM','InnoDB') default 'InnoDB',
  `mview_definition` varchar(20000),
#   `incremental_hwm` bigint(20) default NULL,
#   `refreshed_to_uow_id` bigint(20) default NULL,
  `parent_mview_id` int null,
#   `created_at_signal_id` bigint null,
  `create_datetime` datetime default null,
  `last_update_timestamp` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`mview_id`),
  UNIQUE KEY `mview_name` (`mview_name`,`mview_schema`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `mview_expression`;
CREATE TABLE `mview_expression` (
  `mview_expression_id` int(11) NOT NULL AUTO_INCREMENT,
  `mview_id` int(11),
  `mview_expr_type` varchar(128), /*enum('GROUP','SUM','AVG','COUNT','MIN','MAX','WHERE','PRIMARY','KEY','COLUMN','COUNT_DISTINCT','STDDEV_POP','VAR_POP','STDDEV_SAMP','VAR_SAMP','BIT_AND','BIT_OR','BIT_XOR', 'PERCENTILE','UNIQUE') DEFAULT 'GROUP',*/
  `mview_expression` varchar(10000),
  `mview_expr_alias` varchar(100) default NULL,
  `mview_expr_order` int(11) default '999',
  `create_datetime` datetime default null ,
  PRIMARY KEY  (`mview_expression_id`),
  UNIQUE KEY `mview_id` (`mview_id`,`mview_expr_alias`),
  KEY `mview_id_2` (`mview_id`, `mview_expr_order`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

/**
tables interest materialized view.
short for "table of interest"
 */
DROP TABLE IF EXISTS `mview_toi`;
CREATE TABLE `mview_toi`(
  `mview_toi_id` int(11) NOT NULL AUTO_INCREMENT ,
  `mview_id` int(11) NOT NULL ,
  `schema_name` varchar(100),
  `table_name` varchar(100),
  `alias` varchar(100),
  `setup_finished` tinyint(1) default 0,
  `create_datetime` DATETIME DEFAULT NULL,
  `last_update_datetime` DATETIME DEFAULT NULL,
  PRIMARY KEY (`mview_toi_id`)
) ENGINE=InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `mview_refresh_log`;
CREATE TABLE `mview_refresh_log` (
  `mview_refresh_log_id` int(11) NOT NULL AUTO_INCREMENT,
  `mview_id` int(11) not null default 0,
  `refresh_datetime` datetime default null,
  `message` varchar(10000) default null,
  PRIMARY KEY (`mview_refresh_log_id`)
) ENGINE=InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `mview_refresh_scheduler`;
CREATE TABLE `mview_refresh_scheduler` (
  `mview_id` int(11) NOT NULL,
  `apply_interval_seconds` int(11) DEFAULT NULL,
  `last_apply_elapsed_seconds` int(11) DEFAULT '0',
  `last_applied_at` datetime DEFAULT NULL,
  `rec_update_datetime` DATETIME DEFAULT NULL,
  PRIMARY KEY (`mview_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `mview_event_log`;
CREATE TABLE `mview_event_log` (
  `event_log_id` int(11) NOT NULL AUTO_INCREMENT,
  `mview_id` int(11),
  `message` varchar(10000),
  `last_read_time` timestamp not null default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`event_log_id`)
)ENGINE=InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `bin_log_file_logger`;
CREATE TABLE `bin_log_file_logger` (
  `logger_id` int(11) NOT NULL AUTO_INCREMENT,
  `log_file_name` varchar(100) not null,
  `start_read_datetime` datetime,
  `rotate_datatime` DATETIME,
  `last_read_time` timestamp not null default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `last_pointer` BIGINT,
  PRIMARY KEY (`logger_id`)
)ENGINE=InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET=utf8;
