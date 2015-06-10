create database mysqlmv;
use mysqlmv;
DROP TABLE IF EXISTS `mview`;
CREATE TABLE `mview` (
  `mview_id` SERIAL PRIMARY KEY,
  `mview_name` varchar(50) default NULL,
  `mview_schema` varchar(50) default NULL,
  `mview_status` tinyint default 0,
  `mview_last_refresh` datetime default NULL,
  /*`mview_refresh_period` int(11) default '86400',*/
  `refresh_type` enum('Increamental', 'complete') default 'Increamental',
  # Only InnoDB currently, will extend later.
  `mview_engine` enum('InnoDB') default 'InnoDB',
  `mview_definition` varchar(20000),
#   `incremental_hwm` bigint(20) default NULL,
#   `refreshed_to_uow_id` bigint(20) default NULL,
  `parent_mview_id` int null,
#   `created_at_signal_id` bigint null,
  `create_datetime` datetime default null,
  `last_update_timestamp` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  UNIQUE KEY `mview_name` (`mview_name`,`mview_schema`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `mview_table_marker`;
CREATE TABLE `mview_table_marker`(
  `id` SERIAL PRIMARY KEY,
  `table_schema` varchar(50),
  `table_name` varchar(50),
  `last_update_timestamp` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  UNIQUE KEY `mview_name` (`table_schema`,`table_name`)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `mview_delta_mapping`;
CREATE TABLE `mview_delta_mapping`(
  `id` SERIAL PRIMARY KEY,
  `mview_id` BIGINT UNSIGNED,
  `table_marker_id` BIGINT UNSIGNED,
  FOREIGN KEY(`mview_id`) REFERENCES `mview`(`mview_id`),
  FOREIGN KEY(`table_marker_id`) REFERENCES `mview_table_marker`(`id`)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `mview_expression`;
CREATE TABLE `mview_expression` (
  `id` SERIAL PRIMARY KEY,
  `mview_id` bigint UNSIGNED,
  `type` varchar(128), /* select, from, where, join, group by*/
  `expression` varchar(1024),
  # for from clause.
  `table_owner` varchar(1024),
  `table_name` varchar(1024),
  `table_alias` varchar(1024),
  # for join
  `join_type` varchar(128), /* INNER JOIN, LEFT JOIN, RIGHT JOIN, OUTER JOIN*/
  `join_left` bigint UNSIGNED,
  `join_right` bigint UNSIGNED,
  `expr_order` int(11) default '999',
  `last_update_timestamp` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `parameter_group`;
CREATE TABLE `parameter_group`(
  `id` SERIAL PRIMARY KEY,
  `create_datetime` datetime default null
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `parameter_list`;
CREATE TABLE `parameter_list` (
  `id` SERIAL PRIMARY KEY,
  `group_id` bigint UNSIGNED,
  `table_owner` varchar(512),
  `table_name` varchar(512),
  `from` bigint default(-1),
  `to` bigint default(-1),
  `table_order` int,
  `create_datetime` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP
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
