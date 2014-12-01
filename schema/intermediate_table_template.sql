CREATE TABLE `cd_log_1`(
  id bigint(20) not null auto_increment,
  id int(11) not null,
  `opr_type` varchar(10),
  `mview_toi_id` int(11),
  `is_applied` tinyint(1) DEFAULT 0,
  `create_datetime` datetime default null,
  `last_update_time` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  FOREIGN KEY(`mview_toi_id`) REFERENCES `mview_toi`(`mview_toi_id`)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;