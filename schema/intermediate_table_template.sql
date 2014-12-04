CREATE TABLE cd_log_test_test_log (
  `id` bigint(20) not null auto_increment,
  `rec_id` int(11) not null,
  `mview_toi_id` int(11),
  `opr_type` tinyint,
  `is_applied` tinyint(1) DEFAULT 0,
  `create_datetime` datetime default null,
  `last_update_time` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  FOREIGN KEY(`mview_toi_id`) REFERENCES `mview_toi`(`mview_toi_id`)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8