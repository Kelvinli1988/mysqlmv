CREATE TABLE `cd_log_1`(
  id bigint(20) not null auto_increment,
/* primary key*/
  `opr_type` enum('INSERT', 'UPDATE', 'DELETE'),
  `is_applied` tinyint(1) DEFAULT 0,
  `create_datetime` datetime default null,
  `last_update_time` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;