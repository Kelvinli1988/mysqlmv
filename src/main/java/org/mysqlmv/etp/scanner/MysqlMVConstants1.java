package org.mysqlmv.etp.scanner;

/**
 * Created by Kelvin Li on 12/1/2014 10:17 AM.
 */
public class MysqlMVConstants1 {
    public static final String TABLE_NAME_FORMAT = "cd_log_%s_%s";

    public static final String CREATE_TOI_TEMPLATE = "CREATE TABLE `" + TABLE_NAME_FORMAT + "`(" +
            "  `id` bigint(20) not null auto_increment," +
            "  `rec_id` int(11) not null," +
            "  `mview_toi_id` int(11)," +
            "  `opr_type` varchar(10)," +
            "  `is_applied` tinyint(1) DEFAULT 0," +
            "  `create_datetime` datetime default null," +
            "  `last_update_time` timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP," +
            "  PRIMARY KEY (`id`)," +
            "  FOREIGN KEY(`mview_toi_id`) REFERENCES `mview_toi`(`mview_toi_id`)" +
            ")ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
}
