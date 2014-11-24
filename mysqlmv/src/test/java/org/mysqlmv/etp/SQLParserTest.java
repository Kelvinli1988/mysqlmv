package org.mysqlmv.etp;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Kelvin Li on 11/19/2014 1:53 PM.
 */
public class SQLParserTest {
    @Test
    public void parse() {
        List<SQLStatement> sqlStmtList = SQLUtils.parseStatements("select `test`.`test_log`.`id` AS `id`,`test`.`test_log`.`name` AS `name` from `test`.`test_log` where (`test`.`test_log`.`id` > 3)", JdbcConstants.MYSQL);
        Assert.assertEquals(sqlStmtList.size(), 0);
    }
}
