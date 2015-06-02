package org.mysqlmv.mvm;

import org.mysqlmv.common.exception.CDCException;
import org.mysqlmv.mvm.sql.CreateTableSqlModifier;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by I312762 on 6/1/2015.
 */
public class MVMonitorTest {

    @BeforeClass
    public void switchFile()  {

    }

    @Test
    public void testMVMonitor() throws CDCException {
        CreateTableSqlModifier modifier = new CreateTableSqlModifier();
        String sql = "CREATE TABLE `student` (`id` int(11) NOT NULL,`name` varchar(50) DEFAULT NULL,PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=utf8";
        modifier.modify(sql);
        System.out.println(modifier.getResult());

    }
}
