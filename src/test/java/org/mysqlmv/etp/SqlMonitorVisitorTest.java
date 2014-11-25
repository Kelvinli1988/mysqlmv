package org.mysqlmv.etp;

import org.mysqlmv.etp.mv.SqlMonitorVisitor;
import org.testng.annotations.Test;

/**
 * Created by Kelvin Li on 11/24/2014 1:31 PM.
 */
public class SqlMonitorVisitorTest {
    @Test
    public void testMointor() {
        StringBuilder sb = new StringBuilder();
        SqlMonitorVisitor visitor = new SqlMonitorVisitor(sb);
        visitor.addFunction();
    }
}
