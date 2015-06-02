package org.mysqlmv.mvm.mv;

import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

/**
 * Created by I312762 on 6/2/2015.
 */
public class SqlRewriteVisitor extends MySqlOutputVisitor {

    public SqlRewriteVisitor(Appendable appender) {
        super(appender);
    }

    public SqlRewriteVisitor() {
        super(new StringBuilder());
    }

    public String toString() {
        return this.getAppender().toString();
    }
}
