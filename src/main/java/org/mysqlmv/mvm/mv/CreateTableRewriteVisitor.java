package org.mysqlmv.mvm.mv;

import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

/**
 * Created by I312762 on 6/2/2015.
 */
public class CreateTableRewriteVisitor extends MySqlOutputVisitor {

    static StringBuilder sb = new StringBuilder();

    public CreateTableRewriteVisitor(Appendable appender) {
        super(appender);
    }

    public CreateTableRewriteVisitor() {
        super(sb);
    }

    public String toString() {
        return sb.toString();
    }
}
