package org.mysqlmv.mvm.sql;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * Created by Kelvin Li on 6/2/2015.
 */
public interface SqlModifier {
    public SqlModifier modify(String sql);

    public String getResult();

}
