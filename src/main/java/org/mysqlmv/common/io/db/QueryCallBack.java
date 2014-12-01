package org.mysqlmv.common.io.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/1/2014 11:05 AM.
 */
public abstract class QueryCallBack<T> {
    protected ResultSet rs;
    public abstract String getSql();
    public abstract T doInCallback(PreparedStatement pstmt) throws SQLException;
    public ResultSet getResultSet() {
        return rs;
    }
}
