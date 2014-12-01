package org.mysqlmv.common.io.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Kelvin Li on 11/24/2014 3:41 PM.
 */
public class DBUtil {
    public static Integer getLastInsertedID() throws SQLException {
        String sql = "SELECT LAST_INSERT_ID();";
        PreparedStatement stmt = ConnectionUtil.getConnection().prepareStatement(sql);
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        while(rs.next()) {
            return rs.getInt(1);
        }
        return null;
    }

    public static Object executeInPrepareStmt(QueryCallBack cb) throws SQLException {
        PreparedStatement pstmt = ConnectionUtil.getConnection().prepareStatement(cb.getSql());
        Object result = cb.doInCallback(pstmt);
        ResultSet rs = cb.getResultSet();
        if(rs != null) {
            rs.close();
        }
        pstmt.close();
        return result;
    }
}
