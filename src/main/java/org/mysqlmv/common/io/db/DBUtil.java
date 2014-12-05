package org.mysqlmv.common.io.db;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 11/24/2014 3:41 PM.
 */
public class DBUtil {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(DBUtil.class);

    public static Integer getLastInsertedID() throws SQLException {
        String sql = "SELECT LAST_INSERT_ID();";
        PreparedStatement stmt = ConnectionUtil.getConnection().prepareStatement(sql);
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            return rs.getInt(1);
        }
        return null;
    }

    public static Object executeInPrepareStmt(QueryCallBack cb) {
        Object result = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = ConnectionUtil.getConnection().prepareStatement(cb.getSql());
            result = cb.doInCallback(pstmt);
            rs = cb.getResultSet();
            if (rs != null) {
                rs.close();
            }
            pstmt.close();
        } catch (SQLException e) {
            logger.error("Meet SQL Exception.");
            logger.error(ExceptionUtils.getStackTrace(e));
            try{
                if(rs != null && !rs.isClosed()) {
                    rs.close();
                }
                if(pstmt != null && !pstmt.isClosed()) {
                    pstmt.close();
                }
            } catch (SQLException e1) {
            }
        }
        return result;
    }
}
