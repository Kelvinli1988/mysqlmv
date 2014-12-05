package org.mysqlmv.common.util.db;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/5/2014 1:34 PM.
 */
public class DBUtil {
    public static Logger logger = org.slf4j.LoggerFactory.getLogger(DBUtil.class);

    public static Integer getLastInsertedID() {
        return (Integer) executeInPrepareStmt(new QueryCallBack<Integer>() {
            @Override
            public String getSql() {
                return "SELECT LAST_INSERT_ID()";
            }

            @Override
            public Integer doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.execute();
                rs = pstmt.getResultSet();
                while (rs.next()) {
                    return rs.getInt(1);
                }
                return null;
            }
        });
    }

    public static<T> T  executeInPrepareStmt(QueryCallBack<T> cb) {
        T result = null;
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
            try {
                if (rs != null && !rs.isClosed()) {
                    rs.close();
                }
                if (pstmt != null && !pstmt.isClosed()) {
                    pstmt.close();
                }
            } catch (SQLException e1) {
            }
        }
        return result;
    }
}
