package org.mysqlmv.cd.dao;

import org.mysqlmv.cd.logevent.LogFileStatus;
import org.mysqlmv.common.util.db.DBUtil;
import org.mysqlmv.common.util.db.QueryCallBack;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/5/2014 2:30 PM.
 */
public class CdDao {

    public static String findCurrentLogFileName() {
        return DBUtil.executeInPrepareStmt(new QueryCallBack<String>() {
            @Override
            public String getSql() {
                return "select * from bin_log_file_logger order by logger_id desc limit 1";
            }

            @Override
            public String doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.execute();
                rs = pstmt.getResultSet();
                rs.next();
                return rs.getString("log_file_name");
            }
        });
    }

    public static LogFileStatus findLogFileStatus() {
        return DBUtil.executeInPrepareStmt(new QueryCallBack<LogFileStatus>() {
            @Override
            public String getSql() {
                return "select * from bin_log_file_logger order by logger_id desc limit 1";
            }
            @Override
            public LogFileStatus doInCallback(PreparedStatement pstmt) throws SQLException {
                rs = pstmt.executeQuery();
                if(!rs.next()) return null;
                return new LogFileStatus(rs.getString("log_file_name"),
                        rs.getLong("last_pointer"), rs.getInt("logger_id"));
            }
        });
    }

    public static LogFileStatus insertNewFileStatus(final String fileName, final long lastPointer) {
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "insert into bin_log_file_logger(log_file_name, start_read_datetime, rotate_datatime, last_pointer) values(?, now(), null, ?)";
            }
            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setString(1, fileName);
                pstmt.setLong(2, lastPointer);
                pstmt.execute();
                return null;
            }
        });
        int id = DBUtil.getLastInsertedID();
        return new LogFileStatus(fileName, lastPointer, id);
    }

    public static void updateLogFileStatus(final int currentLogRecordId, final long lastPointer) throws SQLException {
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "update bin_log_file_logger set last_pointer = ? where logger_id = ?";
            }
            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setLong(1, lastPointer);
                pstmt.setInt(2, currentLogRecordId);
                pstmt.executeUpdate();
                return null;
            }
        });
    }

    public static void updateLogFileRotate(final long date) {
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "update bin_log_file_logger set rotate_datatime = ? where logger_id in(select logger_id from bin_log_file_logger order by logger_id desc limit 1)";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                Date rotateData = new Date(date);
                pstmt.setDate(1, rotateData);
                pstmt.executeUpdate();
                return null;
            }
        });
    }

}
