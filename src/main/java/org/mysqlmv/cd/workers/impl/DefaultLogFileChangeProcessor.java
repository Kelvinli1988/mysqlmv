package org.mysqlmv.cd.workers.impl;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.parser.EventParsers;
import org.mysqlmv.cd.workers.EventDispatcher;
import org.mysqlmv.cd.workers.LogFileChangeProcessor;
import org.mysqlmv.common.config.reader.ConfigFactory;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.mysqlmv.common.io.db.DBUtil;
import org.mysqlmv.common.io.db.QueryCallBack;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/1/2014 1:43 PM.
 */
public class DefaultLogFileChangeProcessor implements LogFileChangeProcessor {

//    private volatile int currentLogRecordId;

    @Override
    public void onFileChange(File logfile) throws SQLException {
        String findLoggerSQL = "select * from bin_log_file_logger order by id desc limit 1";
        PreparedStatement stmt = ConnectionUtil.getConnection().prepareStatement(findLoggerSQL);
        stmt.execute();
        ResultSet loggerRS = stmt.getResultSet();
        boolean isFirstTime = !loggerRS.next();
        String currentLogFile = null;
        long lastPointer = 0L;
        int currentLogRecordId = 0;
        if(isFirstTime) {
            // 2. get log file strategy.
            String strategy = ConfigFactory.getINSTANCE().getProperty("log.miner.strategy");
            currentLogFile = logfile.getAbsolutePath();
            lastPointer = 4;
            currentLogRecordId = initBinLogRecord(currentLogFile, lastPointer);
        } else {
            lastPointer = loggerRS.getLong("last_pointer");
            currentLogFile = loggerRS.getString("log_file_name");
            currentLogRecordId = loggerRS.getInt("logger_id");
        }
        // 3. read log.
        EventMiner miner = EventMiner.getINSTANCE().setCurrentFileName(currentLogFile).setLastPointer(lastPointer);
        while(miner.hasNext()) {
            Event event = miner.next();
            try {
                event = EventParsers.parse(event);
                EventDispatcher.dispatchEvent(event);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        miner.release();
        lastPointer = miner.getLastPointer();
        updateBinLogRecord(currentLogRecordId, lastPointer);
    }

    private int initBinLogRecord(final String currentLogFile, final long lastPointer) throws SQLException {
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "insert into bin_log_file_logger(log_file_name, start_read_datetime, rotate_datatime, last_pointer) values(?, now(), null, ?)";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setString(1, currentLogFile);
                pstmt.setLong(2, lastPointer);
                pstmt.execute();
                return null;
            }
        });
        return DBUtil.getLastInsertedID();
    }

    private void updateBinLogRecord(final int currentLogRecordId, final long lastPointer) throws SQLException {
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
}
