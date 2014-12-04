package org.mysqlmv.cd.workers.impl;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.EventProcessor;
import org.mysqlmv.cd.logevent.parser.EventParsers;
import org.mysqlmv.cd.logevent.processors.DefaultEventProcessor;
import org.mysqlmv.cd.workers.LogFileChangeProcessor;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.mysqlmv.common.io.db.DBUtil;
import org.mysqlmv.common.io.db.QueryCallBack;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/1/2014 1:43 PM.
 */
public class DefaultLogFileChangeProcessor implements LogFileChangeProcessor {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultLogFileChangeProcessor.class);

    EventProcessor eventProcessor = new DefaultEventProcessor();

    @Override
    public void onFileChange(File logfile) throws SQLException, IOException {
        String findLoggerSQL = "select * from bin_log_file_logger order by logger_id desc limit 1";
        PreparedStatement stmt = ConnectionUtil.getConnection().prepareStatement(findLoggerSQL);
        stmt.execute();
        ResultSet loggerRS = stmt.getResultSet();
        boolean isFirstTime = !loggerRS.next();
        String currentLogFile = null;
        long lastPointer = 0L;
        int currentLogRecordId = 0;
        if(isFirstTime) {
            currentLogFile = logfile.getAbsolutePath();
            lastPointer = 4;
            currentLogRecordId = initBinLogRecord(currentLogFile, lastPointer);
        } else {
            lastPointer = loggerRS.getLong("last_pointer");
            currentLogFile = loggerRS.getString("log_file_name");
            currentLogRecordId = loggerRS.getInt("logger_id");
        }
        loggerRS.close();
        stmt.close();
        // 3. read log.
        EventMiner miner = EventMiner.getINSTANCE().setCurrentFileName(currentLogFile).setLastPointer(lastPointer);
        int i=0;
        while(miner.hasNext()) {
            if(i == 1) {
                lastPointer = miner.getLastPointer();
                updateBinLogRecord(currentLogRecordId, lastPointer);
            }
            Event event = miner.next();
            try{
                event = EventParsers.parse(event);
            } catch(Exception ex) {
                ex.printStackTrace();
            }

            eventProcessor.processEvent(event);
            i++;
        }
        miner.release();
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
