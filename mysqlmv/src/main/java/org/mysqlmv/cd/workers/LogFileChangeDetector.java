package org.mysqlmv.cd.workers;

import org.mysqlmv.Switch;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.parser.EventParsers;
import org.mysqlmv.common.config.reader.ConfigFactory;
import org.mysqlmv.common.io.db.ConnectionUtil;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Kelvin Li on 11/18/2014 2:38 PM.
 */
public class LogFileChangeDetector implements Runnable {

    private volatile long lastChangeTimeStamp;

    private volatile String logRoot;

    private volatile String currentLogFile;

    private volatile long lastPointer = 0L;

    @Override
    public void run() {
        logRoot = ConfigFactory.getINSTANCE().getProperty("ConfigFactory");
        Switch controller = Switch.getSwitch();
        while(controller.getStatus()) {
            try {
                scannLog();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void scannLog() throws SQLException {
        // 1. get bin_log_logger in db
        String findLoggerSQL = "select * from bin_log_file_logger order by id desc limit 1";
        PreparedStatement stmt = ConnectionUtil.getConnection().prepareStatement(findLoggerSQL);
        stmt.execute();
        ResultSet loggerRS = stmt.getResultSet();
        boolean isFirstTime = !loggerRS.next();
        if(isFirstTime) {
            // 2. get log file strategy.
            String strategy = ConfigFactory.getINSTANCE().getProperty("log.miner.strategy");
            try{
                if("ALL".equals(strategy)) {
                    currentLogFile = findCurrentLogFile(true);
                } else {
                    // if the strategy is not set or set as LATEST read the latest log file.
                    currentLogFile = findCurrentLogFile(false);
                }
            } catch (IOException e) {
                // TODO add logger and exception handling.
            }
        } else {

        }
        // 3. read log.
        EventMiner miner = EventMiner.getINSTANCE().setCurrentFileName(currentLogFile).setLastPointer(lastPointer);
        while(miner.hasNext()) {
            Event event = miner.next();
            try {
                event = EventParsers.parse(event);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private String findCurrentLogFile(boolean isFirst) throws IOException {
        File logDir = new File(logRoot);
        File indexFile = null;
        for(File ff : logDir.listFiles()) {
            if(ff.getName().endsWith(".index")) {
                indexFile = ff;
            }
        }
        if(indexFile == null) {
            return null;
        }
        BufferedReader indexReader = new BufferedReader(new FileReader(indexFile));
        if(indexReader == null) {
            return null;
        }
        String line = null;
        while((line = indexReader.readLine()) != null) {
            if(line.contains("\\.\\")) {
                currentLogFile = line.replace("\\.\\", "");
                if(isFirst) {
                    break;
                }
            }
        }
        return logRoot + File.pathSeparator + currentLogFile;
    }
}
