package org.mysqlmv.cd.workers;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mysqlmv.Switch;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.parser.EventParsers;
import org.mysqlmv.cd.workers.impl.DefaultLogFileChangeProcessor;
import org.mysqlmv.cd.workers.impl.LogFileScanStatus;
import org.mysqlmv.common.config.reader.ConfigFactory;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.mysqlmv.common.io.db.DBUtil;
import org.mysqlmv.common.io.db.QueryCallBack;
import org.slf4j.Logger;

import javax.swing.*;
import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Kelvin Li on 11/18/2014 2:38 PM.
 */
public class LogFileChangeDetector implements Runnable {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(LogFileChangeDetector.class);

    private volatile long lastChangeTimeStamp;

    private volatile String logRoot;

    private LogFileChangeProcessor processor = new DefaultLogFileChangeProcessor();

    @Override
    public void run() {
        logRoot = ConfigFactory.getINSTANCE().getProperty("log-root-folder");
        Switch controller = Switch.getSwitch();
        while(controller.getStatus()) {
            scannLog();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void scannLog() {
        try {
            // When mysql is shutdown normally, a stop event will appears in the log file,
            // then should handle this situation.
            LogFileScanStatus status = LogFileScanStatus.SUCCESS;
            while(true) {
                File logFile = new File(findCurrentLogFile());
                status = processor.onFileChange(logFile, status.equals(LogFileScanStatus.STOP));
                if(status.equals(LogFileScanStatus.SUCCESS)) {
                    break;
                } else if(status.equals(LogFileScanStatus.STOP)) {
                    logger.warn("Mysql DB service is shutdown.");
                }
            }

        } catch (IOException e) {
            logger.error("Error happened when reading bin-log.");
            logger.error(ExceptionUtils.getStackTrace(e));
//            throw new RuntimeException(e);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    private String findCurrentLogFile() throws IOException {
        String currentLogFile = null;
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
            if(line.contains(".\\")) {
                currentLogFile = line.replace(".\\", "");
            }
        }
        return logRoot + "/" + currentLogFile;
    }
}
