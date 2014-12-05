package org.mysqlmv.cd.workers;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mysqlmv.Switch;
import org.mysqlmv.cd.dao.CdDao;
import org.mysqlmv.cd.workers.impl.DefaultLogFileChangeProcessor;
import org.mysqlmv.cd.workers.impl.LogFileScanStatus;
import org.mysqlmv.common.config.reader.ConfigFactory;
import org.slf4j.Logger;

import java.io.*;
import java.sql.SQLException;

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
            File logFile = new File(findCurrentLogFile());
            boolean isNewFile = false;
            while(true) {
                status = processor.onFileChange(logFile, isNewFile);
                if(status.equals(LogFileScanStatus.SUCCESS)) {
                    break;
                } else if(status.equals(LogFileScanStatus.STOP)) {
                    isNewFile = true;
                    logger.warn("Mysql DB service is shutdown.");
                } else if(status.equals(LogFileScanStatus.CONTINUE_NEXT)) {
                    logFile = findNextFile();
                    isNewFile = true;
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

    private File findNextFile() throws SQLException, IOException {
        String logFullName = CdDao.findCurrentLogFileName();
        String curFileName = new File(logFullName).getName();
        File indexFile = findIndexLogfile();
        BufferedReader indexReader = new BufferedReader(new FileReader(indexFile));
        if(indexReader == null) {
            return null;
        }
        String line = null;
        while((line = indexReader.readLine()) != null) {
            if(line.contains(".\\")) {
                line = line.replace(".\\", "");
                if(curFileName.equals(line)) {
                    line = indexReader.readLine().replace(".\\", "");
                    break;
                }
            }
        }
        if("".equals(line)) {
            return null;
        }
        return new File(logRoot + "/" + line);
    }

    public File findIndexLogfile() {
        File logDir = new File(logRoot);
        File indexFile = null;
        for(File ff : logDir.listFiles()) {
            if(ff.getName().endsWith(".index")) {
                indexFile = ff;
            }
        }
        return indexFile;
    }

    private String findCurrentLogFile() throws IOException {
        String currentLogFile = null;
        File indexFile = findIndexLogfile();
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
