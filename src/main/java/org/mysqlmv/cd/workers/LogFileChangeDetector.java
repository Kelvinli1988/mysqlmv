package org.mysqlmv.cd.workers;

import org.mysqlmv.Switch;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.parser.EventParsers;
import org.mysqlmv.cd.workers.impl.DefaultLogFileChangeProcessor;
import org.mysqlmv.common.config.reader.ConfigFactory;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.mysqlmv.common.io.db.DBUtil;
import org.mysqlmv.common.io.db.QueryCallBack;

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

    private volatile long lastChangeTimeStamp;

    private volatile String logRoot;

    private LogFileChangeProcessor processor = new DefaultLogFileChangeProcessor();

    @Override
    public void run() {
        logRoot = ConfigFactory.getINSTANCE().getProperty("log-root-folder");
        Switch controller = Switch.getSwitch();
        while(controller.getStatus()) {
            scannLog();
        }
    }

    private void scannLog() {
        try {
            File logFile = new File(findCurrentLogFile());
            long lastmodify = logFile.lastModified();
            if(lastmodify > lastChangeTimeStamp) {
                processor.onFileChange(logFile);
                lastChangeTimeStamp = lastmodify;
            }
        } catch (IOException e) {

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
