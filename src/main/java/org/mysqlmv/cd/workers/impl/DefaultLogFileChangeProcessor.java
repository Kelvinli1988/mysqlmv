package org.mysqlmv.cd.workers.impl;

import org.mysqlmv.cd.dao.CdDao;
import org.mysqlmv.cd.logevent.*;
import org.mysqlmv.cd.logevent.parser.EventParsers;
import org.mysqlmv.cd.logevent.processors.DefaultEventProcessor;
import org.mysqlmv.cd.workers.LogFileChangeProcessor;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/1/2014 1:43 PM.
 */
public class DefaultLogFileChangeProcessor implements LogFileChangeProcessor {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultLogFileChangeProcessor.class);

    EventProcessor eventProcessor = new DefaultEventProcessor();

    @Override
    public LogFileScanStatus onFileChange(File logfile, boolean isNewFile) throws SQLException, IOException {
        LogFileStatus fileStatus  = getLogFile(logfile, isNewFile);
        long lastPointer = fileStatus.getLastPointer();
        int id = fileStatus.getId();
        String fileName = fileStatus.getFileName();
        // 3. read log.
        EventMiner miner = EventMiner.getINSTANCE().setCurrentFileName(fileName)
                .setLastPointer(lastPointer);
        int i=0;
        boolean hasNext = miner.hasNext();
        if(!hasNext && !logfile.getAbsolutePath().equals(fileName)) {
            return LogFileScanStatus.CONTINUE_NEXT;
        }
        while(miner.hasNext()) {
            Event event = miner.next();
            event = EventParsers.parse(event);
            boolean isStopEvent = event.getHeader().getEventType().equals(LogEventType.STOP);
            if(i == 100 || isStopEvent) {
                lastPointer = miner.getLastPointer();
                CdDao.updateLogFileStatus(id, lastPointer);
            }
            if(isStopEvent) {
                return LogFileScanStatus.STOP;
            }
            eventProcessor.processEvent(event);
            i++;
        }
        lastPointer = miner.getLastPointer();
        CdDao.updateLogFileStatus(id, lastPointer);
        miner.release();
        return LogFileScanStatus.SUCCESS;
    }

    private LogFileStatus getLogFile(File logfile, boolean isNewFile) {
        LogFileStatus status = CdDao.findLogFileStatus();
        if(status == null || isNewFile) {
            status = CdDao.insertNewFileStatus(logfile.getAbsolutePath(), 4);
        }
        return status;
    }


}
