package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventHeader;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by Kelvin Li on 11/14/2014 10:45 AM.
 */

/**
 * As there will only one bin file which will be written by MYSQL, so this class will be a singleton class.
 * You can call the switchFile function to switch a bin file when bin file is changed.
 */
public class EventMiner implements Iterator<Event> {

    public static Logger logger = LoggerFactory.getLogger(EventMiner.class);
    /**
     * Need a header parser to recognize what event it is.
     */
    private EventHeaderParser headerParser;
    /**
     * Log file it is working on.
     */
    private InputStream logFileStream;

    private boolean streamClosed = true;
    /**
     * for log.
     */
    private String currentFileName;
    /**
     * From this point to start mining.
     */
    private long lastPointer;


    private EventMiner() {
        headerParser = new EventHeaderV4Parser();
        currentFileName = null;
    }

    private static EventMiner INSTANCE;

    public synchronized static EventMiner getINSTANCE() {
        if(INSTANCE == null) {
            INSTANCE = new EventMiner();
        }
        return INSTANCE;
    }

    /**
     * Switch log file.
     *
     * @param newFile
     * @param startPoint
     * @return
     * @throws IOException, this exception will be thrown when last file was closed failed or new file failed to open.
     */
    public boolean switchFile(String newFile, long startPoint) {
        logger.info("Switch binary log file now, from " + currentFileName + " to " + newFile);
        logger.info("Previous end point is " + currentFileName);
        if (logFileStream != null && currentFileName != null) {
            try {
                logFileStream.close();
                streamClosed = true;
            } catch (IOException e) {
                logger.warn("Fail to close binary log file : " + currentFileName, e);
            }
        }
        boolean isValidLogFile = false;
        try {
            logFileStream = new FileInputStream(newFile);
            isValidLogFile = validateLogFile(logFileStream);
            if(isValidLogFile) {
                streamClosed = false;
                logFileStream.skip(startPoint - 4);// skip unused bytes, usually it is 4;
            } else {
                logFileStream.close();
            }
        } catch (FileNotFoundException ex) {
            logger.error("Fail to find new binary log file : " + newFile, ex);
        } catch (IOException ex) {
            logger.error("Fail to access new binary log file: " + newFile, ex);
        }
        if(!isValidLogFile) {
            logger.error("Invalid binary log file, file path:" + newFile);
            throw new RuntimeException("Invalid binary log file, file path:" + newFile);
        }
        lastPointer = startPoint;
        logger.info("New file start point is " + startPoint);
        return true;
    }

    private boolean validateLogFile(InputStream input) throws IOException {
        byte[] MAGIC_HEADER = {(byte) 0xfe, (byte) 0x62, (byte) 0x69, (byte) 0x6e};
        byte[] magicPart = new byte[MAGIC_HEADER.length];
        input.read(magicPart);
        return Arrays.equals(MAGIC_HEADER, magicPart);
    }

    private void checkStream() {
        if (streamClosed) {
            try {
                logFileStream = new FileInputStream(currentFileName);
                logFileStream.skip(lastPointer);
            } catch (FileNotFoundException e) {
                logger.error("Binary log file is missing, log file path: " + currentFileName, e);
                throw new RuntimeException(e);
            } catch (IOException e) {
                logger.error("Fail to read log file, log file path: " + currentFileName, e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Event next() {
        checkStream();
        EventHeader header = null;
        byte[] eventData = null;
        try {
            header = headerParser.parse(new ByteArrayInputStream(logFileStream));
            eventData = new byte[header.getDataLength()];
            logFileStream.read(eventData);
        } catch (IOException e) {
            logger.error("Fail to read log file, log file path: " + currentFileName, e);
            throw new RuntimeException(e);
        }
        lastPointer = header.getNextPosition();
        logger.trace("Current log start point: " + lastPointer);
        return new Event(header, new BinaryEventData(eventData));
    }

    @Override
    public boolean hasNext() {
        checkStream();
        int available = 0;
        try {
            available = logFileStream.available();
        } catch (IOException e) {
            logger.error("Fail to access log file and try to close stream, log file path: " + currentFileName, e);
            try {
                logFileStream.close();
            } catch (IOException ex) {
                logger.error("Fail to close log file, log file path: " + currentFileName, ex);
            }
        }
        if (available <= 0) {
            try {
                logFileStream.close();
            } catch (IOException e) {
                logger.error("Fail to close log file, log file path: " + currentFileName, e);
            }
        }
        return available > 0;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
