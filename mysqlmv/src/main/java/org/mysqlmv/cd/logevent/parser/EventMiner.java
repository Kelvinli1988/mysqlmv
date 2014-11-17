package org.mysqlmv.cd.logevent.parser;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mysqlmv.cd.logevent.BinLogFile;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventHeader;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by Kelvin Li on 11/14/2014 10:45 AM.
 */

/**
 * As there will only one bin file which will be written by MYSQL, so this class will be a singleton class.
 * You can call the switchFile function to switch a bin file when bin file is changed.
 */
public class EventMiner {

    public static Logger logger = Logger.getLogger(EventMiner.class.getName());
    /**
     * Need a header parser to recognize what event it is.
     */
    private static EventHeaderParser headerParser;
    /**
     * Log file it is working on.
     */
    private static InputStream logFileStream;

    private static boolean streamClosed = false;
    /**
     * for log.
     */
    private static String currentFileName;
    /**
     * From this point to start mining.
     */
    private static long lastPointer;

    static {
        headerParser = new EventHeaderV4Parser();
        currentFileName = null;
    }

    /**
     * Switch log file.
     *
     * @param newFile
     * @param startPoint
     * @return
     * @throws IOException, this exception will be thrown when last file was closed failed or new file failed to open.
     */
    public static boolean switchFile(BinLogFile newFile, long startPoint) throws IOException {
        logger.info("Switch binary log file now, from " + currentFileName + " to " + newFile.getBinlogFile());
        logger.info("Previous end point is " + currentFileName);
        if (logFileStream != null && currentFileName != null) {
            try {
                logFileStream.close();
                streamClosed = true;
            } catch (IOException e) {
                logger.warning("Fail to close binary log file : " + currentFileName);
                logger.warning(ExceptionUtils.getStackTrace(e));
                throw e;
            }
        }

        try {
            logFileStream = newFile.getInputStream();
            streamClosed = false;
            logFileStream.skip(4L);// skip the magic header;
        } catch (FileNotFoundException ex) {
            logger.warning("Fail to fine new binary log file : " + newFile.getBinlogFile());
            logger.warning(ExceptionUtils.getStackTrace(ex));
            throw ex;
        }
        lastPointer = startPoint;
        logger.info("New file start point is " + startPoint);
        return true;
    }

    public static Event nextEvent() throws IOException {
        if (streamClosed) {
            logFileStream = new FileInputStream(currentFileName);
            logFileStream.skip(lastPointer);
        }
        EventHeader header = null;
        try {
            header = headerParser.parse(new ByteArrayInputStream(EventMiner.logFileStream));
        } catch (IOException e) {
            logger.warning("Parse log file header error.");
            logger.warning(ExceptionUtils.getStackTrace(e));
            throw e;
        }
        header.getDataLength();
        byte[] eventData = new byte[header.getDataLength()];
        int byteRed = logFileStream.read(eventData);
        if (byteRed < eventData.length) {
            logger.info("Current event was not fully recorded.");
            logger.info("Current log start point: " + lastPointer);
            try {
                logFileStream.close();
                streamClosed = true;
                logger.info("Binary log stream closed.");
            } catch (IOException e) {
                logger.warning("Fail to close current file stream, file name:" + currentFileName);
                throw e;
            }
            return null;
        }
        lastPointer = header.getNextPosition();
        return new Event(header, new BinaryEventData(eventData));
    }
}
