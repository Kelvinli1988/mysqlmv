package org.mysqlmv.cd.workers;

/**
 * Created by Kelvin Li on 12/1/2014 1:30 PM.
 */
public class LogMinerRepository {
    private volatile String logRoot;

    private volatile String currentLogFile;

    private volatile long lastPointer = 0L;

    private volatile int currentLogRecordId;


}
