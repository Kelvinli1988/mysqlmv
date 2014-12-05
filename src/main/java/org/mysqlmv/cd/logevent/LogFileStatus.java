package org.mysqlmv.cd.logevent;

/**
 * Created by Kelvin Li on 12/5/2014 2:34 PM.
 */
public class LogFileStatus {
    private String fileName;

    private long lastPointer;

    private int id;

    public LogFileStatus(String fileName, long lastPointer, int id) {
        this.fileName = fileName;
        this.lastPointer = lastPointer;
        this.id = id;
    }

    public LogFileStatus() {
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getLastPointer() {
        return lastPointer;
    }

    public void setLastPointer(long lastPointer) {
        this.lastPointer = lastPointer;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
