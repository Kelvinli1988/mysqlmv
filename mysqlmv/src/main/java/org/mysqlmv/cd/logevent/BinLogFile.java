package org.mysqlmv.cd.logevent;

import com.sun.corba.se.spi.orbutil.fsm.Input;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Kelvin Li on 11/14/2014 9:55 AM.
 */
public class BinLogFile {
    private String binlogFile;

    public static byte[] MAGIC_HEADER = {(byte) 0xfe, (byte) 0x62, (byte) 0x69, (byte) 0x6e};

    private List<Event> eventList;

    public BinLogFile(String filePath) {
        this.binlogFile = filePath;
        eventList = new LinkedList<Event>();
    }

    public boolean validateLogFile() throws IOException {
        File logFile = new File(binlogFile);
        InputStream lfs = null;
        try {
            lfs = new FileInputStream(logFile);
        } catch (FileNotFoundException e) {
            // TODO add logger
            return false;
        }
        byte[] magicPart = new byte[MAGIC_HEADER.length];
        lfs.read(magicPart);
        return Arrays.equals(MAGIC_HEADER, magicPart);
    }

    public InputStream getInputStream() throws FileNotFoundException {
        return new FileInputStream(binlogFile);
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }
}
