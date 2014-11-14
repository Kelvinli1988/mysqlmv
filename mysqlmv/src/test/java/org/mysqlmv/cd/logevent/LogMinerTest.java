package org.mysqlmv.cd.logevent;

import junit.framework.Assert;
import org.mysqlmv.cd.logevent.eventdef.header.EventVersion;
import org.mysqlmv.cd.logevent.parser.EventMiner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 2:13 PM.
 */
public class LogMinerTest {

    @BeforeClass
    public void switchFile() throws IOException {
        BinLogFile logFile = new BinLogFile("src/test/resources/PVGN50874064A-bin.000001");
        EventMiner.switchFile(logFile, 4L);
    }

    @Test
    public void testNextEvent() throws IOException {
        Event ee = EventMiner.nextEvent();
        EventHeader header = ee.getHeader();
        Assert.assertEquals(header.getVersion(), EventVersion.V_4);
    }
}
