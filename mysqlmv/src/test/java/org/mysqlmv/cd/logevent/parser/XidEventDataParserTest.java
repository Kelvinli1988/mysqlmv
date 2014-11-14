package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.BinLogFile;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.datainmanual.BinaryEventData;
import org.mysqlmv.cd.logevent.eventdef.datainmanual.XidEventData;
import org.mysqlmv.cd.logevent.parser.impl.XidEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 4:52 PM.
 */
public class XidEventDataParserTest {
    Event xidEvent;
    @BeforeClass
    public void switchFile() throws IOException {
        BinLogFile logFile = new BinLogFile("src/test/resources/PVGN50874064A-bin.000001");
        EventMiner.switchFile(logFile, 4L);
        for(int i=0; ; i++) {
            Event ee = EventMiner.nextEvent();
            if(i == 44) {
                xidEvent = ee;
                break;
            }
        }
    }

    @Test
    public void testParse() throws IOException {
        Assert.assertEquals(xidEvent.getHeader().getEventType(), LogEventType.XID);
        Assert.assertEquals(xidEvent.isRawData(), true);
        if(xidEvent.isRawData()) {
            // parse the data;
            BinaryEventData eData = xidEvent.getData();
            XidEventDataParser parser = new XidEventDataParser();
            XidEventData data =  parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
            Assert.assertEquals(data.getXid(), 60);
        }
    }
}
