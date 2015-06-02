package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryIEventData;
import org.mysqlmv.cd.logevent.eventdef.data.XidIEventData;
import org.mysqlmv.cd.logevent.parser.impl.XidEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 4:52 PM.
 */
public class XidIEventDataParserTest {
    Event event;
    @BeforeClass
    public void switchFile() throws IOException {
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000001", 4L);
        for(int i=0; ; i++) {
            Event ee = EventMiner.getINSTANCE().next();
            if(i == 44) {
                event = ee;
                break;
            }
        }
    }

    @Test
    public void testParse() throws IOException {
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.XID);
        Assert.assertEquals(event.isRawData(), true);
        if(event.isRawData()) {
            // parse the data;
            BinaryIEventData eData = (BinaryIEventData)event.getData();
            XidEventDataParser parser = new XidEventDataParser();
            XidIEventData data =  parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
            Assert.assertEquals(data.getXid(), 60);
        }
    }
}
