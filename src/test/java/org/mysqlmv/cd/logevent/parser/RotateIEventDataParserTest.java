package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryIEventData;
import org.mysqlmv.cd.logevent.eventdef.data.RotateIEventData;
import org.mysqlmv.cd.logevent.parser.impl.RotateEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/18/2014 9:18 AM.
 */
public class RotateIEventDataParserTest {
    Event event;
    @BeforeClass
    public void switchFile() throws IOException {
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000004", 4L);
        for(int i=0; ; i++) {
            Event ee = EventMiner.getINSTANCE().next();
            if(i == 9) {
                event = ee;
                break;
            }
        }
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.ROTATE);
        Assert.assertEquals(event.isRawData(), true);
    }

    @Test
    public void parse() throws IOException {
        BinaryIEventData eData = (BinaryIEventData)event.getData();
        RotateEventDataParser parser = new RotateEventDataParser();
        RotateIEventData data = parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
        Assert.assertNotNull(data);
        Assert.assertEquals(data.getPosOfNextLog(), 4);
        Assert.assertEquals(data.getNameOfNextLog(), "PVGN50874064A-bin.000005");
    }
}
