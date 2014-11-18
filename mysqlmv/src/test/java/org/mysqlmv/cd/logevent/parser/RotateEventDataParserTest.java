package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.BinLogFile;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryEventData;
import org.mysqlmv.cd.logevent.eventdef.data.RotateEventData;
import org.mysqlmv.cd.logevent.eventdef.data.UpdateRowsEventData;
import org.mysqlmv.cd.logevent.parser.impl.RotateEventDataParser;
import org.mysqlmv.cd.logevent.parser.impl.UpdateRowsEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/18/2014 9:18 AM.
 */
public class RotateEventDataParserTest {
    Event event;
    @BeforeClass
    public void switchFile() throws IOException {
        BinLogFile logFile = new BinLogFile("src/test/resources/PVGN50874064A-bin.000004");
        EventMiner.switchFile(logFile, 4L);
        for(int i=0; ; i++) {
            Event ee = EventMiner.nextEvent();
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
        BinaryEventData eData = event.getData();
        RotateEventDataParser parser = new RotateEventDataParser();
        RotateEventData data = parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
        Assert.assertNotNull(data);
        Assert.assertEquals(data.getPosOfNextLog(), 4);
        Assert.assertEquals(data.getNameOfNextLog(), "PVGN50874064A-bin.000005");
    }
}
