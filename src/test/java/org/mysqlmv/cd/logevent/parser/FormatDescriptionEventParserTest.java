package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryEventData;
import org.mysqlmv.cd.logevent.eventdef.data.FormatDescriptionEventData;
import org.mysqlmv.cd.logevent.parser.impl.FormatDescriptionEventParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 3:13 PM.
 */
public class FormatDescriptionEventParserTest {
    private Event event;
    @BeforeClass
    public void prepareEvent() throws IOException {
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000001", 4L);
        event = EventMiner.getINSTANCE().next();
    }

    @Test
    public void testEventDataparser() throws IOException {
        // Guarantee this event is a format description event.
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.FORMAT_DESCRIPTION);
        Assert.assertEquals(event.isRawData(), true);
        if(event.isRawData()) {
            // parse the data;
            BinaryEventData eData = event.getData();
            FormatDescriptionEventParser parser = new FormatDescriptionEventParser();
            FormatDescriptionEventData data =  parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
            Assert.assertEquals(data.getLogVersion(), 4);
            Assert.assertEquals(data.getHeaderLength(), 19);
            Assert.assertEquals(data.getServerVersion(), "5.5.27-log");
        }
    }

}
