package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.*;
import org.mysqlmv.cd.logevent.eventdef.data.RowsIEventData;
import org.mysqlmv.cd.logevent.eventdef.data.TableMapIEventData;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.cd.logevent.parser.impl.TableMapEventDataParser;
import org.mysqlmv.cd.logevent.parser.impl.WriteRowsEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 1:20 PM.
 */
public class WriteRowsIEventDataParserTest {
    Event event;


    @BeforeClass
    public void switchFile() throws IOException {
        TableMapEventDataParser tableMapParser = new TableMapEventDataParser();
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000003", 4L);
        for (int i = 0; ; i++) {
            Event ee = EventMiner.getINSTANCE().next();
            if (ee.getHeader().getEventType().equals(LogEventType.TABLE_MAP)) {
                TableMapIEventData tableData = tableMapParser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(((BinaryIEventData) (ee.getData())).getData())));
                TableMapContext.addTableMap(tableData.getTableID(), tableData);
            }
            if (i == 3) {
                event = ee;
                break;
            }
        }
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.WRITE_ROWS);
        Assert.assertEquals(event.isRawData(), true);
    }

    @Test
    public void parse() throws IOException {
        BinaryIEventData eData = (BinaryIEventData)event.getData();
        WriteRowsEventDataParser parser = new WriteRowsEventDataParser();
        WriteRowsIEventData data = parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
        Assert.assertNull(data.getColumnUsageAfterUpdate());
        Assert.assertEquals(data.getColumnNum(), 2);
        Assert.assertEquals(data.getRows().size(), 1);
        RowsIEventData.Row row = data.getRows().get(0);
        Assert.assertEquals(row.getCells().get(0).getValue(), 5);
        Assert.assertEquals(row.getCells().get(1).getValue(), "mm");
    }
}
