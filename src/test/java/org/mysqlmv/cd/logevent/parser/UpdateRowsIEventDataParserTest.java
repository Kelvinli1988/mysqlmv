package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.*;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.cd.logevent.parser.impl.TableMapEventDataParser;
import org.mysqlmv.cd.logevent.parser.impl.UpdateRowsEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 5:32 PM.
 */
public class UpdateRowsIEventDataParserTest {
    Event event;


    @BeforeClass
    public void switchFile() throws IOException {
        TableMapEventDataParser tableMapParser = new TableMapEventDataParser();
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000008", 4L);
        while(EventMiner.getINSTANCE().hasNext()) {
            Event ee = EventMiner.getINSTANCE().next();
            if (ee.getHeader().getEventType().equals(LogEventType.TABLE_MAP)) {
                TableMapIEventData tableData = tableMapParser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(((BinaryIEventData) (ee.getData())).getData())));
                TableMapContext.addTableMap(tableData.getTableID(), tableData);
            }
            if (ee.getHeader().getEventType().equals(LogEventType.UPDATE_ROWS)) {
                event = ee;
                break;
            }
        }
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.UPDATE_ROWS);
        Assert.assertEquals(event.isRawData(), true);
    }

    @Test
    public void parse() throws IOException {
        BinaryIEventData eData = (BinaryIEventData)event.getData();
        UpdateRowsEventDataParser parser = new UpdateRowsEventDataParser();
        UpdateRowsIEventData data = parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
//        Assert.assertNull(data.getColumnUsageAfterUpdate());
        Assert.assertEquals(data.getColumnNum(), 5);
        Assert.assertEquals(data.getRows().size(), 2);
        RowsIEventData.Row row = data.getRows().get(0);
        Assert.assertEquals(row.getCells().get(0).getValue(), 3);
        Assert.assertEquals(row.getCells().get(1).getValue(), "11");
        row = data.getRows().get(1);
        Assert.assertEquals(row.getCells().get(0).getValue(), 3);
        Assert.assertEquals(row.getCells().get(1).getValue(), "11");
    }
}
