package org.mysqlmv.cd.logevent;

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
        long start = System.currentTimeMillis();
        for(int i=0; i<16010 ; i++) {
            Event ee = EventMiner.nextEvent();
//            System.out.println(i);
            if(!RecognizedEventType.isRecognized(ee.getHeader().getEventType())) {
                System.out.println(i);
                break;
            }
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testNextEvent() throws IOException {
    }
}
