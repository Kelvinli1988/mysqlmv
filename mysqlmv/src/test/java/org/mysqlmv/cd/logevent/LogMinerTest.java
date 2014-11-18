package org.mysqlmv.cd.logevent;

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
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000006", 4L);
        long start = System.currentTimeMillis();
        for(int i=0; i<16010 ; i++) {
            if(EventMiner.getINSTANCE().hasNext()) {
                Event ee = EventMiner.getINSTANCE().next();
                if(!RecognizedEventType.isRecognized(ee.getHeader().getEventType())) {
                    System.out.println(i);
                    break;
                }
            }

        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testNextEvent() throws IOException {
    }
}
