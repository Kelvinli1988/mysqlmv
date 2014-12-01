package org.mysqlmv.cd.logevent;

import org.mysqlmv.Switch;
import org.mysqlmv.cd.workers.LogFileChangeDetector;
import org.testng.annotations.Test;

/**
 * Created by Kelvin Li on 12/1/2014 3:38 PM.
 */
public class FileChangeDetectorTest {
    @Test
    public void test() {
        Switch.getSwitch().startup();
        LogFileChangeDetector detector = new LogFileChangeDetector();
        detector.run();
    }
}
