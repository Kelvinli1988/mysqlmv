package org.mysqlmv.etp;

import org.mysqlmv.cd.workers.LogFileChangeDetector;
import org.testng.annotations.Test;

/**
 * Created by Kelvin Li on 12/3/2014 3:40 PM.
 */
public class LogDetectorTest {

    @Test
    public void test() {
        new Thread(new LogFileChangeDetector()).start();
    }
}
