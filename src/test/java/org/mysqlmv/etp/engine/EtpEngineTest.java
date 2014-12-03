package org.mysqlmv.etp.engine;

import org.mysqlmv.etp.EtpEngine;
import org.testng.annotations.Test;

/**
 * Created by Kelvin Li on 12/3/2014 3:06 PM.
 */
public class EtpEngineTest {
    @Test
    public void testEngine() {
        EtpEngine engine = new EtpEngine();
        engine.init();
    }
}
