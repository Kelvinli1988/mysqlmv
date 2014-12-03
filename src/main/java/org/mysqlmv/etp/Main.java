package org.mysqlmv.etp;

import org.mysqlmv.Switch;

/**
 * Created by Kelvin Li on 12/3/2014 2:55 PM.
 */
public class Main {
    public static void main(String[] args) {
        Switch appSwitch = Switch.getSwitch();
        appSwitch.startup();
        EtpEngine engine = new EtpEngine();
        engine.init();
        engine.start();
        while(!appSwitch.getStatus()) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}