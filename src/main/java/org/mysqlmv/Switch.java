package org.mysqlmv;

/**
 * Created by Kelvin Li on 11/24/2014 4:33 PM.
 */
public class Switch {

    private static Switch INSTANCE;

    private volatile boolean status;

    public void startup() {
        status = true;
    }

    public void shutdown() {
        status = false;
    }

    public boolean getStatus() {
        return status;
    }

    private Switch() {}

    public synchronized static Switch getSwitch() {
        if(INSTANCE == null) {
            INSTANCE = new Switch();
        }
        return INSTANCE;
    }
}
