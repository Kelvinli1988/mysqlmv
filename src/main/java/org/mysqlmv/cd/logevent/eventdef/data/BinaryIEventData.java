package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.IEventData;

/**
 * Created by Kelvin Li on 11/14/2014 11:16 AM.
 */
public class BinaryIEventData implements IEventData {
    private byte[] data;

    public BinaryIEventData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
