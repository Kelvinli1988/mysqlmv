package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/14/2014 11:16 AM.
 */
public class BinaryEventData implements EventData {
    private byte[] data;

    public BinaryEventData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
