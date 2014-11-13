package org.mysqlmv.cd.logevent.impl.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 10:22 AM.
 */
public class ByteArrayEventData implements EventData {

    private byte[] data;

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ByteArrayEventData");
        sb.append("{dataLength=").append(data.length);
        sb.append('}');
        return sb.toString();
    }
}
