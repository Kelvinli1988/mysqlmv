package org.mysqlmv.cd.logevent.impl.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 10:34 AM.
 */
public class XidEventData implements EventData {

    private long xid;

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("XidEventData");
        sb.append("{xid=").append(xid);
        sb.append('}');
        return sb.toString();
    }
}
