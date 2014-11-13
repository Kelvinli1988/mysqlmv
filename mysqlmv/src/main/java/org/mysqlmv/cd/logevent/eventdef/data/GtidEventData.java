package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 10:34 AM.
 */
public class GtidEventData implements EventData {

    public static final byte COMMIT_FLAG = 1;

    private String gtid;
    private byte flags;

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public byte getFlags() {
        return flags;
    }

    public void setFlags(byte flags) {
        this.flags = flags;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("GtidEventData");
        sb.append("{flags=").append(flags).append(", gtid='").append(gtid).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
