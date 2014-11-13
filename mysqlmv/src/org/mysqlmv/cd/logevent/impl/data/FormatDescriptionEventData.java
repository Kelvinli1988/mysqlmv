package org.mysqlmv.cd.logevent.impl.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 10:34 AM.
 */
public class FormatDescriptionEventData implements EventData {

    private int binlogVersion;
    private String serverVersion;
    private int headerLength;

    public int getBinlogVersion() {
        return binlogVersion;
    }

    public void setBinlogVersion(int binlogVersion) {
        this.binlogVersion = binlogVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    public int getHeaderLength() {
        return headerLength;
    }

    public void setHeaderLength(int headerLength) {
        this.headerLength = headerLength;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("FormatDescriptionEventData");
        sb.append("{binlogVersion=").append(binlogVersion);
        sb.append(", serverVersion='").append(serverVersion).append('\'');
        sb.append(", headerLength=").append(headerLength);
        sb.append('}');
        return sb.toString();
    }
}
