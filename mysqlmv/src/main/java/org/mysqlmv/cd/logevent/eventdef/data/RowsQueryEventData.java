package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 10:34 AM.
 */
public class RowsQueryEventData implements EventData {

    private String query;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RowsQueryEventData");
        sb.append("{query='").append(query).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
