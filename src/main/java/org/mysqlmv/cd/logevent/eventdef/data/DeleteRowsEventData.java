package org.mysqlmv.cd.logevent.eventdef.data;

import java.util.BitSet;

/**
 * Created by Kelvin Li on 11/17/2014 11:07 AM.
 */
public class DeleteRowsEventData extends RowsEventData {
    public BitSet getColumnUsageAfterUpdate() {
        return null;
    }

    public void setColumnUsageAfterUpdate(BitSet columnUsageAfterUpdate) {
        throw new UnsupportedOperationException();
    }

}
