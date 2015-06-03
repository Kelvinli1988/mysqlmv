package org.mysqlmv.mvm.mv;

/**
 * This table recorded the relationship between marker and materialized view.
 * Created by Kelvin Li on 6/3/2015.
 */
public class TableMarkerMvMapping {

    private long tableMarkerId;

    private long mvId;

    public long getTableMarkerId() {
        return tableMarkerId;
    }

    public long getMvId() {
        return mvId;
    }

    public TableMarkerMvMapping(long tableMarkerId, long mvId) {
        this.tableMarkerId = tableMarkerId;
        this.mvId = mvId;
    }

    public boolean equals(Object toCompare) {
        if(toCompare == null) return false;
        if(!(toCompare instanceof TableMarkerMvMapping)) return false;
        TableMarkerMvMapping mapping = (TableMarkerMvMapping) toCompare;
        if(mapping.getMvId() == mvId && mapping.getTableMarkerId() == tableMarkerId)  return true;
        return false;
    }
}
