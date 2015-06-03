package org.mysqlmv.mvm.mv;

/**
 * Created by I312762 on 6/1/2015.
 */
public class BaseDeltaTable {

    private String schema;

    private String table;

    private long mvID;

    private String alias;

    private long markerId;

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public long getMvID() {
        return mvID;
    }

    public String getAlias() {
        return alias;
    }

    public long getMarkerId() {
        return markerId;
    }

    public void setMarkerId(long markerId) {
        this.markerId = markerId;
    }

    public BaseDeltaTable(long mvID, String schema, String table, String alias) {
        this.schema = schema;
        this.table = table;
        this.mvID = mvID;
        this.alias = alias;
    }
}
