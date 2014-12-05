package org.mysqlmv.etp.context;

import java.io.Serializable;

/**
 * Created by Kelvin Li on 12/3/2014 1:31 PM.
 */
public class ToiEntry implements Serializable {
    private final String schema;

    private final String table;

    public ToiEntry(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public int hashCode() {
        return (schema + table).hashCode();
    }

    public boolean equals(Object obj) {
        if(obj == null) return false;
        return this.hashCode() == obj.hashCode();
    }

    public String toString() {
        return schema + ", " + table;
    }
}
