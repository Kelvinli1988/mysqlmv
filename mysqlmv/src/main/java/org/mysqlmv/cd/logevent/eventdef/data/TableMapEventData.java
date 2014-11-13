package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

import java.util.BitSet;

/**
 * Created by Kelvin Li on 11/13/2014 10:34 AM.
 */
public class TableMapEventData implements EventData {

    private long tableId;
    private String database;
    private String table;
    private byte[] columnTypes;
    private int[] columnMetadata;
    private BitSet columnNullability;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public byte[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(byte[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public int[] getColumnMetadata() {
        return columnMetadata;
    }

    public void setColumnMetadata(int[] columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public BitSet getColumnNullability() {
        return columnNullability;
    }

    public void setColumnNullability(BitSet columnNullability) {
        this.columnNullability = columnNullability;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TableMapEventData");
        sb.append("{tableId=").append(tableId);
        sb.append(", database='").append(database).append('\'');
        sb.append(", table='").append(table).append('\'');
        sb.append(", columnTypes=").append(columnTypes == null ? "null" : "");
        for (int i = 0; columnTypes != null && i < columnTypes.length; ++i) {
            sb.append(i == 0 ? "" : ", ").append(columnTypes[i]);
        }
        sb.append(", columnMetadata=").append(columnMetadata == null ? "null" : "");
        for (int i = 0; columnMetadata != null && i < columnMetadata.length; ++i) {
            sb.append(i == 0 ? "" : ", ").append(columnMetadata[i]);
        }
        sb.append(", columnNullability=").append(columnNullability);
        sb.append('}');
        return sb.toString();
    }
}
