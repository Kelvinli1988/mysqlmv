package org.mysqlmv.cd.logevent.impl.data;

import org.mysqlmv.cd.logevent.EventData;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Created by Kelvin Li on 11/13/2014 10:22 AM.
 */
public class DeleteRowsEventData implements EventData {

    private long tableId;
    private BitSet includedColumns;
    private List<Serializable[]> rows;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public BitSet getIncludedColumns() {
        return includedColumns;
    }

    public void setIncludedColumns(BitSet includedColumns) {
        this.includedColumns = includedColumns;
    }

    public List<Serializable[]> getRows() {
        return rows;
    }

    public void setRows(List<Serializable[]> rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DeleteRowsEventData");
        sb.append("{tableId=").append(tableId);
        sb.append(", includedColumns=").append(includedColumns);
        sb.append(", rows=[");
        for (Object[] row : rows) {
            sb.append("\n    ").append(Arrays.toString(row)).append(",");
        }
        if (!rows.isEmpty()) {
            sb.replace(sb.length() - 1, sb.length(), "\n");
        }
        sb.append("]}");
        return sb.toString();
    }
}
