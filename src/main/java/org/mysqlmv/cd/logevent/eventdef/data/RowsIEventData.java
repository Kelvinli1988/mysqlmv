package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.IEventData;

import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Kelvin Li on 11/13/2014 4:54 PM.
 */

/**
 * Used for row-based binary logging beginning with MySQL 5.1.18.
 */


/**
 * Write_rows_log_event/WRITE_ROWS_EVENT
 * <p/>
 * Used for row-based binary logging beginning with MySQL 5.1.18.
 * <p/>
 * [TODO: following needs verification; it's guesswork]
 * <p/>
 * Fixed data part:
 * <p/>
 * 6 bytes. The table ID.
 * <p/>
 * 2 bytes. Reserved for future use.
 * <p/>
 * Variable data part:
 * <p/>
 * Packed integer. The number of columns in the table.
 * <p/>
 * Variable-sized. Bit-field indicating whether each column is used, one bit per column. For this field, the amount of storage required for N columns is INT((N+7)/8) bytes.
 * <p/>
 * Variable-sized (for UPDATE_ROWS_LOG_EVENT only). Bit-field indicating whether each column is used in the UPDATE_ROWS_LOG_EVENT after-image; one bit per column. For this field, the amount of storage required for N columns is INT((N+7)/8) bytes.
 * <p/>
 * Variable-sized. A sequence of zero or more rows. The end is determined by the size of the event. Each row has the following format:
 * <p/>
 * Variable-sized. Bit-field indicating whether each field in the row is NULL. Only columns that are "used" according to the second field in the variable data part are listed here. If the second field in the variable data part has N one-bits, the amount of storage required for this field is INT((N+7)/8) bytes.
 * <p/>
 * Variable-sized. The row-image, containing values of all table fields. This only lists table fields that are used (according to the second field of the variable data part) and non-NULL (according to the previous field). In other words, the number of values listed here is equal to the number of zero bits in the previous field (not counting padding bits in the last byte).
 * <p/>
 * The format of each value is described in the log_event_print_value() function in log_event.cc.
 * <p/>
 * (for UPDATE_ROWS_EVENT only) the previous two fields are repeated, representing a second table row.
 * <p/>
 * For each row, the following is done:
 * <p/>
 * For WRITE_ROWS_LOG_EVENT, the row described by the row-image is inserted.
 * <p/>
 * For DELETE_ROWS_LOG_EVENT, a row matching the given row-image is deleted.
 * <p/>
 * For UPDATE_ROWS_LOG_EVENT, a row matching the first row-image is removed, and the row described by the second row-image is inserted.
 */
public class RowsIEventData implements IEventData {

    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    /**
     * 6 bytes. The table ID.
     */
    private long tableId;
    /**
     * 2 bytes. Reserved for future use.
     */
    private int toUse;
//    /**
//     * 2 bytes
//     */
//    private int extraInfoLength;
//    /**
//     *
//     */
//    private byte[] extraInfo;
    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * Packed integer. The number of columns in the table.
     */
    private int columnNum;

    /**
     * Variable-sized. Bit-field indicating whether each column is used, one bit per column.
     * For this field, the amount of storage required for N columns is INT((N+7)/8) bytes.
     */
    private BitSet columnUsageBeforeUpdate;
    /**
     * Variable-sized (for UPDATE_ROWS_LOG_EVENT only). Bit-field indicating whether each
     * column is used in the UPDATE_ROWS_LOG_EVENT after-image; one bit per column.
     * For this field, the amount of storage required for N columns is INT((N+7)/8) bytes.
     */
    private BitSet columnUsageAfterUpdate;

    private List<Row> rows;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public int getToUse() {
        return toUse;
    }

    public void setToUse(int toUse) {
        this.toUse = toUse;
    }

    public int getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(int columnNum) {
        this.columnNum = columnNum;
    }

    public BitSet getColumnUsageBeforeUpdate() {
        return columnUsageBeforeUpdate;
    }

    public void setColumnUsageBeforeUpdate(BitSet columnUsageBeforeUpdate) {
        this.columnUsageBeforeUpdate = columnUsageBeforeUpdate;
    }

    public BitSet getColumnUsageAfterUpdate() {
        return columnUsageAfterUpdate;
    }

    public void setColumnUsageAfterUpdate(BitSet columnUsageAfterUpdate) {
        this.columnUsageAfterUpdate = columnUsageAfterUpdate;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public RowsIEventData() {
        this.rows = new LinkedList<Row>();
    }

    public static class Row implements Serializable {
        private List<Cell> cells;

        public List<Cell> getCells() {
            return cells;
        }

        public void setCells(List<Cell> cells) {
            this.cells = cells;
        }

        public Row() {
            cells = new LinkedList<Cell>();
        }
    }

    public static class Cell implements Serializable {
        private ColumnType columnType;

        private Serializable value;

        public Serializable getValue() {
            return value;
        }

        public void setValue(Serializable value) {
            this.value = value;
        }

        public ColumnType getColumnType() {
            return columnType;
        }

        public void setColumnType(ColumnType columnType) {
            this.columnType = columnType;
        }
    }

    public RowOperation getOperation() {
        throw new UnsupportedOperationException();
    }
}
