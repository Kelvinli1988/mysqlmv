package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

/**
 * Created by Kelvin Li on 11/13/2014 4:54 PM.
 */

/**
 * Used for row-based binary logging beginning with MySQL 5.1.18.
 */


/**
 *
 Write_rows_log_event/WRITE_ROWS_EVENT

 Used for row-based binary logging beginning with MySQL 5.1.18.

 [TODO: following needs verification; it's guesswork]

 Fixed data part:

 6 bytes. The table ID.

 2 bytes. Reserved for future use.

 Variable data part:

 Packed integer. The number of columns in the table.

 Variable-sized. Bit-field indicating whether each column is used, one bit per column. For this field, the amount of storage required for N columns is INT((N+7)/8) bytes.

 Variable-sized (for UPDATE_ROWS_LOG_EVENT only). Bit-field indicating whether each column is used in the UPDATE_ROWS_LOG_EVENT after-image; one bit per column. For this field, the amount of storage required for N columns is INT((N+7)/8) bytes.

 Variable-sized. A sequence of zero or more rows. The end is determined by the size of the event. Each row has the following format:

 Variable-sized. Bit-field indicating whether each field in the row is NULL. Only columns that are "used" according to the second field in the variable data part are listed here. If the second field in the variable data part has N one-bits, the amount of storage required for this field is INT((N+7)/8) bytes.

 Variable-sized. The row-image, containing values of all table fields. This only lists table fields that are used (according to the second field of the variable data part) and non-NULL (according to the previous field). In other words, the number of values listed here is equal to the number of zero bits in the previous field (not counting padding bits in the last byte).

 The format of each value is described in the log_event_print_value() function in log_event.cc.

 (for UPDATE_ROWS_EVENT only) the previous two fields are repeated, representing a second table row.

 For each row, the following is done:

 For WRITE_ROWS_LOG_EVENT, the row described by the row-image is inserted.

 For DELETE_ROWS_LOG_EVENT, a row matching the given row-image is deleted.

 For UPDATE_ROWS_LOG_EVENT, a row matching the first row-image is removed, and the row described by the second row-image is inserted.

 */
public class UpdateEventData implements EventData {
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
    /**
     * 2 bytes
     */
    private int extraInfoLength;
    /**
     *
     */
    private byte[] extraInfo;
    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * Packed integer. The number of columns in the table.
     */
    private int packedInteger;

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

    public int getExtraInfoLength() {
        return extraInfoLength;
    }

    public void setExtraInfoLength(int extraInfoLength) {
        this.extraInfoLength = extraInfoLength;
    }

    public byte[] getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(byte[] extraInfo) {
        this.extraInfo = extraInfo;
    }

    public int getPackedInteger() {
        return packedInteger;
    }

    public void setPackedInteger(int packedInteger) {
        this.packedInteger = packedInteger;
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

    private class Row implements Serializable {
        public List<Cell> getCells() {
            return cells;
        }

        public void setCells(List<Cell> cells) {
            this.cells = cells;
        }

        private List<Cell> cells;
    }

    private class Cell<T> implements Serializable {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }
    }
}
