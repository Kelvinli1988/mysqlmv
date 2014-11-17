package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

import java.util.BitSet;

/**
 * Created by Kelvin Li on 11/14/2014 5:38 PM.
 */

/**
 *     Used for row-based binary logging beginning with MySQL 5.1.5.
 */
public class TableMapEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    /**
     * 6 bytes. The table ID.
     */
    private long tableID;
    /**
     * 2 bytes. Reserved for future use.
     */
    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * 1 byte. The length of the database name.
     */
    private int dbNameLength;

    /**
     * Variable-sized. The database name (null-terminated).
     */
    private String dbName;
    /**
     *  1 byte. The length of the table name.
     */
    private int tableNameLength;
    /**
     * Variable-sized. The table name (null-terminated).
     */
    private String tableName;
    /**
     * Packed integer. The number of columns in the table.
     */
    private int columnNum;
    /**
     * Variable-sized. An array of column types, one byte per column.
     */
    private byte[] columnTypeArray;
    /**
     *     Packed integer. The length of the metadata block.
     */
    private int metaDataLength;
    /**
     * Variable-sized. The metadata block; see log_event.h for contents and format.
     */
    private int[] metadata;
    /**
     * Variable-sized. Bit-field indicating whether each column can be NULL, one bit per column. For this field, the amount of storage required for N columns is INT((N+7)/8) bytes.
     */
    private BitSet columnNullable;

    public long getTableID() {
        return tableID;
    }

    public void setTableID(long tableID) {
        this.tableID = tableID;
    }

    public int getDbNameLength() {
        return dbNameLength;
    }

    public void setDbNameLength(int dbNameLength) {
        this.dbNameLength = dbNameLength;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public int getTableNameLength() {
        return tableNameLength;
    }

    public void setTableNameLength(int tableNameLength) {
        this.tableNameLength = tableNameLength;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(int columnNum) {
        this.columnNum = columnNum;
    }

    public byte[] getColumnTypeArray() {
        return columnTypeArray;
    }

    public void setColumnTypeArray(byte[] columnTypeArray) {
        this.columnTypeArray = columnTypeArray;
    }

    public int getMetaDataLength() {
        return metaDataLength;
    }

    public void setMetaDataLength(int metaDataLength) {
        this.metaDataLength = metaDataLength;
    }


    public BitSet getColumnNullable() {
        return columnNullable;
    }

    public void setColumnNullable(BitSet columnNullable) {
        this.columnNullable = columnNullable;
    }

    public int[] getMetadata() {
        return metadata;
    }

    public void setMetadata(int[] metadata) {
        this.metadata = metadata;
    }
}
