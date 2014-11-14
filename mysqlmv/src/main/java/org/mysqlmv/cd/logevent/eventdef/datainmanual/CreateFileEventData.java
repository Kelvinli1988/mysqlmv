package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 3:45 PM.
 */

/**
 * This event is used for LOAD DATA INFILE statements. See also LOAD DATA INFILE Events.
 * <p/>
 * This event tells the slave to create a temporary file and fill it with a first data block.
 * Later, zero or more APPEND_BLOCK_EVENT events append blocks to this temporary file.
 * EXEC_LOAD_EVENT tells the slave to load the temporary file into the table, or DELETE_FILE_EVENT
 * tells the slave not to do the load and to delete the temporary file. DELETE_FILE_EVENT occurs
 * when the LOAD DATA failed on the master: On the master we start to write loaded blocks to the
 * binary log before the end of the statement. If for some reason there is an error, we must tell
 * the slave to abort the load.
 * <p/>
 * The format for this event is more complicated than for others, because it contains information
 * about many LOAD DATA INFILE statement clauses.
 */
public class CreateFileEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    /**
     * 4 bytes. The time in seconds which the LOAD DATA INFILE took for execution. Only useful for inspection by the DBA.
     */
    private long threadId;
    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * 4 bytes. The time in seconds which the LOAD DATA INFILE took for execution. Only useful for inspection by the DBA.
     */
    private long timestamp;
    /**
     * 4 bytes. The number of lines to skip at the beginning of the file (corresponds to the IGNORE N LINES clause of LOAD DATA INFILE).
     */
    private long lineNumber2Skip;
    /**
     * 1 byte. The length of the name of the table to load.
     */
    private int tableNameLength2Load;
    /**
     * 1 byte. The length of the name of the database containing the table.
     */
    private int dbNameLengthContainTable;
    /**
     * 4 bytes. The number of lines to skip at the beginning of the file (corresponds to the IGNORE N LINES clause of LOAD DATA INFILE).
     */
    private int columnNumber2Load;
    /**
     * 4 bytes. An ID for the data file. This is necessary in case several LOAD DATA INFILE statements
     * occur in parallel on the master. In that case, the binary log may contain intermixed events for
     * the statements. The ID resolves which file the blocks in each APPEND_BLOCK_EVENT must be appended,
     * and the file that must be loaded by the EXEC_LOAD_EVENT or deleted by the DELETE_FILE_EVENT.
     */
    private int dataFileID;
    /**
     * 1 byte. The length of the field-terminating string (FIELDS TERMINATED BY option).
     */
    private int filedTerminatorLength;
    /**
     * Variable-sized. The field-terminating string.
     */
    private String fieldTerminator;
    /**
     * 1 byte. The length of the field-enclosing string (FIELDS ENCLOSED BY option).
     */
    private int fieldEncloserLength;
    /**
     * Variable-sized. The field-enclosing string.
     */
    private String fieldEncloser;
    /**
     * 1 byte. The length of the line-terminating string (LINES TERMINATED BY option).
     */
    private int lineTerminatorLength;
    /**
     * Variable-sized. The line-terminating string.
     */
    private String lineTerminator;
    /**
     * 1 byte. The length of the line-starting string (LINES STARTING BY option).
     */
    private int lineStartLength;
    /**
     * Variable-sized. The line-starting string.
     */
    private String lineStart;
    /**
     * 1 byte. The length of the escaping string (FIELDS ESCAPED BY option).
     */
    private int escaporLength;
    /**
     * Variable-sized. The escaping string.
     */
    private String escapor;
    /**
     * 1 byte. Flags that indicate whether certain keywords are present in the statement:
     * <p/>
     * DUMPFILE_FLAG =0x1 (unused; this flag appears to be a botch because it would apply to SELECT ... INTO OUTFILE, not LOAD DATA INFILE)
     * <p/>
     * OPT_ENCLOSED_FLAG = 0x2 (FIELD OPTIONALLY ENCLOSED BY option)
     * <p/>
     * REPLACE_FLAG = 0x4 (LOAD DATA INFILE REPLACE)
     * <p/>
     * IGNORE_FLAG = 0x8 (LOAD DATA INFILE IGNORE)
     */
    private byte keyWordsIndicator;
    /**
     * 1 byte. The length of the name of the first column to load.
     */
    private int firstColumnLength2Load;
    /**
     * 1 byte. The length of the name of the last column to load.
     */
    private int lastColumnLength2Load;
    /**
     * Variable-sized. The name of the first column to load (null-terminated).
     */
    private String firstColumnName;
    /**
     * Variable-sized. The name of the last column to load (null-terminated).
     */
    private String lastColumnName;
    /**
     * Variable-sized. The name of the table to load (null-terminated).
     */
    private String tableName2Load;
    /**
     * Variable-sized. The name of the database that contains the table (null-terminated).
     */
    private String dbNameContainTable;
    /**
     * Variable-sized. The name of the file that was loaded (the original name on the master,
     * not the name of the temporary file created on the slave) (null-terminated). The length
     * of the data filename is not explicit in the event. However, it is null-terminated, so
     * the length can be determined by reading to the null byte.
     */
    private String fileNameLoaded;
    /**
     * Variable-sized. The block of raw data to load. If the file size exceeds a threshold,
     * additional APPEND_BLOCK_EVENT instances will follow, each containing a data block.
     * The size of the raw data is the event size minus the size of all other parts.
     */
    private byte[] rawDataBlock;

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getLineNumber2Skip() {
        return lineNumber2Skip;
    }

    public void setLineNumber2Skip(long lineNumber2Skip) {
        this.lineNumber2Skip = lineNumber2Skip;
    }

    public int getTableNameLength2Load() {
        return tableNameLength2Load;
    }

    public void setTableNameLength2Load(int tableNameLength2Load) {
        this.tableNameLength2Load = tableNameLength2Load;
    }

    public int getDbNameLengthContainTable() {
        return dbNameLengthContainTable;
    }

    public void setDbNameLengthContainTable(int dbNameLengthContainTable) {
        this.dbNameLengthContainTable = dbNameLengthContainTable;
    }

    public int getColumnNumber2Load() {
        return columnNumber2Load;
    }

    public void setColumnNumber2Load(int columnNumber2Load) {
        this.columnNumber2Load = columnNumber2Load;
    }

    public int getDataFileID() {
        return dataFileID;
    }

    public void setDataFileID(int dataFileID) {
        this.dataFileID = dataFileID;
    }

    public int getFiledTerminatorLength() {
        return filedTerminatorLength;
    }

    public void setFiledTerminatorLength(int filedTerminatorLength) {
        this.filedTerminatorLength = filedTerminatorLength;
    }

    public String getFieldTerminator() {
        return fieldTerminator;
    }

    public void setFieldTerminator(String fieldTerminator) {
        this.fieldTerminator = fieldTerminator;
    }

    public int getFieldEncloserLength() {
        return fieldEncloserLength;
    }

    public void setFieldEncloserLength(int fieldEncloserLength) {
        this.fieldEncloserLength = fieldEncloserLength;
    }

    public String getFieldEncloser() {
        return fieldEncloser;
    }

    public void setFieldEncloser(String fieldEncloser) {
        this.fieldEncloser = fieldEncloser;
    }

    public int getLineTerminatorLength() {
        return lineTerminatorLength;
    }

    public void setLineTerminatorLength(int lineTerminatorLength) {
        this.lineTerminatorLength = lineTerminatorLength;
    }

    public String getLineTerminator() {
        return lineTerminator;
    }

    public void setLineTerminator(String lineTerminator) {
        this.lineTerminator = lineTerminator;
    }

    public int getLineStartLength() {
        return lineStartLength;
    }

    public void setLineStartLength(int lineStartLength) {
        this.lineStartLength = lineStartLength;
    }

    public String getLineStart() {
        return lineStart;
    }

    public void setLineStart(String lineStart) {
        this.lineStart = lineStart;
    }

    public int getEscaporLength() {
        return escaporLength;
    }

    public void setEscaporLength(int escaporLength) {
        this.escaporLength = escaporLength;
    }

    public String getEscapor() {
        return escapor;
    }

    public void setEscapor(String escapor) {
        this.escapor = escapor;
    }

    public byte getKeyWordsIndicator() {
        return keyWordsIndicator;
    }

    public void setKeyWordsIndicator(byte keyWordsIndicator) {
        this.keyWordsIndicator = keyWordsIndicator;
    }

    public int getFirstColumnLength2Load() {
        return firstColumnLength2Load;
    }

    public void setFirstColumnLength2Load(int firstColumnLength2Load) {
        this.firstColumnLength2Load = firstColumnLength2Load;
    }

    public int getLastColumnLength2Load() {
        return lastColumnLength2Load;
    }

    public void setLastColumnLength2Load(int lastColumnLength2Load) {
        this.lastColumnLength2Load = lastColumnLength2Load;
    }

    public String getFirstColumnName() {
        return firstColumnName;
    }

    public void setFirstColumnName(String firstColumnName) {
        this.firstColumnName = firstColumnName;
    }

    public String getLastColumnName() {
        return lastColumnName;
    }

    public void setLastColumnName(String lastColumnName) {
        this.lastColumnName = lastColumnName;
    }

    public String getTableName2Load() {
        return tableName2Load;
    }

    public void setTableName2Load(String tableName2Load) {
        this.tableName2Load = tableName2Load;
    }

    public String getDbNameContainTable() {
        return dbNameContainTable;
    }

    public void setDbNameContainTable(String dbNameContainTable) {
        this.dbNameContainTable = dbNameContainTable;
    }

    public String getFileNameLoaded() {
        return fileNameLoaded;
    }

    public void setFileNameLoaded(String fileNameLoaded) {
        this.fileNameLoaded = fileNameLoaded;
    }

    public byte[] getRawDataBlock() {
        return rawDataBlock;
    }

    public void setRawDataBlock(byte[] rawDataBlock) {
        this.rawDataBlock = rawDataBlock;
    }
}
