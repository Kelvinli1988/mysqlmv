package org.mysqlmv.cd.logevent.eventdef.datainmanual;

/**
 * Created by Kelvin Li on 11/13/2014 3:00 PM.
 */
public class LoadEventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    /**
     * 4 bytes. The ID of the thread on the master that issued this LOAD DATA INFILE statement.
     * Needed for temporary tables. This is also useful for a DBA for knowing who did what on the master.
     */
    private long threadId;
    /**
     * 4 bytes. The time in seconds which the LOAD DATA INFILE took for execution.
     * Only useful for inspection by the DBA.
     */
    private long timestamp;
    /**
     *  1byte. The length of the name of the table to load.
     */
    private int lenOfTable2Load;
    /**
     * 1 byte. The length of the name of the database containing the table.
     */
    private int lenOfDBName;
    /**
     * 4 bytes. The number of columns to load ((col_name,...) clause). Will be nonzero only if
     * the columns to load were explicitly mentioned in the statement.
     */
    private int columnNum;
    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * 1 byte. The field-terminating character (FIELDS TERMINATED BY option).
     */
    private byte fieldTerminator;
    /**
     * 1 byte. The field-enclosing character (FIELDS ENCLOSED BY option).
     */
    private byte fieldEncloser;
    /**
     * 1 byte. The line-starting character (LINES STARTING BY option).
     */
    private byte lineStarter;
    /**
     * 1 byte. The escaping character (FIELDS ESCAPED BY option).
     */
    private byte escapor;
    /**
     *  Flags that indicate whether certain keywords are present in the statement:

     DUMPFILE_FLAG =0x1 (unused; this flag appears to be a botch because it would apply to SELECT ... INTO OUTFILE, not LOAD DATA INFILE)

     OPT_ENCLOSED_FLAG = 0x2 (FIELD OPTIONALLY ENCLOSED BY option)

     REPLACE_FLAG = 0x4 (LOAD DATA INFILE REPLACE)

     IGNORE_FLAG = 0x8 (LOAD DATA INFILE IGNORE)
     */
    private byte keyWordsIndicator;
    /**
     * 1 byte. Flags that indicate whether each of the field and line options are empty. The low-order five bits are 1 to indicate an empty option (has a length of 0) or 0 to indicate a non-empty option (has a length of 1).

     FIELD_TERM_EMPTY = 0x1

     ENCLOSED_EMPTY = 0x2

     LINE_TERM_EMPTY = 0x4

     LINE_START_EMPTY = 0x8

     ESCAPED_EMPTY = 0x10
     */
    private byte fieldAndLineOptionIndicator;
    /**
     * 1 byte. The length of the name of the first column to load.
     */
    private int firstColumnNameLength;
    /**
     * 1 byte. The length of the name of the last column to load.
     */
    private int lastColumnNameLength;
    /**
     * Variable-sized. The name of the first column to load (null-terminated).
     */
    private String firstColumnName2Load;
    /**
     * Variable-sized. The name of the last column to load (null-terminated).
     */
    private String lastColumnName2Load;
    /**
     * Variable-sized. The name of the table to load (null-terminated).
     */
    private String tableName2Load;
    /**
     * Variable-sized. The name of the database that contains the table (null-terminated).
     */
    private String dbNameOfTable;
    /**
     * Variable-sized. The name of the file that was loaded (the original name on the master,
     * not the name of the temporary file created on the slave). The length of the data filename
     * is the event size minus the size of all other parts.
     */
    private String fileName2LoadOnMaster;

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

    public int getLenOfTable2Load() {
        return lenOfTable2Load;
    }

    public void setLenOfTable2Load(int lenOfTable2Load) {
        this.lenOfTable2Load = lenOfTable2Load;
    }

    public int getLenOfDBName() {
        return lenOfDBName;
    }

    public void setLenOfDBName(int lenOfDBName) {
        this.lenOfDBName = lenOfDBName;
    }

    public int getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(int columnNum) {
        this.columnNum = columnNum;
    }

    public byte getFieldTerminator() {
        return fieldTerminator;
    }

    public void setFieldTerminator(byte fieldTerminator) {
        this.fieldTerminator = fieldTerminator;
    }

    public byte getFieldEncloser() {
        return fieldEncloser;
    }

    public void setFieldEncloser(byte fieldEncloser) {
        this.fieldEncloser = fieldEncloser;
    }

    public byte getLineStarter() {
        return lineStarter;
    }

    public void setLineStarter(byte lineStarter) {
        this.lineStarter = lineStarter;
    }

    public byte getEscapor() {
        return escapor;
    }

    public void setEscapor(byte escapor) {
        this.escapor = escapor;
    }

    public byte getKeyWordsIndicator() {
        return keyWordsIndicator;
    }

    public void setKeyWordsIndicator(byte keyWordsIndicator) {
        this.keyWordsIndicator = keyWordsIndicator;
    }

    public byte getFieldAndLineOptionIndicator() {
        return fieldAndLineOptionIndicator;
    }

    public void setFieldAndLineOptionIndicator(byte fieldAndLineOptionIndicator) {
        this.fieldAndLineOptionIndicator = fieldAndLineOptionIndicator;
    }

    public int getFirstColumnNameLength() {
        return firstColumnNameLength;
    }

    public void setFirstColumnNameLength(int firstColumnNameLength) {
        this.firstColumnNameLength = firstColumnNameLength;
    }

    public int getLastColumnNameLength() {
        return lastColumnNameLength;
    }

    public void setLastColumnNameLength(int lastColumnNameLength) {
        this.lastColumnNameLength = lastColumnNameLength;
    }

    public String getFirstColumnName2Load() {
        return firstColumnName2Load;
    }

    public void setFirstColumnName2Load(String firstColumnName2Load) {
        this.firstColumnName2Load = firstColumnName2Load;
    }

    public String getLastColumnName2Load() {
        return lastColumnName2Load;
    }

    public void setLastColumnName2Load(String lastColumnName2Load) {
        this.lastColumnName2Load = lastColumnName2Load;
    }

    public String getTableName2Load() {
        return tableName2Load;
    }

    public void setTableName2Load(String tableName2Load) {
        this.tableName2Load = tableName2Load;
    }

    public String getDbNameOfTable() {
        return dbNameOfTable;
    }

    public void setDbNameOfTable(String dbNameOfTable) {
        this.dbNameOfTable = dbNameOfTable;
    }

    public String getFileName2LoadOnMaster() {
        return fileName2LoadOnMaster;
    }

    public void setFileName2LoadOnMaster(String fileName2LoadOnMaster) {
        this.fileName2LoadOnMaster = fileName2LoadOnMaster;
    }
}
