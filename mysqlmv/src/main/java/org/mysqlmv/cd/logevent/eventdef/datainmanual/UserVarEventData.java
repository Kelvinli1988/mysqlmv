package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 4:33 PM.
 */
public class UserVarEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    // Empty

    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * 4 bytes. the size of the user variable name.
     */
    private int userVarNameSize;
    /**
     * The user variable name.
     */
    private String userVarName;
    /**
     * 1 byte. Non-zero if the variable value is the SQL NULL value, 0 otherwise. If this byte is 0,
     * the following parts exist in the event.
     */
    private boolean isNull;
    /**
     * 1 byte. The user variable type. The value corresponds to elements of enum Item_result defined in include/mysql_com.h
     * (STRING_RESULT=0, REAL_RESULT=1, INT_RESULT=2, ROW_RESULT=3, DECIMAL_RESULT=4).
     */
    private int userVarType;
    /**
     * 4 bytes. The number of the character set for the user variable (needed for a string variable).
     * The character set number is really a collation number that indicates a character set/collation pair.
     */
    // TODO what is the real type?
    private int charSetNo;
    /**
     * 4 bytes. The size of the user variable value (corresponds to member val_len of class Item_string).
     */
    private int userVarValueSize;
    /**
     * Variable-sized. For a string variable, this is the string. For a float or integer variable,
     * this is its value in 8 bytes. For a decimal this value is a packed value - 1 byte for the precision,
     * 1 byte for the scale, and $size - 2 bytes for the actual value. See the decimal2bin function in
     * strings/decimal.c for the format of this packed value.
     */
    // TODO what is this?
    private Object packedValue;

    public int getUserVarNameSize() {
        return userVarNameSize;
    }

    public void setUserVarNameSize(int userVarNameSize) {
        this.userVarNameSize = userVarNameSize;
    }

    public String getUserVarName() {
        return userVarName;
    }

    public void setUserVarName(String userVarName) {
        this.userVarName = userVarName;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean isNull) {
        this.isNull = isNull;
    }

    public int getUserVarType() {
        return userVarType;
    }

    public void setUserVarType(int userVarType) {
        this.userVarType = userVarType;
    }

    public int getCharSetNo() {
        return charSetNo;
    }

    public void setCharSetNo(int charSetNo) {
        this.charSetNo = charSetNo;
    }

    public int getUserVarValueSize() {
        return userVarValueSize;
    }

    public void setUserVarValueSize(int userVarValueSize) {
        this.userVarValueSize = userVarValueSize;
    }

    public Object getPackedValue() {
        return packedValue;
    }

    public void setPackedValue(Object packedValue) {
        this.packedValue = packedValue;
    }
}
