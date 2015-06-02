package org.mysqlmv.cdc.logevent.schema;

import java.sql.Types;

/**
 * Provides a means for getting
 * metadata about a specific field in a database object.
 */
public interface IFieldMetadata extends IName {

    /**
     * Returns the field id.
     *
     * @return the field id.
     */
    public Object getID();

    /**
     * @return the position (starting with 1) in the database object
     * of this field.
     */
    public Number getPosition();

    public void setPosition(Number p);

    /**
     * @return the <code>java.sql.Types</code> type for this field.
     * @see java.sql.Types
     */
    public Types getType();

    public void setType(Types t);

    /**
     * @return the database's type name for this field.
     */
    public String getTypeName();

    public void setTypeName(String n);

    /**
     * @return the default value of this field.  If this field has no
     * default value, returns <code>null</code>.
     */
    public Object getDefaultValue();

    public void setDefaultValue(Object v);

    /**
     * @return <code>true</code> if this field has a default value;
     * otherwise <code>false</code>.
     */
    public boolean hasDefaultValue();

    /**
     * @return <code>true</code> if this field allows null values;
     * otherwise <code>false</code>.
     */
    public boolean isNullable();

    public void setNullable();

    /**
     * @return <code>true</code> if this field is a large object (LOB);
     * otherwise <code>false</code>.
     */
    public boolean isLob();

    public void setLob();

    /**
     * @return <code>true</code> if this field is a large object (LOB);
     * AND the lob is stored away from the other row data.
     * otherwise <code>false</code>.
     */
    public boolean isLobOffRow();

    public void setLobOffRow(boolean b);

    /**
     * @return <code>true</code> if this field is a binary datatype;
     * otherwise <code>false</code>.
     */
    public boolean isBinary();

    public void setBinary();

    /**
     * @return <code>true</code> if this field is an identity field;
     * otherwise <code>false</code>.
     */
    public boolean isIdentity();

    public void setIdentity();

    /**
     * @return <code>true</code> if this field is part of a primary key;
     * otherwise <code>false</code>.
     */
    public boolean isPrimaryKey();

    public void setPrimaryKey();

    /**
     * Return the description of this field.
     *
     * @return the description of this field.
     */
    public String getDescription();
}
