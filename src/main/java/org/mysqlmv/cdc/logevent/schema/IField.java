package org.mysqlmv.cdc.logevent.schema;


/**
 * Represent a field of a table.
 */
public interface IField {
    /**
     * Return the value of this field.
     *
     * @returns the value of this field.
     */
    public Object getValue();
}
