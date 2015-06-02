package org.mysqlmv.cdc.logevent;

/**
 *
 * Created by Kelvin Li on 6/1/2015.
 */
public interface IRowElement {
    /**
     * @return type of this row element.
     */
    java.sql.Types getType();

    /**
     * @return the value of the row element.
     */
    Object getValue();
}
