package org.mysqlmv.cdc.logevent;

        import java.io.Serializable;

/**
 * Created by Kelvin Li on 11/13/2014 10:43 AM.
 */

/**
 * Event data structure
 * +=====================================+
 * | event  | fixed part                 |
 * | data   +----------------------------+
 * |        | variable part              |
 * +=====================================+
 */
public interface IEventData extends Serializable {
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
    // Empty
}
